"""Event correlation for unified_alert_mqtt.py.

Mixed into UnifiedAlertMonitor. Decides, per alert type, whether a message rings, arrives silently,
joins the current drive's thread or becomes part of the parked-car incident, so the phone rings once
per incident instead of once per check:

  * Drive thread: night use (23:00-07:00, car's local time) and speeding (> 110 km/h for 5 min) share
    ONE ringing message that is then edited silently; the end-of-drive summary is a silent reply and
    the parked-weather digest a silent reply to that. Drives without an alarm get one silent summary.
  * Parked incident: doors/windows open, unlocked with nobody inside, someone inside while locked,
    cabin hot, climate left on -> one message edited as conditions change, closed with a silent
    "all clear" reply. A new incident rings at most every INCIDENT_RING_INTERVAL; someone inside +
    hot always rings.
  * Every alarm message carries a map link to where the car was when it happened (fixed), plus the
    live position while it updates.
"""
import html
import logging
import math
import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from service_mode import is_service_mode
from telemetry_watchdog import TelemetryWatchdog

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ configuration
NIGHT_START = os.getenv('NIGHT_START', '23:00')
NIGHT_END = os.getenv('NIGHT_END', '07:00')
SPEED_ABS_LIMIT = float(os.getenv('SPEED_ABS_LIMIT_KMH', '110'))
SPEED_ABS_SUSTAIN = int(os.getenv('SPEED_ABS_SUSTAIN_S', '300'))    # 5 minutes above the limit
SPEED_ABS_GAP = int(os.getenv('SPEED_ABS_GAP_S', '30'))             # dips shorter than this don't reset
DRIVE_END_DELAY = int(os.getenv('DRIVE_END_DELAY_S', '90'))
INCIDENT_RING_INTERVAL = int(os.getenv('INCIDENT_RING_INTERVAL_S', '900'))
INCIDENT_CLEAR_DELAY = int(os.getenv('INCIDENT_CLEAR_DELAY_S', '60'))
LIVE_EDIT_INTERVAL = int(os.getenv('LIVE_EDIT_INTERVAL_S', '60'))
WEATHER_REPLY_WINDOW = 900
ALERTS_DRY_RUN = os.getenv('ALERTS_DRY_RUN', '0') == '1'
# Phone call (PagerDuty) when the car is charged to CHARGE_CALL_SOC %, locked, away from Home/Work.
# The routing key is read at run time (PAGERDUTY_ROUTING_KEY in .env).
CHARGE_CALL_SOC = float(os.getenv('CHARGE_CALL_SOC', '75'))
TYRE_PAGERDUTY_BAR = float(os.getenv('TYRE_PAGERDUTY_BAR', '1.9'))
CHARGE_CALL_SAFE_M = float(os.getenv('CHARGE_CALL_SAFE_RADIUS_M', '150'))   # "at home/work" also within this of the centre
CHARGE_CALL_SAFE = re.compile(os.getenv('CHARGE_CALL_SAFE_GEOFENCES', r'^(home|work)\b'), re.I)
PAGERDUTY_EVENTS_URL = os.getenv('PAGERDUTY_EVENTS_URL', 'https://events.pagerduty.com/v2/enqueue')
DEFAULT_TZ = os.getenv('TZ', 'UTC')
TELEGRAM_API_BASE = os.getenv('TELEGRAM_API_BASE', 'https://api.telegram.org')

# How each existing alert type is delivered
#   ring      normal notification          silent    delivered without sound
#   incident  merged into the parked-car incident message
#   drive     absorbed by the drive thread (no separate message)
#   weather   silent reply to the last drive summary
#   drop      not sent here (another service already sends it)
ALERT_POLICY = {
    'tpms_low_pressure': 'ring',
    'charging_summary': 'ring',
    'low_power': 'ring',
    'update_available': 'drop',      # sent by another service
    'low_range': 'ring',
    'sentry_mode': 'silent',
    'charging': 'silent',            # plugged / unplugged
    'charge_limit': 'silent',        # 78 % / 98 % reached
    'update_installing': 'silent',
    'update_completed': 'silent',
    'dog_mode': 'silent',
    'geofence': 'silent',
    'doors_open': 'incident',
    'unlocked_no_user': 'incident',
    'locked_user': 'incident',
    'cabin_overheat': 'incident',
    'climate_left_on': 'incident',
    'car_started': 'drive',
    'speed_violation': 'drive',      # replaced by the 110 km/h / 5 min rule below
    'parked_weather': 'weather',
    'charge_call': 'ring',           # sent directly by _charge_call_tick
}

INCIDENT_LABELS = {
    'doors_open': '🚪 Doors / windows / trunk open',
    'unlocked_no_user': '🔓 Unlocked, nobody inside',
    'locked_user': '🧍 <b>Someone is inside</b> while the car is locked',
    'cabin_overheat': '🌡️ <b>Cabin hot</b>',
    'climate_left_on': '❄️ Climate left on while parked',
}


def _esc(s):
    return html.escape(str(s), quote=False)


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _b(v):
    return str(v).strip().lower() == 'true'


def _dur(seconds):
    seconds = int(max(0, seconds))
    h, m = divmod(seconds // 60, 60)
    return f"{h} h {m:02d} min" if h else (f"{m} min" if m else f"{seconds} s")


def _km(a, b):
    r = math.radians
    dlat, dlon = r(b[0] - a[0]), r(b[1] - a[1])
    h = math.sin(dlat / 2) ** 2 + math.cos(r(a[0])) * math.cos(r(b[0])) * math.sin(dlon / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def _link(pos, label):
    if not pos or pos[0] is None or pos[1] is None:
        return ''
    return f'<a href="https://www.google.com/maps?q={pos[0]:.5f},{pos[1]:.5f}">{_esc(label)}</a>'


class ThreadsMixin:
    """Requires: self.car_data, self.alert_history, self.alert_counts, self.telegram_*,
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID via self._tg_token / self._tg_chat."""

    def threads_init(self, token, chat, max_per_hour):
        self._tg_token, self._tg_chat, self._max_per_hour = token, chat, max_per_hour
        self._tg_last_call = 0.0
        self.drives = {}           # car_id -> drive thread
        self.incidents = {}        # car_id -> parked incident
        self.last_summary = {}     # car_id -> {'id', 'at'}
        self.last_incident_ring = {}
        self._tz_cache, self._town_cache = {}, {}
        self.dry_sink = None       # list in tests; None = print in dry-run
        self.charge_calls = {}     # car_id -> charging session state for the 75 % phone call 
        self.safety_calls = {}
        self._safe_places, self._safe_loaded = [], 0
        self.clock = time.time
        self.telemetry_watchdog = TelemetryWatchdog(self.clock)

    # ------------------------------------------------------------- telegram primitives
    def _tg(self, method, data):
        if ALERTS_DRY_RUN:
            self._dry_id = getattr(self, '_dry_id', 5000) + 1
            rec = {'method': method, **{k: v for k, v in data.items() if k != 'chat_id'}}
            if self.dry_sink is not None:
                self.dry_sink.append(rec)
            else:
                logger.info("DRY-RUN %s", rec)
            return {'ok': True, 'result': {'message_id': self._dry_id}}
        if is_service_mode():   # car at the workshop: nothing leaves, nothing is stored (no message_id)
            logger.info("service mode: Telegram %s muted", method)
            return {'ok': True, 'result': {}}
        url = f"{TELEGRAM_API_BASE}/bot{self._tg_token}/{method}"
        for attempt in range(4):
            wait = 1.1 - (time.time() - self._tg_last_call)
            if wait > 0:
                time.sleep(wait)
            try:
                r = requests.post(url, data=data, timeout=15)
                self._tg_last_call = time.time()
                j = r.json()
            except Exception as e:
                logger.error(f"✗ Telegram {method} failed (attempt {attempt + 1}): {e}")
                time.sleep(2 * (attempt + 1))
                continue
            if j.get('ok') or 'message is not modified' in j.get('description', ''):
                return j
            retry = (j.get('parameters') or {}).get('retry_after')
            if retry:
                time.sleep(int(retry) + 1)
                continue
            logger.error(f"✗ Telegram {method} error: {j.get('description')}")
            return j
        return {'ok': False}

    def tg_send(self, text, silent=False, reply_to=None, alert_type='general'):
        if not silent:
            one_hour_ago = time.time() - 3600
            self.alert_history = [a for a in self.alert_history if (a.timestamp() if hasattr(a, 'timestamp') else a) > one_hour_ago]
            if len(self.alert_history) >= self._max_per_hour:
                logger.warning(f"⚠️  Ringing rate limit reached ({self._max_per_hour}/hour): sending silently")
                silent = True
        data = {'chat_id': self._tg_chat, 'text': text, 'parse_mode': 'HTML',
                'disable_web_page_preview': 'true', 'disable_notification': 'true' if silent else 'false'}
        if reply_to:
            data['reply_to_message_id'] = reply_to
            data['allow_sending_without_reply'] = 'true'
        j = self._tg('sendMessage', data)
        if j.get('ok'):
            self.telegram_successes += 1
            self.alert_counts[alert_type] += 1
            if not silent:
                self.alert_history.append(datetime.now())
            return (j.get('result') or {}).get('message_id') or True
        self.telegram_failures += 1
        return None

    def tg_edit(self, message_id, text):
        if message_id and message_id is not True:
            self._tg('editMessageText', {'chat_id': self._tg_chat, 'message_id': message_id, 'text': text,
                                         'parse_mode': 'HTML', 'disable_web_page_preview': 'true'})

    # ------------------------------------------------------------- the router
    def send_telegram(self, message, alert_type='general', car_id=None):
        """Replaces the old one-message-per-alert sender. Returns True when the alert is handled
        (callers then start their own cooldowns exactly as before)."""
        policy = ALERT_POLICY.get(alert_type, 'ring')
        car_id = car_id or (next(iter(self.car_data)) if self.car_data else '1')
        if policy == 'drop':
            logger.info(f"   ({alert_type} not sent: handled by another service)")
            return True
        if policy == 'drive':
            logger.info(f"   ({alert_type} absorbed by the drive thread)")
            return True
        if policy == 'incident':
            self.incident_add(car_id, alert_type)
            return True
        if policy == 'weather':
            ls = self.last_summary.get(car_id)
            reply = ls['id'] if ls and self.clock() - ls['at'] <= WEATHER_REPLY_WINDOW else None
            return bool(self.tg_send(message, silent=True, reply_to=reply, alert_type=alert_type))
        return bool(self.tg_send(message, silent=(policy == 'silent'), alert_type=alert_type))

    # ------------------------------------------------------------- helpers
    def _pos(self, car_id):
        c = self.car_data[car_id]
        return [_f(c.get('latitude')), _f(c.get('longitude'))]

    def _name(self, car_id):
        return self.car_data[car_id].get('display_name') or f"Car {car_id}"

    def _tz(self, pos):
        if not pos or None in pos:
            return ZoneInfo(DEFAULT_TZ)
        key = (round(pos[0], 1), round(pos[1], 1))
        if key not in self._tz_cache:
            name = None
            if not ALERTS_DRY_RUN:
                try:
                    r = requests.get('https://api.open-meteo.com/v1/forecast', timeout=8, params={
                        'latitude': key[0], 'longitude': key[1], 'timezone': 'auto', 'current': 'temperature_2m'})
                    name = r.json().get('timezone')
                except Exception as e:
                    logger.warning(f"timezone lookup failed: {e}")
            self._tz_cache[key] = name or getattr(self, 'test_tz', None) or DEFAULT_TZ
        try:
            return ZoneInfo(self._tz_cache[key])
        except Exception:
            return ZoneInfo(DEFAULT_TZ)

    def _local(self, car_id, ts=None, pos=None):
        return datetime.fromtimestamp(ts if ts is not None else self.clock(), self._tz(pos or self._pos(car_id)))

    def local_stamp(self, car_id, fmt='%Y-%m-%d %H:%M:%S %Z'):
        """Time in the car's current time zone (not the container's)."""
        try:
            return self._local(car_id).strftime(fmt)
        except Exception:
            return datetime.now(ZoneInfo(DEFAULT_TZ)).strftime(fmt)

    def _place(self, car_id, pos=None):
        g = (self.car_data[car_id].get('geofence') or '').strip()
        if g and pos is None:
            return g
        pos = pos or self._pos(car_id)
        if None in pos:
            return g or 'unknown place'
        key = (round(pos[0], 2), round(pos[1], 2))
        if key not in self._town_cache:
            name = ''
            if not ALERTS_DRY_RUN:
                try:
                    r = requests.get('https://nominatim.openstreetmap.org/reverse', timeout=8,
                                     headers={'User-Agent': os.getenv('NOMINATIM_USER_AGENT', 'teslamate-alerts/1.0')},
                                     params={'format': 'jsonv2', 'zoom': 14, 'accept-language': 'en', 'lat': key[0], 'lon': key[1]})
                    a = r.json().get('address', {})
                    parts = [a.get('suburb') or a.get('village') or a.get('town'), a.get('city') or a.get('county')]
                    name = ', '.join(dict.fromkeys(p for p in parts if p)) or a.get('country', '')
                except Exception as e:
                    logger.warning(f"reverse geocode failed: {e}")
            self._town_cache[key] = name or f"{pos[0]:.3f}, {pos[1]:.3f}"
        return g or self._town_cache[key]

    @staticmethod
    def _in_night(dt):
        s = datetime.strptime(NIGHT_START, '%H:%M').time()
        e = datetime.strptime(NIGHT_END, '%H:%M').time()
        t = dt.time()
        return (t >= s or t < e) if s > e else (s <= t < e)

    def _driving(self, car_id):
        c = self.car_data[car_id]
        return c.get('state') == 'driving' or c.get('shift_state') in ('D', 'R', 'N')

    # ------------------------------------------------------------- tick (every second)
    def threads_tick(self):
        now = self.clock()
        for car_id in list(self.car_data):
            try:
                self._drive_tick(car_id, now)
                self._incident_tick(car_id, now)
                self._charge_call_tick(car_id, now)
                self._safety_tick(car_id, now)
                self._telemetry_tick(car_id, now)
            except Exception as e:
                logger.error(f"✗ correlation tick failed for car {car_id}: {e}")

    # ------------------------------------------------------------- drive thread
    def _drive_tick(self, car_id, now):
        c = self.car_data[car_id]
        d = self.drives.get(car_id)
        speed = _f(c.get('speed')) or 0.0
        pos = self._pos(car_id)
        if self._driving(car_id):
            if d is None:
                d = self.drives[car_id] = {
                    'start': now, 'last_moving': now, 'last_tick': now, 'start_pos': pos,
                    'start_place': self._place(car_id), 'start_odo': _f(c.get('odometer')),
                    'start_bat': _f(c.get('battery_level')), 'max_speed': 0.0, 'over_start': None,
                    'below_since': None, 'over_total': 0.0, 'episodes': 0, 'speed_alarm': False,
                    'speed_pos': None, 'speed_at': None, 'night': False, 'night_pos': None, 'night_at': None,
                    'alert_id': None, 'last_edit': 0}
                logger.info(f"🚗 Car {car_id}: drive started at {d['start_place']}")
                if car_id in self.incidents:
                    self._incident_close(car_id, now, 'car is driving')
            dt = min(5.0, now - d['last_tick'])
            d['last_tick'] = d['last_moving'] = now
            d['max_speed'] = max(d['max_speed'], speed)
            if speed > SPEED_ABS_LIMIT:
                d['below_since'] = None
                if d['over_start'] is None:
                    d['over_start'], d['over_start_pos'] = now, pos
                    d['episodes'] += 1
                d['over_total'] += dt
                if not d['speed_alarm'] and now - d['over_start'] >= SPEED_ABS_SUSTAIN:
                    # the link points to where this > limit stretch began; the time is when 5 min were reached
                    d['speed_alarm'], d['speed_pos'], d['speed_at'] = True, d.get('over_start_pos') or pos, now
                    self._drive_alarm(car_id, d, 'speed')
            elif d['over_start'] is not None:
                d['below_since'] = d['below_since'] or now
                if now - d['below_since'] >= SPEED_ABS_GAP:
                    d['over_start'] = d['below_since'] = None
            if not d['night'] and self._in_night(self._local(car_id, now)):
                d['night'], d['night_pos'], d['night_at'] = True, pos, now
                self._drive_alarm(car_id, d, 'night')
            if d['alert_id'] and now - d['last_edit'] >= LIVE_EDIT_INTERVAL:
                d['last_edit'] = now
                self.tg_edit(d['alert_id'], self._drive_text(car_id, d, live=True))
        elif d is not None and now - d['last_moving'] >= DRIVE_END_DELAY:
            self._drive_summary(car_id, d, now)
            del self.drives[car_id]

    def _distance(self, car_id, d):
        odo = _f(self.car_data[car_id].get('odometer'))
        if odo is not None and d.get('start_odo') is not None:
            return max(0.0, odo - d['start_odo'])
        pos = self._pos(car_id)
        return _km(d['start_pos'], pos) if None not in pos and None not in d['start_pos'] else 0.0

    def _drive_text(self, car_id, d, live=True):
        c = self.car_data[car_id]
        head = []
        if d['night']:
            head.append('🌙 <b>Night use</b>')
        if d['speed_alarm']:
            head.append(f"⚠️ <b>Speeding</b>: over {SPEED_ABS_LIMIT:.0f} km/h for {SPEED_ABS_SUSTAIN // 60}+ min")
        tz = self._local(car_id).tzname()
        lines = [' · '.join(head), f"🚗 <b>{_esc(self._name(car_id))}</b>" + (" is driving" if live else " · drive finished"),
                 f"🕐 Started {self._local(car_id, d['start'], d['start_pos']).strftime('%H:%M')} {tz} at {_esc(d['start_place'])}"]
        if d['night']:
            lines.append(f"🌙 Night use since {self._local(car_id, d['night_at'], d['night_pos']).strftime('%H:%M')} · " +
                         _link(d['night_pos'], '📍 where'))
        if d['speed_alarm']:
            lines.append(f"⚠️ Speeding confirmed at {self._local(car_id, d['speed_at'], d['speed_pos']).strftime('%H:%M')} · " +
                         _link(d['speed_pos'], '📍 where it started'))
        if live:
            lines.append(f"🏎️ Now {(_f(c.get('speed')) or 0):.0f} km/h · max {d['max_speed']:.0f} km/h")
            lines.append(f"🛣️ {self._distance(car_id, d):.1f} km so far · {_dur(self.clock() - d['start'])}")
        if d['over_total'] >= 1:
            lines.append(f"⏱️ Above {SPEED_ABS_LIMIT:.0f} km/h: {_dur(d['over_total'])} ({d['episodes']} stretch(es))")
        bat = _f(c.get('battery_level'))
        if bat is not None:
            lines.append(f"🔋 {bat:.0f}%")
        if live:
            lines.append(_link(self._pos(car_id), '🛰️ live position'))
            lines.append('<i>Updates here silently · summary follows as a reply</i>')
        else:
            lines.append('<i>Drive finished · see the reply</i>')
        return '\n'.join(l for l in lines if l)

    def _drive_alarm(self, car_id, d, reason):
        text = self._drive_text(car_id, d, live=True)
        if d['alert_id'] is None:
            d['alert_id'] = self.tg_send(text, silent=False, alert_type=f'drive_{reason}')   # the one ring
            logger.info(f"🔔 Car {car_id}: drive alarm ({reason})")
        else:
            self.tg_edit(d['alert_id'], text)                                                 # 2nd reason: no ring

    def _drive_summary(self, car_id, d, now):
        c = self.car_data[car_id]
        km = self._distance(car_id, d)
        secs = d['last_moving'] - d['start']
        avg = km / (secs / 3600) if secs > 60 else 0
        bat = _f(c.get('battery_level'))
        flags = (['🌙 night use'] if d['night'] else []) + (['⚠️ speeding'] if d['speed_alarm'] else [])
        end_pos = self._pos(car_id)
        lines = [f"🏁 <b>Drive finished</b> · {_esc(self._name(car_id))}" + (f" · {' · '.join(flags)}" if flags else ''),
                 f"🗺️ {_esc(d['start_place'])} → {_esc(self._place(car_id))}",
                 f"🕐 {self._local(car_id, d['start'], d['start_pos']).strftime('%H:%M')}–"
                 f"{self._local(car_id, d['last_moving']).strftime('%H:%M %Z')} ({_dur(secs)})",
                 f"🛣️ {km:.1f} km · avg {avg:.0f} km/h · max {d['max_speed']:.0f} km/h"]
        if d['over_total'] >= 1:
            lines.append(f"⏱️ Above {SPEED_ABS_LIMIT:.0f} km/h: {_dur(d['over_total'])} in {d['episodes']} stretch(es)")
        if d.get('start_bat') is not None and bat is not None:
            lines.append(f"🔋 {d['start_bat']:.0f}% → {bat:.0f}% ({bat - d['start_bat']:+.0f}%)")
        lines.append(_link(end_pos, '📍 parked here'))
        text = '\n'.join(l for l in lines if l)
        if d['alert_id']:
            self.tg_edit(d['alert_id'], self._drive_text(car_id, d, live=False))
            mid = self.tg_send(text, silent=True, reply_to=d['alert_id'], alert_type='drive_summary')
        else:
            mid = self.tg_send(text, silent=True, alert_type='drive_summary')
        if mid:
            self.last_summary[car_id] = {'id': mid, 'at': now}
        logger.info(f"🏁 Car {car_id}: drive summary ({km:.1f} km)")

    # ------------------------------------------------------------- parked incident
    def _incident_active(self, car_id, key):
        c = self.car_data[car_id]
        if key == 'doors_open':
            return any(_b(c.get(k)) for k in ('doors_open', 'windows_open', 'trunk_open', 'frunk_open'))
        if key == 'unlocked_no_user':
            return not _b(c.get('locked')) and not _b(c.get('is_user_present'))
        if key == 'locked_user':
            return _b(c.get('locked')) and _b(c.get('is_user_present'))
        if key == 'cabin_overheat':
            t = _f(c.get('inside_temp'))
            return t is not None and t >= getattr(self, 'cabin_threshold', 40) - 2
        if key == 'climate_left_on':
            return _b(c.get('is_climate_on'))
        return False

    def _incident_lines(self, car_id, inc):
        c = self.car_data[car_id]
        out = []
        for key in inc['keys']:
            label = INCIDENT_LABELS.get(key, key)
            if key == 'doors_open':
                open_items = [n for k, n in (('doors_open', 'doors'), ('windows_open', 'windows'), ('trunk_open', 'trunk'),
                                             ('frunk_open', 'frunk')) if _b(c.get(k))]
                if open_items:
                    label = '🚪 Open: ' + ', '.join(open_items)
            if key == 'cabin_overheat' and _f(c.get('inside_temp')) is not None:
                label += f": {_f(c.get('inside_temp')):.0f}°C" + (' · climate on' if _b(c.get('is_climate_on')) else ' · climate off')
            done = '' if self._incident_active(car_id, key) else ' ✅ <i>resolved</i>'
            out.append(label + done)
        return out

    def _incident_text(self, car_id, inc, closed=False):
        c = self.car_data[car_id]
        title = ('✅ <b>Resolved</b>' if closed else ('🚨' if inc['danger'] else '🅿️') + ' <b>Parked car needs attention</b>')
        lines = [f"{title} · {_esc(self._name(car_id))}",
                 f"📍 {_esc(inc['place'])} · since {self._local(car_id, inc['start'], inc['pos']).strftime('%H:%M %Z')}",
                 *self._incident_lines(car_id, inc)]
        bat = _f(c.get('battery_level'))
        if bat is not None and not closed:
            lines.append(f"🔋 {bat:.0f}%")
        lines.append(_link(inc['pos'], '📍 where it happened'))
        if not closed:
            lines.append('<i>Updates here silently · a reply follows when resolved</i>')
        return '\n'.join(l for l in lines if l)

    def incident_add(self, car_id, key):
        now = self.clock()
        inc = self.incidents.get(car_id)
        if inc is None:
            pos = self._pos(car_id)
            inc = {'id': None, 'start': now, 'keys': [key], 'pos': pos, 'place': self._place(car_id),
                   'danger': False, 'last_edit': 0, 'clear_since': None}
            inc['danger'] = {'locked_user', 'cabin_overheat'} <= set(inc['keys'])
            ring = now - self.last_incident_ring.get(car_id, 0) >= INCIDENT_RING_INTERVAL
            inc['id'] = self.tg_send(self._incident_text(car_id, inc), silent=not ring, alert_type=key)
            if ring:
                self.last_incident_ring[car_id] = now
            self.incidents[car_id] = inc
            logger.info(f"🅿️  Car {car_id}: incident opened ({key}, {'ring' if ring else 'silent'})")
            return
        if key not in inc['keys']:
            inc['keys'].append(key)
        inc['clear_since'] = None
        if not inc['danger'] and {'locked_user', 'cabin_overheat'} <= set(inc['keys']):
            inc['danger'] = True
            self.tg_send('🚨 <b>Someone inside and the cabin is hot</b>\n' + '\n'.join(self._incident_lines(car_id, inc)),
                         silent=False, reply_to=inc['id'], alert_type='incident_danger')
        inc['last_edit'] = now
        self.tg_edit(inc['id'], self._incident_text(car_id, inc))

    def _incident_tick(self, car_id, now):
        inc = self.incidents.get(car_id)
        if not inc:
            return
        if any(self._incident_active(car_id, k) for k in inc['keys']):
            inc['clear_since'] = None
            if now - inc['last_edit'] >= LIVE_EDIT_INTERVAL:
                inc['last_edit'] = now
                self.tg_edit(inc['id'], self._incident_text(car_id, inc))
            return
        inc['clear_since'] = inc['clear_since'] or now
        if now - inc['clear_since'] >= INCIDENT_CLEAR_DELAY:
            self._incident_close(car_id, now, 'all clear')

    def _incident_close(self, car_id, now, reason):
        inc = self.incidents.pop(car_id, None)
        if not inc:
            return
        self.tg_edit(inc['id'], self._incident_text(car_id, inc, closed=True))
        self.tg_send(f"✅ All clear after {_dur(now - inc['start'])} ({_esc(reason)})", silent=True, reply_to=inc['id'],
                     alert_type='incident_clear')
        logger.info(f"✅ Car {car_id}: incident closed ({reason})")

    # ------------------------------------------------------------- charging phone call 
    def _safe_centres(self):
        """Home/Work geofence centres from TeslaMate (refreshed hourly)."""
        if self.clock() - self._safe_loaded > 3600 and getattr(self, 'db_conn', None):
            try:
                with self.db_conn.cursor() as cur:
                    cur.execute('SELECT name, latitude, longitude, radius FROM geofences')
                    rows = cur.fetchall()
                self.db_conn.rollback()
                self._safe_places = [(n, float(la), float(lo), float(r)) for n, la, lo, r in rows if CHARGE_CALL_SAFE.search(n or '')]
                self._safe_loaded = self.clock()
            except Exception as e:
                logger.warning(f"geofence lookup failed: {e}")
                try:
                    self.db_conn.rollback()
                except Exception:
                    pass
        return self._safe_places

    def _at_safe_place(self, car_id):
        """At Home/Work: inside such a geofence, or within CHARGE_CALL_SAFE_M of its centre."""
        g = (self.car_data[car_id].get('geofence') or '').strip()
        if g and CHARGE_CALL_SAFE.search(g):
            return g
        pos = self._pos(car_id)
        for name, la, lo, r in self._safe_centres():
            if _km(pos, (la, lo)) * 1000 <= max(r, CHARGE_CALL_SAFE_M):
                return name
        return None

    def _pagerduty(self, action, dedup, summary=None, details=None, link=None, component='charging'):
        key = os.getenv('PAGERDUTY_ROUTING_KEY', '')
        body = {'routing_key': key, 'event_action': action, 'dedup_key': dedup}
        if action == 'trigger':
            body['payload'] = {'summary': summary[:1000], 'severity': 'critical', 'source': 'teslamate',
                               'component': component, 'custom_details': details or {}}
            if link:
                body['links'] = [{'href': link, 'text': 'Car location'}]
        if ALERTS_DRY_RUN:
            rec = {'method': 'pagerduty', 'action': action, 'dedup': dedup, 'summary': summary}
            if self.dry_sink is not None:
                self.dry_sink.append(rec)
            else:
                logger.info("DRY-RUN %s", rec)
            return 'sent (dry run)'
        if is_service_mode():   # not 'sent...': conditions stay pending and fire after service mode ends
            logger.info("service mode: PagerDuty %s %s muted", action, dedup)
            return 'muted (service mode)'
        if not key:
            logger.error("✗ PagerDuty: PAGERDUTY_ROUTING_KEY is not set")
            return 'NOT sent: PagerDuty key missing'
        for attempt in range(3):
            try:
                r = requests.post(PAGERDUTY_EVENTS_URL, json=body, timeout=15)
                if r.status_code == 202:
                    return 'sent'
                logger.error(f"✗ PagerDuty {action}: HTTP {r.status_code} {r.text[:200]}")
            except Exception as e:
                logger.error(f"✗ PagerDuty {action} failed (attempt {attempt + 1}): {e}")
            time.sleep(2 * (attempt + 1))
        return 'FAILED'

    def _charge_call_tick(self, car_id, now):
        c = self.car_data[car_id]
        cc = self.charge_calls.get(car_id)
        if is_service_mode():   # would latch cc['sent'] and swallow the call for the whole charge
            return
        if not _b(c.get('plugged_in')):
            if cc and cc.get('sent'):
                self._pagerduty('resolve', cc['dedup'])
                self.tg_send(f"🔌 Unplugged at {self.local_stamp(car_id, '%H:%M %Z')} · charging alarm closed",
                             silent=True, reply_to=cc.get('tg_id'), alert_type='charge_call_clear')
                logger.info(f"🔌 Car {car_id}: unplugged, charging call resolved")
            self.charge_calls.pop(car_id, None)
            return
        if cc is None:
            cc = self.charge_calls[car_id] = {'since': now, 'sent': False, 'dedup': f'teslamate-car{car_id}-charge-{int(now)}'}
        if cc['sent']:
            return
        soc = _f(c.get('battery_level'))
        pos = self._pos(car_id)
        if soc is None or soc < CHARGE_CALL_SOC or not _b(c.get('locked')) or None in pos or self._driving(car_id):
            return
        if self._at_safe_place(car_id):
            return
        place = self._place(car_id)
        link = f"https://www.google.com/maps?q={pos[0]:.5f},{pos[1]:.5f}"
        summary = (f"{self._name(car_id)} is charged to {soc:.0f}% at {place}, car locked. "
                   f"Unplug / move the car (idle fees).")
        result = self._pagerduty('trigger', cc['dedup'], summary,
                                 {'battery_level': soc, 'place': place, 'local_time': self.local_stamp(car_id),
                                  'latitude': pos[0], 'longitude': pos[1]}, link)
        cc['sent'] = True
        text = '\n'.join([
            f"📞 <b>Charging alarm</b> · {_esc(self._name(car_id))}",
            f"🔋 <b>{soc:.0f}%</b> (alarm at {CHARGE_CALL_SOC:.0f}%) · plugged in · car locked",
            f"📍 {_esc(place)} · not Home/Work",
            f"🕐 {self.local_stamp(car_id, '%H:%M %Z')}",
            f"☎️ Phone call (PagerDuty): {_esc(result)}",
            _link(pos, '📍 where the car is'),
        ])
        cc['tg_id'] = self.tg_send(text, silent=False, alert_type='charge_call')
        logger.info(f"📞 Car {car_id}: charging call at {soc:.0f}% ({result})")

    def _safety_tick(self, car_id, now):
        """One incident per tyre/door condition; retry failures and resolve on recovery."""
        c = self.car_data[car_id]
        conditions = {}
        for suffix, label in [('fl', 'Front left'), ('fr', 'Front right'),
                              ('rl', 'Rear left'), ('rr', 'Rear right')]:
            pressure = _f(c.get('tpms_pressure_' + suffix))
            # Missing/invalid readings cannot clear an existing incident.
            if pressure is not None and math.isfinite(pressure) and pressure >= 0:
                conditions['tyre_' + suffix] = (
                    pressure < TYRE_PAGERDUTY_BAR,
                    f'{label} tyre {pressure:.2f} bar '
                    f'({pressure * 14.5038:.1f} psi), below {TYRE_PAGERDUTY_BAR:g} bar',
                    {'tyre': label, 'pressure_bar': pressure, 'threshold_bar': TYRE_PAGERDUTY_BAR})
        if c.get('locked') in ('true', 'false') and c.get('doors_open') in ('true', 'false'):
            conditions['locked_doors'] = (
                _b(c['locked']) and _b(c['doors_open']),
                'Door open while car is locked',
                {'locked': _b(c['locked']), 'doors_open': _b(c['doors_open'])})
        self._dispatch_safety(car_id, conditions, now)

    def _telemetry_tick(self, car_id, now):
        snapshot = self.telemetry_watchdog.poll(car_id, now)
        conditions = self.telemetry_watchdog.conditions(
            car_id, self.car_data[car_id], now, snapshot,
            self.safety_calls.get(f'{car_id}:occupant_hot', {}).get('active', False))
        self._dispatch_safety(car_id, conditions, now)

    def _dispatch_safety(self, car_id, conditions, now):
        for condition, (active, summary, details) in conditions.items():
            key = f'{car_id}:{condition}'
            state = self.safety_calls.setdefault(key, {'active': False, 'retry_at': 0})
            if active == state['active'] or now < state['retry_at']:
                continue
            dedup = f'teslamate-car{car_id}-{condition}'
            result = self._pagerduty('trigger' if active else 'resolve', dedup,
                                    f'{self._name(car_id)}: {summary}', details, component='vehicle_safety')
            state['retry_at'] = now + 60
            if result.startswith('sent'):
                state['active'] = active
                state['retry_at'] = 0
                logger.info('PagerDuty %s %s: %s', condition, 'triggered' if active else 'resolved', result)
                self.save_state()

    # ------------------------------------------------------------- persistence
    def threads_state(self):
        return {'drives': self.drives, 'incidents': self.incidents, 'last_summary': self.last_summary,
                'last_incident_ring': self.last_incident_ring, 'charge_calls': self.charge_calls,
                'safety_calls': self.safety_calls}

    def threads_restore(self, st):
        if not st:
            return
        self.drives = st.get('drives', {})
        self.incidents = st.get('incidents', {})
        self.last_summary = st.get('last_summary', {})
        self.last_incident_ring = st.get('last_incident_ring', {})
        self.charge_calls = st.get('charge_calls', {})
        self.safety_calls = st.get('safety_calls', {})
