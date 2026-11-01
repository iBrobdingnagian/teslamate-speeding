"""Correlate fresh stored telemetry with live MQTT presence; no vehicle commands."""
import logging
import math
import os
from datetime import timezone
from contextlib import closing

import psycopg2

log = logging.getLogger(__name__)
DRIVE_STALE_S = int(os.getenv('DRIVE_STALE_S', '300'))
CHARGE_STALE_S = int(os.getenv('CHARGE_STALE_S', '600'))
CORRELATION_FRESH_S = int(os.getenv('CORRELATION_FRESH_S', '180'))
OCCUPANT_HOT_C = float(os.getenv('OCCUPANT_HOT_C', '40'))
OCCUPANT_CLEAR_C = OCCUPANT_HOT_C - 2
OCCUPANT_HOT_DWELL_S = int(os.getenv('OCCUPANT_HOT_DWELL_S', '60'))


def epoch(value):
    return value.replace(tzinfo=timezone.utc).timestamp() if value else None


def read_snapshot(car_id):
    """Use stored sample dates, not MQTT publication frequency, to measure freshness."""
    with closing(psycopg2.connect(host=os.getenv('DB_HOST', 'localhost'), port=os.getenv('DB_PORT', '5432'),
                          dbname=os.getenv('DB_NAME', 'teslamate'), user=os.getenv('DB_USER', 'teslamate'),
                          password=os.getenv('DB_PASSWORD', ''), connect_timeout=5,
                          options='-c statement_timeout=5000')) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute('''SELECT date, speed, inside_temp FROM positions
                           WHERE car_id=%s ORDER BY date DESC LIMIT 1''', (car_id,))
            p = cur.fetchone()
            snapshot = {'position_at': epoch(p[0]) if p else None,
                        'speed': float(p[1]) if p and p[1] is not None else None,
                        'temperature': float(p[2]) if p and p[2] is not None else None}
            cur.execute('''SELECT d.start_date, (SELECT max(p.date) FROM positions p WHERE p.drive_id=d.id)
                           FROM drives d WHERE car_id=%s AND end_date IS NULL
                           ORDER BY start_date DESC LIMIT 1''', (car_id,))
            drive = cur.fetchone()
            cur.execute('''SELECT c.start_date, ch.date, ch.charger_power
                           FROM charging_processes c
                           LEFT JOIN LATERAL (SELECT date, charger_power FROM charges
                              WHERE charging_process_id=c.id ORDER BY date DESC LIMIT 1) ch ON true
                           WHERE c.car_id=%s AND c.end_date IS NULL ORDER BY c.start_date DESC LIMIT 1''', (car_id,))
            charge = cur.fetchone()
            cur.execute('''SELECT state, start_date FROM states WHERE car_id=%s AND end_date IS NULL''', (car_id,))
            state = cur.fetchone()
            snapshot.update(drive_at=epoch(drive[1] or drive[0]) if drive else None,
                            charge_at=epoch(charge[1] or charge[0]) if charge else None,
                            charge_power=charge[2] if charge else None,
                            state=state[0] if state else None,
                            state_at=epoch(state[1]) if state else None)
            return snapshot


class TelemetryWatchdog:
    def __init__(self, clock):
        self.started = clock()
        self.snapshots = {}
        self.next_poll = {}
        self.live = {}
        self.hot_since = {}

    def observe(self, car_id, field, value, retained, now):
        # A retained value can be hours old. Never treat it as new presence evidence.
        if not retained and field in ('is_user_present', 'inside_temp', 'speed', 'state', 'shift_state', 'charging_state'):
            self.live.setdefault(car_id, {})[field] = (value, now)

    def conditions(self, car_id, data, now, snapshot, hot_active=False):
        result = {}
        if snapshot is None:
            self.hot_since.pop(car_id, None)
            return result  # Unknown never means recovered.
        live = self.live.get(car_id, {})
        def fresh(field):
            item = live.get(field)
            return item[0] if item and 0 <= now - item[1] <= CORRELATION_FRESH_S else None

        drive_at, charge_at = snapshot['drive_at'], snapshot['charge_at']
        # Explicit sleep after the last active sample is a normal terminal state.
        last_active = max((t for t in (drive_at, charge_at) if t is not None), default=None)
        sleeping = (snapshot['state'] in ('asleep', 'suspended') and snapshot['state_at'] is not None
                    and last_active is not None and snapshot['state_at'] >= last_active)
        charge_state = live.get('charging_state')
        charging_stopped = (charge_state is not None and charge_state[0] in
                            ('Complete', 'Stopped', 'Disconnected', 'NoPower')
                            and charge_at is not None and charge_state[1] >= charge_at)
        stale = []
        if not sleeping:
            if drive_at is not None and now - drive_at > DRIVE_STALE_S:
                stale.append(('driving', int(now - drive_at)))
            if (charge_at is not None and (snapshot['charge_power'] or 0) > 0
                    and not charging_stopped and now - charge_at > CHARGE_STALE_S):
                stale.append(('charging', int(now - charge_at)))
        # Allow a startup grace period; retained MQTT activity alone cannot open an incident.
        if now - self.started >= CORRELATION_FRESH_S:
            result['stale_telemetry'] = (
                bool(stale), 'No fresh vehicle readings while ' + ', '.join(f'{mode} ({age}s)' for mode, age in stale),
                {'stale_activity': stale, 'driving_threshold_s': DRIVE_STALE_S,
                 'charging_threshold_s': CHARGE_STALE_S})

        pos_at, temperature = snapshot['position_at'], snapshot['temperature']
        pos_fresh = pos_at is not None and 0 <= now - pos_at <= CORRELATION_FRESH_S
        # A parked car may publish a fresh temperature without adding a position row.
        temp_at = pos_at
        if fresh('inside_temp') is not None:
            try:
                temperature = float(fresh('inside_temp'))
                temp_at = live['inside_temp'][1]
            except (ValueError, TypeError):
                temperature = None
        presence = fresh('is_user_present')
        motion_fresh = (pos_fresh and snapshot['speed'] == 0) or fresh('speed') == '0'
        parked = (drive_at is None and snapshot['state'] == 'online' and motion_fresh
                  and data.get('state') != 'driving' and data.get('shift_state') not in ('D', 'R', 'N'))
        valid_temp = (temp_at is not None and 0 <= now - temp_at <= CORRELATION_FRESH_S
                      and temperature is not None and math.isfinite(temperature))
        dangerous = parked and presence == 'true' and valid_temp and temperature >= OCCUPANT_HOT_C
        if dangerous:
            since = self.hot_since.setdefault(car_id, now)
            if now - since >= OCCUPANT_HOT_DWELL_S:
                result['occupant_hot'] = (True, f'User-presence sensor active while parked; cabin {temperature:.1f}°C',
                                          {'temperature_c': temperature, 'presence': True,
                                           'temperature_age_s': now - temp_at,
                                           'presence_age_s': now - live['is_user_present'][1],
                                           'sustained_s': now - since})
        else:
            self.hot_since.pop(car_id, None)
            # Only affirmative fresh recovery evidence can clear the alarm.
            recovered = (presence == 'false' or (valid_temp and temperature <= OCCUPANT_CLEAR_C)
                         or (fresh('state') == 'driving' and pos_fresh and (snapshot['speed'] or 0) > 0))
            if hot_active and recovered:
                result['occupant_hot'] = (False, 'Fresh readings clear the parked hot-cabin condition', {})
        return result

    def poll(self, car_id, now):
        if now >= self.next_poll.get(car_id, 0):
            self.next_poll[car_id] = now + 60
            try:
                self.snapshots[car_id] = read_snapshot(car_id)
            except Exception as exc:
                log.error('Telemetry watchdog database check failed for car %s: %s', car_id, type(exc).__name__)
                self.snapshots[car_id] = None
        return self.snapshots.get(car_id)
