# teslamate-speeding

Real speed-limit violations for [TeslaMate](https://github.com/teslamate-org/teslamate), matched to the actual road instead of guessed from loose GPS points.

Each drive is thinned, snapped to the road network with the [TomTom Snap to Roads API](https://developer.tomtom.com/snap-to-roads-api/documentation/product-information/introduction), and compared with the speed limit of the road segment you were on. Every stretch above the limit becomes one incident with its path, road name, how far over you were and how much time you saved. Three Grafana dashboards read the results.

- **Per road, not per point.** The limit comes from the matched road segment, so one bad GPS fix cannot invent a violation.
- **Fits the free tier.** TomTom's free plan allows 2,500 Snap to Roads calls a month per key. Drives are thinned to one point per 25 m and sent 500 points per call. The client tracks usage per key, rotates keys when one is low or refused, and stops before the quota is gone.
- **Month-end backfill.** Quota left over at the end of the month is spent on older drives, so history fills in over time without paying for anything.
- **Autobahn aware.** TomTom reports "Recommended 130" for German autobahn stretches with no limit. That is advice, not law, so those stretches are stored separately and never counted as violations.
- **Resumable.** Every drive has a status row, so a run that stops halfway continues where it ended.
- **Notifications.** A full Telegram and PagerDuty alert system for TeslaMate (tyres, charging, doors, speeding, geofences, weather digest) that rings once per incident. See below.

## What you need

- A running TeslaMate with its PostgreSQL database and Grafana.
- Python 3 on any machine that can reach the database (developed and run on Python 3.12).
- A free TomTom developer key.

## Try it in two minutes (no Tesla account, no TomTom key)

```sh
git clone https://github.com/iBrobdingnagian/teslamate-speeding.git
cd teslamate-speeding
docker compose -f demo/docker-compose.yml up --build
```

This starts TeslaMate, PostgreSQL and Grafana, fills the database with about 150 synthetic drives and 160 speeding incidents, and loads the three dashboards. Open <http://localhost:3000> (login `admin` / `admin`) and look under Dashboards, folders Safety and Location. The routes are invented, so nothing in the demo refers to a real person or place. Stop it with `docker compose -f demo/docker-compose.yml down`.

## Install

```sh
git clone https://github.com/iBrobdingnagian/teslamate-speeding.git
cd teslamate-speeding
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env     # fill in the database login and your TomTom key(s)
```

Create the tables (safe to repeat):

```sh
python3 speeding/speeding_incidents.py --init-schema
```

See what would run without calling TomTom:

```sh
python3 speeding/speeding_incidents.py --car-id 1 --days 7 --dry-run
```

Process the last week:

```sh
python3 speeding/speeding_incidents.py --car-id 1 --days 7
```

Schedule it: see `crontab.example` (nightly run for new drives, plus a month-end backfill).

## Dashboards

The generator writes three dashboards: Real Speed Limit Violations, High Speed Tracking and Location-Based Speed Limits. Ready-made JSON is in `dashboards/provisioned/`. To regenerate it:

```sh
python3 dashboards/build_speeding_dashboards.py --out dashboards/provisioned
```

Mount `dashboards/provisioned` into the Grafana container (for example at `/dashboards_speeding`) and add the two providers from `dashboards/dashboards.yml.example` to your Grafana provisioning folder. Grafana picks the files up within 30 seconds. They use TeslaMate's default PostgreSQL data source, the one named `TeslaMate`.

Edit the generator, not the JSON, if you want to change the dashboards.

## How it decides what counts as speeding

| Rule | Value | Why |
|---|---|---|
| Thinning | one point per 25 m | fewer API calls, same road coverage |
| Chunk | 500 points per call, halved on HTTP 414 | long GET URLs can be refused |
| Tolerance | over by more than max(5 km/h, 5 % of the limit) | the displayed speed reads a few percent high |
| Merge gap | 10 s on the same limit | a brief dip must not split one incident |
| Minimum length | 5 s | a 1 to 2 second blip is noise |
| Limit change | starts a new incident | a new situation |
| More than 50 km/h over | dropped | the match or the limit is more likely wrong |
| Segment without a limit | no incident | roughly 3 % of segments |

The constants are at the top of `speeding/speeding_incidents.py`.

## Tables

`speeding_incidents` (one row per incident, path as GeoJSON), `speeding_drive_status` (which drives are done), `speeding_places` (street and town per rounded coordinate cell; this repository does not fill it, and the dashboards fall back to coordinates when it is empty), `speed_limit_overrides` (your own limits: add rows by hand), `speeding_runs` (month-end bookkeeping), `autobahn_stretches` (stretches with only a recommended speed), `api_usage_log` (TomTom calls per key).

## Weather

Each incident can carry the weather at that moment (condition, temperature, wind, rain, humidity, visibility), so you can see how much of your speeding happens in rain or fog. The weather panels of the dashboards use it.

Weather comes from the free [Open-Meteo](https://open-meteo.com) archive, which needs no API key. `weather/fetch_weather.py` looks up the weather for the drives in your TeslaMate database and stores it in a `weather_data` table.

```sh
psql -U teslamate teslamate -f weather/weather_data.sql      # once: creates the table
python3 weather/fetch_weather.py --days 30 --limit 200       # fetch weather for recent drives
SPEEDING_CORRELATE=weather python3 speeding/speeding_incidents.py --recorrelate-all
```

Set `SPEEDING_CORRELATE=weather` in `.env` to keep it on for every run. Open-Meteo is free for non-commercial use, so keep the volume modest.

## Optional: traffic and roadworks columns

`SPEEDING_CORRELATE=traffic` also fills traffic congestion, live flow and roadworks columns, but it reads tables that are not part of TeslaMate or of this repository (`traffic_correlation`, `traffic_flow_snapshots`, `traffic_incidents`). Use `weather,traffic` or `1` for both. Without them the roadworks and traffic panels stay empty and everything else works.

## Notifications: Telegram and PagerDuty

`alerts/unified_alert_mqtt.py` listens to TeslaMate's MQTT topics and sends Telegram messages. It is the notification system I run on my own TeslaMate server, with personal settings removed.

| Alert | What triggers it | Delivery |
|---|---|---|
| Low tyre pressure | a tyre below 2.0 bar (severity from caution to critical, all four tyres in the message) | rings; a PagerDuty page below 1.9 bar if configured |
| Charging | plugged in, unplugged, charge limit reached, summary at the end | silent, summary rings |
| Charge phone call | charged to 75 % (default), locked and away from home or work | PagerDuty phone call |
| Low range | range below 50 km | rings |
| Low power charging | plugged in but charging at very low power | rings |
| Doors, windows, trunk | open for more than 60 s | one message per parked incident |
| Unlocked, nobody inside / someone inside while locked | | parked incident |
| Cabin overheat, climate left on | cabin above 40 C for 2 minutes; climate running while parked | parked incident; someone inside plus hot always rings |
| Speeding | above 110 km/h for 5 minutes | one drive message, edited as the drive goes on |
| Night driving | car in use between 23:00 and 07:00, car's local time | one drive message |
| Geofence entry and exit, sentry mode, dog mode | | silent |
| Car started | heading change | folded into the drive message |
| Software update installing, completed | | silent ("available" is not sent by this daemon) |
| Parked weather digest | forecast when you park | silent reply to the drive summary |
| Stale telemetry | live MQTT data keeps arriving but TeslaMate has stopped storing a drive (5 min) or a charge (10 min); see `telemetry_watchdog.py` | part of the incident messages |

The idea is that the phone rings once per incident, not once per check: a drive or a parked incident is one message that is edited in place, and its end is a silent reply. `ALERTS_DRY_RUN=1` logs everything without sending. Service mode (`alerts/service_mode.py`) mutes every Telegram and PagerDuty push while the car is at a workshop.

Run it next to TeslaMate:

```sh
pip install -r requirements.txt
# put TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID (or TLG_API and TLG_CHAT) and MQTT_HOST in .env
python3 alerts/unified_alert_mqtt.py
```

The options (thresholds, quiet hours, PagerDuty keys) are environment variables, listed in `.env.example`. TeslaMate must publish to MQTT for any of this to work.

### PagerDuty

Set `PAGERDUTY_ROUTING_KEY` (an Events API v2 integration key) and the daemon opens an incident for a critically low tyre and for the charge phone call, with a dedup key so a repeat does not page twice.

`pagerduty/pagerduty_metrics.py` goes the other way: it copies your PagerDuty services, incidents and notification log into a `pagerduty` schema of the TeslaMate database, using a read-only REST key (`PAGERDUTY_API_KEY_RO`), so `pagerduty/pagerduty-dashboard.json` can chart incidents per day, time to acknowledge and resolve, and how much of the free plan's 100 phone/SMS notifications a month you have used. Run it every 5 minutes from cron.

### How well this is tested

Unit tests cover service mode and the alert routing. I also ran the daemon against a local MQTT broker with a fake Telegram server: a low-pressure reading produced the correctly formatted alert. The other alert types are the same code I use daily, but I have not re-tested each one for this repository. The PagerDuty collector and dashboard queries ran against sample rows, not against the live PagerDuty API.

## Limits and honest caveats

- Limits come from TomTom only. There is no second source to cross-check them, so treat a result as a strong hint, not evidence.
- Coverage of limits depends on the road. Some segments, especially small roads, have none.
- Your car reports speed in whole km/h with some rounding. The 5 % tolerance is there for that.
- This repository does not give legal advice. Do not use the output to contest a fine without checking the road yourself.

## Development

```sh
pip install -r requirements.txt pytest
python -m pytest
```

The tests cover the detection rules (tolerance, merge gap, minimum length, limit changes, advisory speeds), the TomTom response parsing and the chunking. CI also rebuilds the dashboards and fails if `dashboards/provisioned/` is out of date, so change the generator and commit its output.

## License

MIT, see `LICENSE`.
