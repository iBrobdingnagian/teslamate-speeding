#!/usr/bin/env python3
"""
Speeding incidents from TomTom-snapped drives.

Snaps each drive's GPS trace to the road network with TomTom's Snap to Roads API, reads the
speed limit of the matched road, and stores every stretch of driving above the limit as one row in
`speeding_incidents` (with its snapped path, road name and traffic/weather correlation).
`speeding_drive_status` records which drives are done, so every mode is resumable.

Modes:
  --drive-ids 1,2,3          process exactly these drives (testing)
  --days N                   unprocessed drives from the last N days (nightly job)
  --backfill                 all unprocessed drives, newest first
  --month-end                with --backfill: only run on the last day of the month, and spend at most
                             --budget-pct (default 90) % of the Snap to Roads quota still unused this month
  --stop-at-pct P            stop once P % of the monthly quota is used (keeps the nightly job modest)
  --max-calls N              hard cap on API calls in this run
  --dry-run                  show what would be done, call nothing
"""
import argparse
import calendar
import datetime
import json
import math
import os
import sys
import time
from collections import Counter
from contextlib import closing

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'teslamate')
DB_USER = os.getenv('DB_USER', 'teslamate')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

from tomtom_client import TOMTOM_API_KEYS, TOMTOM_QUOTAS, send_telegram, tomtom_request

PRODUCT = 'Snap to Roads API'
URL = "https://api.tomtom.com/snapToRoads/1"
FIELDS = ("{projectedPoints{type,geometry{type,coordinates},properties{routeIndex,snapResult}},"
          "route{type,geometry{type,coordinates},properties{id,speedLimits{value,unit,type},"
          "address{roadName,roadNumbers}}}}")
MIN_SPACING_M = 25      # keep a point only if it is at least this far from the last kept one
CHUNK = 500             # points per request (API allows 5000; long GET URLs can fail with 414)
MAX_GAP_M = 5000        # API limit is 6000 m between consecutive points
TOLERANCE_MIN_KMH = 5   # incident only when over by more than max(5 km/h, 5 % of the limit): the
TOLERANCE_PCT = 0.05    # car's displayed speed reads a few percent above true speed
MIN_DURATION_S = 5
MERGE_GAP_S = 10
MAX_OVER_KMH = 50       # above this the match or the limit is more likely wrong than the driver
MAX_ATTEMPTS = 3
PAUSE_S = 0.3           # stay well under TomTom's 5 requests/second

DDL = """
CREATE TABLE IF NOT EXISTS api_usage_log (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    product TEXT NOT NULL,
    called_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success BOOLEAN,
    status_code INTEGER,
    key_index INTEGER
);
CREATE INDEX IF NOT EXISTS idx_api_usage_provider_product_time ON api_usage_log(provider, product, called_at);
CREATE TABLE IF NOT EXISTS speeding_drive_status (
    drive_id INTEGER PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    api_calls INTEGER,
    points_sent INTEGER,
    matched_pct NUMERIC(5,1)
);
CREATE TABLE IF NOT EXISTS speeding_incidents (
    id SERIAL PRIMARY KEY,
    drive_id INTEGER NOT NULL,
    car_id SMALLINT,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    duration_s INTEGER,
    speed_limit INTEGER,
    max_speed INTEGER,
    avg_speed NUMERIC(5,1),
    max_over INTEGER,
    avg_over NUMERIC(5,1),
    tolerance_kmh INTEGER,
    road_name TEXT,
    road_numbers TEXT,
    start_lat NUMERIC(9,6), start_lon NUMERIC(9,6),
    end_lat NUMERIC(9,6), end_lon NUMERIC(9,6),
    path JSONB,
    limit_source TEXT NOT NULL DEFAULT 'tomtom',
    confidence TEXT NOT NULL DEFAULT 'tomtom_only',
    -- correlation: drive level (traffic_correlation / inferred_traffic)
    traffic_score NUMERIC, congestion_ratio NUMERIC, time_category TEXT, day_type TEXT,
    weather_main TEXT, temperature_c NUMERIC, wind_speed_ms NUMERIC, is_weekend BOOLEAN,
    hour_utc SMALLINT,
    -- correlation: nearest TomTom flow snapshot (within 10 min and ~3 km) and nearby traffic incidents
    flow_current_kmh NUMERIC, flow_free_kmh NUMERIC, flow_delay_pct NUMERIC, flow_snapshot_at TIMESTAMPTZ,
    traffic_incidents_nearby INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE speeding_incidents
    ADD COLUMN IF NOT EXISTS congestion_score NUMERIC,
    ADD COLUMN IF NOT EXISTS slow_traffic_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS rain_1h NUMERIC,
    ADD COLUMN IF NOT EXISTS humidity INTEGER,
    ADD COLUMN IF NOT EXISTS visibility_m INTEGER,
    ADD COLUMN IF NOT EXISTS roadworks_nearby BOOLEAN,
    ADD COLUMN IF NOT EXISTS distance_m INTEGER,
    ADD COLUMN IF NOT EXISTS time_saved_s NUMERIC(8,1),
    ADD COLUMN IF NOT EXISTS correlated_at TIMESTAMPTZ;
-- place name per ~100 m cell (filled by geocode_incident_places.py, TomTom Reverse Geocoding)
CREATE TABLE IF NOT EXISTS speeding_places (
    lat_key NUMERIC(6,3) NOT NULL,
    lon_key NUMERIC(6,3) NOT NULL,
    street TEXT, municipality TEXT, region TEXT, country TEXT, country_code TEXT, postal_code TEXT, label TEXT,
    geocoded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (lat_key, lon_key)
);
-- hand-maintained limits that override TomTom's, e.g. INSERT INTO speed_limit_overrides
--   (name, latitude, longitude, radius_m, limit_kmh, note) VALUES ('School street', 48.5933, 7.7531, 150, 30, 'zone 30');
CREATE TABLE IF NOT EXISTS speed_limit_overrides (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    radius_m INTEGER NOT NULL DEFAULT 150,
    limit_kmh INTEGER NOT NULL,
    note TEXT
);
-- stretches of road where TomTom gives only a recommended speed (130), i.e. no limit: German autobahn without a limit
CREATE TABLE IF NOT EXISTS autobahn_stretches (
    id SERIAL PRIMARY KEY,
    drive_id INTEGER NOT NULL,
    car_id SMALLINT,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    duration_s INTEGER,
    advisory_kmh INTEGER,
    max_speed INTEGER,
    avg_speed NUMERIC(5,1),
    secs_over_130 INTEGER,
    distance_m INTEGER,
    road_name TEXT,
    road_numbers TEXT,
    start_lat NUMERIC(9,6), start_lon NUMERIC(9,6),
    end_lat NUMERIC(9,6), end_lon NUMERIC(9,6),
    path JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE autobahn_stretches ADD COLUMN IF NOT EXISTS speed_hist JSONB;
CREATE INDEX IF NOT EXISTS autobahn_stretches_drive_idx ON autobahn_stretches (drive_id);
CREATE INDEX IF NOT EXISTS autobahn_stretches_start_idx ON autobahn_stretches (start_ts);
CREATE INDEX IF NOT EXISTS speeding_incidents_start_idx ON speeding_incidents (start_ts);
CREATE INDEX IF NOT EXISTS speeding_incidents_drive_idx ON speeding_incidents (drive_id);
-- one row per month-end backfill run, so a catch-up schedule never spends the quota twice
CREATE TABLE IF NOT EXISTS speeding_runs (
    kind TEXT NOT NULL,
    run_month DATE NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_calls INTEGER,
    drives_done INTEGER,
    PRIMARY KEY (kind, run_month)
);
"""


class OutOfBudget(Exception):
    pass


class Budget:
    def __init__(self, limit):
        self.limit = limit
        self.used = 0

    def spend(self):
        if self.limit is not None and self.used >= self.limit:
            raise OutOfBudget()
        self.used += 1


def connect():
    try:
        return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    except psycopg2.Error as e:
        print(f"ERROR: could not connect to database: {e}")
        sys.exit(1)


def hav(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def quota_status(cur, n_keys):
    """The monthly limit applies to each API key separately (tomtom_client rotates keys on 403/429), so the
    quota left is the sum of what every key has left. Returns (calls used in total, calls left in total)."""
    cur.execute("""SELECT COALESCE(key_index, 0), count(*) FROM api_usage_log
                   WHERE product = %s
                     AND called_at >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                   GROUP BY 1""", (PRODUCT,))
    per_key = dict(cur.fetchall())
    per_key_quota = TOMTOM_QUOTAS[PRODUCT]
    left = sum(max(per_key_quota - per_key.get(i, 0), 0) for i in range(n_keys))
    print("per key used this month:", {f"key {i + 1}": per_key.get(i, 0) for i in range(n_keys)},
          f"limit {per_key_quota} each")
    return sum(per_key.values()), left


def eligible_drives(cur, car_id, days=None, drive_ids=None, limit=100000):
    where, args = ["d.car_id = %s", "d.end_date IS NOT NULL", "d.distance >= 0.3"], [car_id]
    if drive_ids:
        where.append("d.id = ANY(%s)")
        args.append(drive_ids)
    else:
        where.append("(s.drive_id IS NULL OR (s.status = 'error' AND s.attempts < %s))")
        args.append(MAX_ATTEMPTS)
    if days:
        where.append("d.start_date >= now() AT TIME ZONE 'UTC' - make_interval(days => %s)")
        args.append(days)
    cur.execute(f"""SELECT d.id FROM drives d LEFT JOIN speeding_drive_status s ON s.drive_id = d.id
                    WHERE {' AND '.join(where)} ORDER BY d.start_date DESC LIMIT %s""", args + [limit])
    return [r[0] for r in cur.fetchall()]


def load_points(cur, drive_id):
    cur.execute("SELECT date, latitude, longitude, COALESCE(speed, 0) FROM positions "
                "WHERE drive_id = %s ORDER BY date", (drive_id,))
    rows = [dict(ts=r[0], lat=float(r[1]), lon=float(r[2]), speed=int(r[3])) for r in cur.fetchall()]
    if not rows:
        return []
    kept = [rows[0]]
    for r in rows[1:]:
        if hav(kept[-1]["lat"], kept[-1]["lon"], r["lat"], r["lon"]) >= MIN_SPACING_M:
            kept.append(r)
    return kept


def chunks(points):
    cur = []
    for p in points:
        if cur and (len(cur) >= CHUNK or hav(cur[-1]["lat"], cur[-1]["lon"], p["lat"], p["lon"]) > MAX_GAP_M):
            yield cur
            cur = [cur[-1]] if len(cur) >= CHUNK else []
        cur.append(p)
    if len(cur) >= 2:
        yield cur


def snap(chunk, budget):
    """Snap one chunk; on 414 (URL too long) retry as two halves. Returns parsed JSON list or None."""
    budget.spend()
    time.sleep(PAUSE_S)
    try:
        resp = tomtom_request(PRODUCT, 'GET', URL, params={
            'points': ";".join(f'{p["lon"]:.6f},{p["lat"]:.6f}' for p in chunk),
            'timestamps': ";".join(p["ts"].strftime('%Y-%m-%dT%H:%M:%SZ') for p in chunk),
            'fields': FIELDS, 'vehicleType': 'PassengerCar', 'measurementSystem': 'metric',
        }, timeout=60)
        return [(chunk, resp.json())]
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else None
        if code == 414 and len(chunk) > 20:
            mid = len(chunk) // 2
            a, b = snap(chunk[:mid + 1], budget), snap(chunk[mid:], budget)
            return None if a is None or b is None else a + b
        print(f"  snap failed: HTTP {code}")
        return None


def annotate(pairs):
    """Per-point records with the matched speed limit, road and snapped coordinate."""
    out, seen = [], set()
    for chunk, j in pairs:
        route, projected = j.get("route") or [], j.get("projectedPoints") or []
        for p, pp in zip(chunk, projected):
            key = (p["ts"], p["lat"], p["lon"])
            if key in seen:
                continue
            seen.add(key)
            props = pp.get("properties") or {}
            rec = dict(ts=p["ts"], speed=p["speed"], limit=None, advisory=None, road=None, numbers=None, coord=None)
            ri = props.get("routeIndex")
            if props.get("snapResult") == "Matched" and ri is not None and ri < len(route):
                rp = route[ri].get("properties") or {}
                sl = rp.get("speedLimits") or {}
                if sl.get("value"):
                    # type "Recommended" is TomTom's advisory speed (130 on German autobahn without a limit): not a limit
                    if str(sl.get("type") or "").lower() == "recommended":
                        rec["advisory"] = int(sl["value"])
                    else:
                        rec["limit"] = int(sl["value"])
                addr = rp.get("address") or {}
                rec["road"] = addr.get("roadName")
                rec["numbers"] = ",".join(addr.get("roadNumbers") or []) or None
                rec["coord"] = (pp.get("geometry") or {}).get("coordinates")
            out.append(rec)
    return out


def detect(recs):
    incidents, group = [], []

    def flush():
        if len(group) >= 2 and (group[-1]["ts"] - group[0]["ts"]).total_seconds() >= MIN_DURATION_S:
            incidents.append(list(group))
        group.clear()

    for r in recs:
        lim = r["limit"]
        tol = max(TOLERANCE_MIN_KMH, round(TOLERANCE_PCT * lim)) if lim else None
        over = r["speed"] - lim if lim else None
        is_over = lim is not None and r["coord"] is not None and tol < over <= MAX_OVER_KMH
        if not is_over:
            flush()
            continue
        if group and (group[0]["limit"] != lim or (r["ts"] - group[-1]["ts"]).total_seconds() > MERGE_GAP_S):
            flush()
        group.append(r)
    flush()
    return incidents


def detect_unlimited(recs):
    """Stretches on roads where TomTom only recommends a speed, i.e. no speed limit applies."""
    out, group = [], []
    def flush():
        if len(group) >= 2 and (group[-1]["ts"] - group[0]["ts"]).total_seconds() >= MIN_DURATION_S:
            out.append(list(group))
        group.clear()
    for r in recs:
        if r["advisory"] is None or r["coord"] is None:
            flush()
            continue
        if group and (r["ts"] - group[-1]["ts"]).total_seconds() > MERGE_GAP_S:
            flush()
        group.append(r)
    flush()
    return out


def store_unlimited(cur, drive_id, car_id, stretches):
    cur.execute("DELETE FROM autobahn_stretches WHERE drive_id = %s", (drive_id,))
    for g in stretches:
        roads = Counter(r["road"] for r in g if r["road"])
        nums = Counter(r["numbers"] for r in g if r["numbers"])
        coords = [r["coord"] for r in g]
        dist = sum(hav(a[1], a[0], b[1], b[0]) for a, b in zip(coords, coords[1:]))
        over = sum((b["ts"] - a["ts"]).total_seconds() for a, b in zip(g, g[1:]) if a["speed"] > 130)
        hist = Counter(str(r["speed"] // 10 * 10) for r in g if r["speed"] >= 100)
        cur.execute("""INSERT INTO autobahn_stretches
            (drive_id, car_id, start_ts, end_ts, duration_s, advisory_kmh, max_speed, avg_speed, secs_over_130, distance_m,
             road_name, road_numbers, start_lat, start_lon, end_lat, end_lon, path, speed_hist)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (drive_id, car_id, g[0]["ts"], g[-1]["ts"], int((g[-1]["ts"] - g[0]["ts"]).total_seconds()),
                     g[0]["advisory"], max(r["speed"] for r in g), round(sum(r["speed"] for r in g) / len(g), 1),
                     int(round(over)), int(round(dist)),
                     roads.most_common(1)[0][0] if roads else None, nums.most_common(1)[0][0] if nums else None,
                     coords[0][1], coords[0][0], coords[-1][1], coords[-1][0],
                     json.dumps({"type": "LineString", "coordinates": coords}), json.dumps(hist)))


def store(cur, drive_id, car_id, incidents):
    cur.execute("DELETE FROM speeding_incidents WHERE drive_id = %s", (drive_id,))
    new_ids = []
    for g in incidents:
        lim = g[0]["limit"]
        overs = [r["speed"] - lim for r in g]
        roads = Counter(r["road"] for r in g if r["road"])
        nums = Counter(r["numbers"] for r in g if r["numbers"])
        coords = [r["coord"] for r in g]
        # time saved: the same stretch at exactly the limit would have taken distance / limit; you took duration
        dist = sum(hav(a[1], a[0], b[1], b[0]) for a, b in zip(coords, coords[1:]))
        saved = round(dist / (lim / 3.6) - (g[-1]["ts"] - g[0]["ts"]).total_seconds(), 1)
        cur.execute("""INSERT INTO speeding_incidents
            (drive_id, car_id, start_ts, end_ts, duration_s, speed_limit, max_speed, avg_speed, max_over, avg_over,
             tolerance_kmh, road_name, road_numbers, start_lat, start_lon, end_lat, end_lon, path, distance_m, time_saved_s)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (drive_id, car_id, g[0]["ts"], g[-1]["ts"], int((g[-1]["ts"] - g[0]["ts"]).total_seconds()), lim,
                     max(r["speed"] for r in g), round(sum(r["speed"] for r in g) / len(g), 1), max(overs),
                     round(sum(overs) / len(overs), 1), max(TOLERANCE_MIN_KMH, round(TOLERANCE_PCT * lim)),
                     roads.most_common(1)[0][0] if roads else None, nums.most_common(1)[0][0] if nums else None,
                     coords[0][1], coords[0][0], coords[-1][1], coords[-1][0],
                     json.dumps({"type": "LineString", "coordinates": coords}), int(round(dist)), saved))
        new_ids.append(cur.fetchone()[0])
    correlate(cur, new_ids)


def pending_correlation_ids(cur):
    """Incidents never correlated, or recent ones still missing data that the nightly traffic/weather jobs
    fill in later (those tables are written after the drive, so a first pass can come up empty)."""
    cur.execute("""SELECT id FROM speeding_incidents
                   WHERE correlated_at IS NULL
                      OR (start_ts > now() AT TIME ZONE 'UTC' - interval '30 days'
                          AND (traffic_score IS NULL OR weather_main IS NULL OR congestion_ratio IS NULL
                               OR flow_snapshot_at IS NULL OR roadworks_nearby IS NULL))""")
    return [r[0] for r in cur.fetchall()]


def correlate_modes():
    """Which optional sources to join, from SPEEDING_CORRELATE: "weather", "traffic", both ("weather,traffic"),
    or "1"/"all" for both. Empty or "0" joins nothing. weather needs weather_data (see weather/); traffic needs
    traffic_correlation, traffic_flow_snapshots and traffic_incidents, which this repository does not provide."""
    raw = {m.strip().lower() for m in os.getenv("SPEEDING_CORRELATE", "0").split(",")}
    if raw & {"1", "all"}:
        return {"weather", "traffic"}
    return raw & {"weather", "traffic"}


def correlate(cur, ids):
    """Fill the correlation columns for these incidents. Never overwrites a value that is already set, so
    it is safe to repeat. Sources, best first: traffic_correlation (since May 2026), inferred_traffic and
    position_traffic_analysis (per drive), weather_data (nearest in time and place, for older drives),
    traffic_flow_snapshots (nearest within 10 min and ~3 km), traffic_incidents (same car, +-30 min)."""
    if not ids:
        return
    p = {"ids": list(ids)}
    modes = correlate_modes()
    if not modes:
        # nothing to join: the optional source tables are not part of TeslaMate
        cur.execute("UPDATE speeding_incidents SET correlated_at = now() WHERE id = ANY(%(ids)s)", p)
        return
    if "traffic" in modes:
        # inferred_traffic and position_traffic_analysis are views that aggregate every position; restricting them
        # to these drives (not just joining on drive_id) lets PostgreSQL aggregate only those drives.
        cur.execute("SELECT DISTINCT drive_id FROM speeding_incidents WHERE id = ANY(%(ids)s)", p)
        p["drives"] = [r[0] for r in cur.fetchall()]
        cur.execute("""UPDATE speeding_incidents s SET traffic_score = COALESCE(s.traffic_score, tc.traffic_score),
                       weather_main = COALESCE(s.weather_main, tc.weather_main),
                       temperature_c = COALESCE(s.temperature_c, tc.temperature_c),
                       wind_speed_ms = COALESCE(s.wind_speed_ms, tc.wind_speed_ms),
                       is_weekend = COALESCE(s.is_weekend, tc.is_weekend)
                       FROM traffic_correlation tc WHERE tc.drive_id = s.drive_id AND s.id = ANY(%(ids)s)""", p)
        cur.execute("""UPDATE speeding_incidents s SET congestion_ratio = COALESCE(s.congestion_ratio, it.congestion_ratio),
                       time_category = COALESCE(s.time_category, it.time_category),
                       day_type = COALESCE(s.day_type, it.day_type)
                       FROM inferred_traffic it WHERE it.drive_id = ANY(%(drives)s) AND it.drive_id = s.drive_id AND s.id = ANY(%(ids)s)""", p)
        cur.execute("""UPDATE speeding_incidents s SET congestion_score = COALESCE(s.congestion_score, pa.congestion_score),
                       slow_traffic_pct = COALESCE(s.slow_traffic_pct, pa.slow_traffic_percentage)
                       FROM position_traffic_analysis pa WHERE pa.drive_id = ANY(%(drives)s) AND pa.drive_id = s.drive_id AND s.id = ANY(%(ids)s)""", p)
    cur.execute("""UPDATE speeding_incidents SET hour_utc = EXTRACT(hour FROM start_ts)::smallint,
                   is_weekend = COALESCE(is_weekend, EXTRACT(isodow FROM start_ts) >= 6),
                   day_type = COALESCE(day_type, CASE WHEN EXTRACT(isodow FROM start_ts) >= 6
                                                      THEN 'weekend' ELSE 'weekday' END)
                   WHERE id = ANY(%(ids)s)""", p)
    if "weather" in modes:
        cur.execute("""UPDATE speeding_incidents s SET weather_main = COALESCE(s.weather_main, w.weather_main),
                       temperature_c = COALESCE(s.temperature_c, w.temperature),
                       wind_speed_ms = COALESCE(s.wind_speed_ms, w.wind_speed),
                       rain_1h = w.rain_1h, humidity = w.humidity, visibility_m = w.visibility
                       FROM (SELECT DISTINCT ON (s2.id) s2.id, w.weather_main, w.temperature, w.wind_speed, w.rain_1h,
                                    w.humidity, w.visibility
                             FROM speeding_incidents s2
                             JOIN weather_data w ON w.car_id = s2.car_id
                              AND abs(extract(epoch FROM (w.date - (s2.start_ts AT TIME ZONE 'UTC')))) <= 10800
                              AND abs(w.latitude - s2.start_lat) <= 0.5 AND abs(w.longitude - s2.start_lon) <= 0.7
                             WHERE s2.id = ANY(%(ids)s)
                             ORDER BY s2.id, abs(extract(epoch FROM (w.date - (s2.start_ts AT TIME ZONE 'UTC'))))) w
                       WHERE s.id = w.id""", p)
    if "traffic" in modes:
        cur.execute("""UPDATE speeding_incidents s SET flow_current_kmh = m.current_speed_kmh,
                       flow_free_kmh = m.free_flow_speed_kmh, flow_delay_pct = m.delay_pct, flow_snapshot_at = m.computed_at
                       FROM (SELECT DISTINCT ON (s2.id) s2.id, f.current_speed_kmh, f.free_flow_speed_kmh, f.delay_pct,
                                    f.computed_at
                             FROM speeding_incidents s2
                             JOIN traffic_flow_snapshots f ON f.car_id = s2.car_id
                              AND abs(extract(epoch FROM (f.computed_at - (s2.start_ts AT TIME ZONE 'UTC')))) <= 600
                              AND f.latitude BETWEEN s2.start_lat - 0.03 AND s2.start_lat + 0.03
                              AND f.longitude BETWEEN s2.start_lon - 0.05 AND s2.start_lon + 0.05
                             WHERE s2.id = ANY(%(ids)s) AND s2.flow_snapshot_at IS NULL
                             ORDER BY s2.id, abs(extract(epoch FROM (f.computed_at - (s2.start_ts AT TIME ZONE 'UTC'))))) m
                       WHERE s.id = m.id""", p)
        cur.execute("""UPDATE speeding_incidents s SET traffic_incidents_nearby = (
                           SELECT count(*) FROM traffic_incidents ti WHERE ti.car_id = s.car_id
                             AND ti.computed_at BETWEEN (s.start_ts AT TIME ZONE 'UTC') - interval '30 minutes'
                                                    AND (s.end_ts AT TIME ZONE 'UTC') + interval '30 minutes')
                       WHERE s.id = ANY(%(ids)s)""", p)
        # Roadworks / lane closure reported by TomTom Traffic Incident Details near the start of the incident
        # (+-30 min). NULL when we have no incident data for that time at all, so "no roadworks" is never guessed.
        cur.execute("""UPDATE speeding_incidents s SET roadworks_nearby = CASE
                       WHEN NOT EXISTS (SELECT 1 FROM traffic_incidents t0 WHERE t0.car_id = s.car_id
                            AND t0.computed_at BETWEEN (s.start_ts AT TIME ZONE 'UTC') - interval '30 minutes'
                                                   AND (s.end_ts AT TIME ZONE 'UTC') + interval '30 minutes') THEN NULL
                       ELSE EXISTS (SELECT 1 FROM traffic_incidents ti, jsonb_array_elements(ti.geometry->'coordinates') c
                            WHERE ti.car_id = s.car_id AND ti.icon_category IN (7, 9)
                              AND ti.geometry->>'type' = 'LineString'
                              AND ti.computed_at BETWEEN (s.start_ts AT TIME ZONE 'UTC') - interval '30 minutes'
                                                     AND (s.end_ts AT TIME ZONE 'UTC') + interval '30 minutes'
                              AND abs((c->>0)::float - s.start_lon::float) < 0.004
                              AND abs((c->>1)::float - s.start_lat::float) < 0.003) END
                       WHERE s.id = ANY(%(ids)s)""", p)
    cur.execute("UPDATE speeding_incidents SET correlated_at = now() WHERE id = ANY(%(ids)s)", p)


FILL_DISTANCE = """
UPDATE speeding_incidents s SET distance_m = round(d.m)::int,
       time_saved_s = round((d.m / (s.speed_limit / 3.6) - s.duration_s)::numeric, 1)
FROM (SELECT id, sum(2 * 6371000 * asin(sqrt(power(sin(radians(lat - plat) / 2), 2)
                 + cos(radians(plat)) * cos(radians(lat)) * power(sin(radians(lon - plon) / 2), 2)))) AS m
      FROM (SELECT i.id, (c.coord->>0)::float AS lon, (c.coord->>1)::float AS lat,
                   lag((c.coord->>0)::float) OVER w AS plon, lag((c.coord->>1)::float) OVER w AS plat
            FROM speeding_incidents i, jsonb_array_elements(i.path->'coordinates') WITH ORDINALITY AS c(coord, ord)
            WHERE i.distance_m IS NULL
            WINDOW w AS (PARTITION BY i.id ORDER BY c.ord)) t
      WHERE plon IS NOT NULL GROUP BY id) d
WHERE s.id = d.id AND s.speed_limit > 0
"""


def recorrelate(conn, everything=False):
    """Refresh correlation for incidents that are new or still incomplete (or all of them after a new
    correlation field was added). Makes no API calls."""
    with closing(conn.cursor()) as cur:
        if everything:
            cur.execute("SELECT id FROM speeding_incidents")
            ids = [r[0] for r in cur.fetchall()]
        else:
            ids = pending_correlation_ids(cur)
        correlate(cur, ids)
        # distance and time saved for incidents stored before those columns existed (or by an older run)
        cur.execute(FILL_DISTANCE)
        cur.execute("UPDATE speeding_incidents SET distance_m = 0, time_saved_s = 0 "
                    "WHERE distance_m IS NULL AND jsonb_array_length(path->'coordinates') < 2")
        cur.execute("""SELECT count(*), count(weather_main), count(congestion_ratio), count(traffic_score),
                              count(flow_snapshot_at) FROM speeding_incidents""")
        total, weather, congestion, score, flow = cur.fetchone()
    conn.commit()
    print(f"correlation refreshed for {len(ids)} incidents; of {total}: weather {weather}, "
          f"congestion {congestion}, traffic score {score}, live flow {flow}")
    return dict(total=total, weather=weather, congestion=congestion, flow=flow)


def process_drive(conn, drive_id, car_id, budget):
    """Returns (status, n_incidents). Raises OutOfBudget before starting a drive it cannot finish cheaply."""
    start_used = budget.used
    with closing(conn.cursor()) as cur:
        points = load_points(cur, drive_id)
    if len(points) < 2:
        return finish(conn, drive_id, "no_data", 0, 0, None, 0)
    if budget.limit is not None and budget.used + math.ceil(len(points) / CHUNK) > budget.limit:
        raise OutOfBudget()     # not enough budget to finish this drive: leave it for the next run
    pairs = []
    for ch in chunks(points):
        got = snap(ch, budget)
        if got is None:
            return finish(conn, drive_id, "error", budget.used - start_used, len(points), None, 0)
        pairs += got
    recs = annotate(pairs)
    matched = 100.0 * sum(1 for r in recs if r["coord"]) / max(len(recs), 1)
    incidents = detect(recs)
    with closing(conn.cursor()) as cur:
        store(cur, drive_id, car_id, incidents)
        store_unlimited(cur, drive_id, car_id, detect_unlimited(recs))
    return finish(conn, drive_id, "done", budget.used - start_used, len(recs), matched, len(incidents))


def finish(conn, drive_id, status, calls, pts, matched, n_inc):
    with closing(conn.cursor()) as cur:
        cur.execute("""INSERT INTO speeding_drive_status (drive_id, status, api_calls, points_sent, matched_pct)
                       VALUES (%s,%s,%s,%s,%s)
                       ON CONFLICT (drive_id) DO UPDATE SET processed_at = now(), status = EXCLUDED.status,
                         attempts = speeding_drive_status.attempts + 1, api_calls = EXCLUDED.api_calls,
                         points_sent = EXCLUDED.points_sent, matched_pct = EXCLUDED.matched_pct""",
                    (drive_id, status, calls, pts, matched))
    conn.commit()
    return status, n_inc


def is_last_day_of_month(now):
    return now.day == calendar.monthrange(now.year, now.month)[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--car-id", type=int, default=1)
    ap.add_argument("--drive-ids")
    ap.add_argument("--days", type=int)
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--month-end", action="store_true")
    ap.add_argument("--budget-pct", type=float, default=90.0)
    ap.add_argument("--stop-at-pct", type=float)
    ap.add_argument("--max-calls", type=int)
    ap.add_argument("--recorrelate", action="store_true", help="only refresh correlation columns (no API calls)")
    ap.add_argument("--recorrelate-all", action="store_true",
                    help="like --recorrelate but for every incident (after adding a correlation field)")
    ap.add_argument("--init-schema", action="store_true", help="create the tables the dashboards read, then exit")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if a.init_schema:
        conn = connect()
        with closing(conn.cursor()) as cur:
            cur.execute(DDL)
        conn.commit()
        print("speeding schema is in place")
        return
    a.recorrelate = a.recorrelate or a.recorrelate_all
    if not (a.drive_ids or a.days or a.backfill or a.recorrelate):
        ap.error("choose --drive-ids, --days, --backfill, --recorrelate or --init-schema")
    if not a.recorrelate and not TOMTOM_API_KEYS:
        print("ERROR: no TomTom API key configured")
        sys.exit(1)
    now = datetime.datetime.now()
    if a.month_end and not is_last_day_of_month(now):
        print(f"{now:%Y-%m-%d}: not the last day of the month, nothing to do")
        return

    quota = TOMTOM_QUOTAS[PRODUCT] * max(len(TOMTOM_API_KEYS), 1)    # total over all keys
    conn = connect()
    conn.autocommit = False
    with closing(conn.cursor()) as cur:
        cur.execute(DDL)
        conn.commit()
        if a.recorrelate:
            recorrelate(conn, everything=a.recorrelate_all)
            return
        this_month = datetime.date(now.year, now.month, 1)
        if a.month_end:
            cur.execute("SELECT finished_at FROM speeding_runs WHERE kind = 'month_end' AND run_month = %s",
                        (this_month,))
            row = cur.fetchone()
            if row:
                print(f"month-end backfill already ran this month ({row[0]:%Y-%m-%d %H:%M}), nothing to do")
                return
        used, remaining = quota_status(cur, len(TOMTOM_API_KEYS))
        ids = eligible_drives(cur, a.car_id, a.days, [int(x) for x in a.drive_ids.split(",")] if a.drive_ids else None)
    limit = a.max_calls
    if a.month_end:
        month_budget = int(remaining * a.budget_pct / 100)
        limit = month_budget if limit is None else min(limit, month_budget)
    if a.stop_at_pct is not None:
        cap = max(int(quota * a.stop_at_pct / 100) - used, 0)
        limit = cap if limit is None else min(limit, cap)

    print(f"{now:%Y-%m-%d %H:%M} quota={quota} used_this_month={used} remaining={remaining} "
          f"budget_this_run={'unlimited' if limit is None else limit} drives_to_do={len(ids)}")
    if a.dry_run:
        return
    if not ids:
        print("nothing left to process: everything is settled")

    budget = Budget(limit)
    done = incidents = errors = 0
    aborted = False
    try:
        for did in ids:
            if limit is not None and budget.used >= limit:
                raise OutOfBudget()
            status, n = process_drive(conn, did, a.car_id, budget)
            print(f"drive {did}: {status}, {n} incidents, calls so far {budget.used}")
            done += status == "done"
            errors += status == "error"
            incidents += n
    except OutOfBudget:
        print("budget for this run used up")
        conn.rollback()
    except RuntimeError as e:
        print(f"stopping: {e}")
        conn.rollback()
        aborted = True

    # Late-arriving traffic/weather data: refresh correlation on every run, whether or not drives were snapped.
    stats = recorrelate(conn)
    with closing(conn.cursor()) as cur:
        left = len(eligible_drives(cur, a.car_id, a.days, None) if not a.drive_ids else [])
        if a.month_end and not aborted:
            cur.execute("""INSERT INTO speeding_runs (kind, run_month, api_calls, drives_done)
                           VALUES ('month_end', %s, %s, %s) ON CONFLICT DO NOTHING""", (this_month, budget.used, done))
    conn.commit()
    print(f"finished: {done} drives done, {incidents} incidents, {errors} errors, {budget.used} calls, {left} drives left")
    if a.month_end and (done or errors or left):
        quota_left = max(quota - used - budget.used, 0)
        pct = lambda n: f"{round(100 * n / stats['total'])}%" if stats["total"] else "-"
        msg = (f"🛰 Speeding backfill (month end)\n━━━━━━━━━━━━━━━━━━\n"
               f"🚗 Drives processed: {done}\n⚠️ Incidents found: {incidents}\n"
               f"📡 TomTom Snap calls used: {budget.used} (budget {limit})\n"
               f"📦 Quota left this month: {quota_left} / {quota}\n"
               f"🔗 Correlation: weather {pct(stats['weather'])}, traffic {pct(stats['congestion'])}, "
               f"live flow {pct(stats['flow'])}\n"
               f"⏳ Drives still to do: {left}\n━━━━━━━━━━━━━━━━━━\n"
               + ("✅ Everything is settled." if left == 0 else "🔁 Continues on the last day of next month."))
        send_telegram(msg)


if __name__ == "__main__":
    main()
