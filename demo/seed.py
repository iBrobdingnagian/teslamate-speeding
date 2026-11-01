"""Fills a fresh TeslaMate database with synthetic drives and speeding incidents for the demo.

Everything here is invented: the routes are straight-ish lines between two points, the speeds are random.
It only exists so the dashboards have something to show without a Tesla account or a TomTom key.
"""
import datetime as dt
import json
import math
import os
import random
import sys
import time
from contextlib import closing

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "speeding"))
import speeding_incidents as si  # noqa: E402  (reuses the real schema and the real tolerance rule)

DAYS = 90
rng = random.Random(42)

# name, road numbers, start, end, speed limits along the road (km/h), town for the "place" column
ROUTES = [
    ("Rijksweg", "A2", (52.330, 4.900), (52.090, 5.110), [100, 120, 100], "Utrecht"),
    ("Rijksweg", "A12", (52.090, 5.110), (52.080, 4.330), [120, 100, 120], "Den Haag"),
    ("Provinciale weg", "N201", (52.300, 4.700), (52.150, 4.600), [80, 60, 80], "Aalsmeer"),
    ("Provinciale weg", "N205", (52.380, 4.640), (52.300, 4.700), [80, 80, 60], "Haarlem"),
    ("Stationsweg", None, (52.372, 4.893), (52.350, 4.930), [50, 50, 30], "Amsterdam"),
    ("Ringweg", "A10", (52.400, 4.900), (52.340, 4.850), [100, 80, 100], "Amsterdam"),
]
ADDRESSES = ["Dam 1, Amsterdam", "Domplein 1, Utrecht", "Binnenhof 1, Den Haag", "Grote Markt 1, Haarlem",
             "Zuidplein 1, Rotterdam", "Marktplein 1, Aalsmeer"]


def wait_for_teslamate():
    for _ in range(120):
        try:
            with closing(connect()) as conn, closing(conn.cursor()) as cur:
                cur.execute("SELECT count(*) FROM schema_migrations")
                if cur.fetchone()[0] >= 100:
                    return
        except psycopg2.Error:
            pass
        print("waiting for TeslaMate to create its tables...")
        time.sleep(3)
    sys.exit("TeslaMate did not create its tables in time")


def connect():
    return psycopg2.connect(host=os.getenv("DB_HOST", "database"), port=os.getenv("DB_PORT", "5432"),
                            dbname=os.getenv("DB_NAME", "teslamate"), user=os.getenv("DB_USER", "teslamate"),
                            password=os.getenv("DB_PASSWORD", "demo"))


def lerp(a, b, f):
    return (a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f)


def road_point(route, f):
    """Point at fraction f along a route, with a gentle bend so the lines do not look ruled."""
    p = lerp(route[2], route[3], f)
    bend = math.sin(f * math.pi) * 0.01
    return (p[0] + bend, p[1] - bend * 0.6)


def route_km(route):
    return si.hav(*route[2], *route[3]) / 1000 * 1.15


def main():
    wait_for_teslamate()
    with closing(connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(si.DDL)
        cur.execute("SELECT count(*) FROM speeding_incidents")
        if cur.fetchone()[0]:
            print("demo data is already there")
            return

        cur.execute("INSERT INTO car_settings DEFAULT VALUES RETURNING id")
        settings_id = cur.fetchone()[0]
        cur.execute("""INSERT INTO cars (eid, vid, model, efficiency, vin, name, inserted_at, updated_at, settings_id)
                       VALUES (1, 1, '3', 0.153, 'DEMO00000000000001', 'Demo Model 3', now(), now(), %s)
                       RETURNING id""", (settings_id,))
        car_id = cur.fetchone()[0]

        addr_ids = []
        for name in ADDRESSES:
            cur.execute("""INSERT INTO addresses (display_name, city, inserted_at, updated_at)
                           VALUES (%s, %s, now(), now()) RETURNING id""", (name, name.split(", ")[1]))
            addr_ids.append(cur.fetchone()[0])

        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)
        odometer = 18000.0
        places = {}
        for day in range(DAYS, 0, -1):
            date = now - dt.timedelta(days=day)
            n_drives = 1 if date.weekday() >= 5 else 2
            for k in range(n_drives):
                route = rng.choice(ROUTES)
                km = route_km(route) * rng.uniform(0.8, 1.1)
                start = date.replace(hour=7 + k * 9, minute=rng.randrange(60))
                speed_avg = rng.uniform(55, 95) if route[1] and route[1].startswith("A") else rng.uniform(35, 60)
                minutes = km / speed_avg * 60
                end = start + dt.timedelta(minutes=minutes)
                a, b = rng.sample(addr_ids, 2)

                incidents = []
                for _ in range(rng.choices([0, 1, 2, 3], weights=[30, 35, 25, 10])[0]):
                    f0 = rng.uniform(0.05, 0.85)
                    f1 = min(f0 + rng.uniform(0.03, 0.12), 0.97)
                    section = min(int(f0 * len(route[4])), len(route[4]) - 1)
                    limit = route[4][section]
                    tol = max(si.TOLERANCE_MIN_KMH, round(si.TOLERANCE_PCT * limit))
                    over = min(tol + 1 + int(rng.expovariate(1 / 9)), si.MAX_OVER_KMH)
                    incidents.append((f0, f1, section, limit, over, tol))

                speed_max = max([route[4][0] + 3] + [i[3] + i[4] for i in incidents])
                cur.execute("""INSERT INTO drives (start_date, end_date, car_id, distance, duration_min, speed_max,
                                                    start_km, end_km, outside_temp_avg, start_address_id, end_address_id)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                            (start, end, car_id, round(km, 2), round(minutes), speed_max, odometer, odometer + km,
                             round(rng.uniform(2, 22), 1), a, b))
                drive_id = cur.fetchone()[0]
                odometer += km

                for f0, f1, section, limit, over, tol in incidents:
                    p0, p1 = road_point(route, f0), road_point(route, f1)
                    coords = [[round(road_point(route, f0 + (f1 - f0) * s / 10)[1], 6),
                               round(road_point(route, f0 + (f1 - f0) * s / 10)[0], 6)] for s in range(11)]
                    dist = si.hav(*p0, *p1) * 1.05
                    max_speed = limit + over
                    avg_speed = limit + max(over - rng.uniform(1, 4), tol + 0.5)
                    duration = max(round(dist / (avg_speed / 3.6)), si.MIN_DURATION_S)
                    t0 = start + dt.timedelta(minutes=minutes * f0)
                    weather = rng.choices(["Clear", "Clouds", "Rain", "Fog", "Snow"], weights=[40, 30, 22, 5, 3])[0]
                    rain = round(rng.uniform(0.3, 4.0), 1) if weather == "Rain" else 0
                    visibility = rng.choice([800, 1500, 3000]) if weather == "Fog" else rng.choice([8000, 10000])
                    cur.execute("""INSERT INTO speeding_incidents
                        (drive_id, car_id, start_ts, end_ts, duration_s, speed_limit, max_speed, avg_speed, max_over,
                         avg_over, tolerance_kmh, road_name, road_numbers, start_lat, start_lon, end_lat, end_lon, path,
                         distance_m, time_saved_s, weather_main, temperature_c, wind_speed_ms, rain_1h, humidity,
                         visibility_m, roadworks_nearby, correlated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s, now())""",
                                (drive_id, car_id, t0, t0 + dt.timedelta(seconds=duration), duration, limit, max_speed,
                                 round(avg_speed, 1), over, round(avg_speed - limit, 1), tol, route[0], route[1],
                                 round(p0[0], 6), round(p0[1], 6), round(p1[0], 6), round(p1[1], 6),
                                 json.dumps({"type": "LineString", "coordinates": coords}), round(dist),
                                 round(dist / (limit / 3.6) - duration, 1), weather, round(rng.uniform(-2, 24), 1),
                                 round(rng.uniform(0.5, 11), 1), rain, rng.randint(45, 98), visibility,
                                 rng.random() < 0.12))
                    places[(round(p0[0], 3), round(p0[1], 3))] = (route[0], route[5])

                cur.execute("""INSERT INTO speeding_drive_status (drive_id, status, api_calls, points_sent, matched_pct)
                               VALUES (%s, 'done', %s, %s, %s)""",
                            (drive_id, max(1, round(km * 1000 / 25 / 500)), round(km * 1000 / 25),
                             round(rng.uniform(95, 99.5), 1)))

        for (lat, lon), (street, town) in places.items():
            cur.execute("""INSERT INTO speeding_places (lat_key, lon_key, street, municipality, country, country_code, label)
                           VALUES (%s, %s, %s, %s, 'Netherlands', 'NL', %s) ON CONFLICT DO NOTHING""",
                        (lat, lon, street, town, f"{street}, {town}"))
        conn.commit()
        cur.execute("SELECT (SELECT count(*) FROM drives), (SELECT count(*) FROM speeding_incidents)")
        print("seeded %d drives and %d incidents (synthetic)" % cur.fetchone())


if __name__ == "__main__":
    main()
