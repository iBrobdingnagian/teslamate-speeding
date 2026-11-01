#!/usr/bin/env python3
"""
Builds the three speeding dashboards (provisioned from files, so they survive every redeploy):

  safety/real-speed-limit-violations.json    uid real_speed_violations    Safety folder
  safety/speed-threshold-violations.json     uid speed_threshold_tracking Safety folder
  location/location-speed-limits.json        uid location_speed_limits    Location folder

All three read the tables written by speeding_incidents.py / geocode_incident_places.py
(speeding_incidents, speeding_places, speeding_drive_status, speed_limit_overrides).

The SQL below is a static template: @TOKENS@ are replaced by the fragments in TOKENS, and Grafana
fills in $variables and $__macros when the dashboard runs. Nothing user-supplied is ever spliced in.

  python3 build_speeding_dashboards.py --out ../provisioned          write the JSON files
  python3 build_speeding_dashboards.py --emit-sql queries.sql        dump every query (macros filled in) for testing
"""
import argparse
import json
import os

DS = {"type": "grafana-postgresql-datasource", "uid": "TeslaMate"}
DRIVE_DETAILS = "/d/zm7wN6Zgz/drive-details?orgId=1&var-car_id=${car_id}&var-drive_id=${__data.fields.drive_id}"

# ---------------------------------------------------------------- shared SQL fragments
TOKENS = {
    "@FILT@": "i.car_id = $car_id AND $__timeFilter(i.start_ts) AND i.max_over >= $min_over",
    "@PLACE_JOIN@": ("LEFT JOIN speeding_places p ON p.lat_key = round(i.start_lat, 3) "
                     "AND p.lon_key = round(i.start_lon, 3)"),
    "@PLACE@": "COALESCE(NULLIF(concat_ws(', ', p.street, p.municipality), ''), p.label)",
    "@ROAD@": "COALESCE(i.road_name, i.road_numbers, NULLIF(p.street, ''))",
    "@TRAFFIC@": ("CASE WHEN i.congestion_ratio IS NULL THEN 'unknown' WHEN i.congestion_ratio < 0.2 THEN '1 free-flowing' "
                  "WHEN i.congestion_ratio < 0.5 THEN '2 some traffic' ELSE '3 slow / congested' END"),
    "@DR@": "d.car_id = $car_id AND $__timeFilter(d.start_date) AND d.distance >= 0.3",
    "@OVERTHR@": "convert_km(d.speed_max::numeric, '$length_unit') > $speed_threshold",
    # per drive: worst incident, and incident count, for the drives in the time range
    "@CTE@": ("WITH worst AS (SELECT DISTINCT ON (i.drive_id) i.drive_id, i.max_speed, i.speed_limit, i.max_over, "
              "i.road_name, i.road_numbers, i.start_lat, i.start_lon, i.hour_utc, i.weather_main, i.congestion_ratio "
              "FROM speeding_incidents i WHERE i.car_id = $car_id AND i.start_ts >= $__timeFrom()::timestamp - interval '1 day' "
              "AND i.start_ts <= $__timeTo() ORDER BY i.drive_id, i.max_over DESC), "
              "cnt AS (SELECT drive_id, count(*) AS n FROM speeding_incidents WHERE car_id = $car_id "
              "AND start_ts >= $__timeFrom()::timestamp - interval '1 day' AND start_ts <= $__timeTo() GROUP BY 1) "),
    "@DIST_TO_OVERRIDE@": ("sqrt(power((i.start_lat::float - o.latitude::float) * 111320, 2) + "
                           "power((i.start_lon::float - o.longitude::float) * 111320 * cos(radians(o.latitude::float)), 2))"),
}
# tokens that contain other tokens are expanded last-to-first until nothing is left
TOKENS["@WHERE_NAME@"] = ("COALESCE(@PLACE@, concat(round(i.start_lat, 3), ', ', round(i.start_lon, 3)))")


def T(sql):
    for _ in range(3):
        for k, v in TOKENS.items():
            sql = sql.replace(k, v)
    assert "@" not in sql.replace("@@", ""), "unexpanded token in: " + sql[:120]
    return sql


# ---------------------------------------------------------------- panel helpers
_ids = iter(range(1, 1000))


def target(sql, fmt="table"):
    return {"datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": sql, "refId": "A"}


def base(kind, title, sql, x, y, w, h, desc="", fmt="table"):
    return {"id": next(_ids), "type": kind, "title": title, "description": desc, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": [target(sql, fmt)],
            "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}


def stat(title, sql, x, y, w=4, h=4, unit="none", desc="", color="semi-dark-red", decimals=None):
    p = base("stat", title, sql, x, y, w, h, desc)
    d = {"mappings": [], "unit": unit, "thresholds": {"mode": "absolute", "steps": [{"color": color, "value": None}]}}
    if decimals is not None:
        d["decimals"] = decimals
    p["fieldConfig"]["defaults"] = d
    p["options"] = {"colorMode": "value", "graphMode": "none", "justifyMode": "auto", "orientation": "horizontal",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "auto"}
    return p


def bars_over_time(title, sql, x, y, w, h, desc="", fmt="time_series"):
    p = base("timeseries", title, sql, x, y, w, h, desc, fmt)
    p["fieldConfig"]["defaults"] = {
        "color": {"mode": "palette-classic"}, "unit": "none",
        "custom": {"drawStyle": "bars", "fillOpacity": 80, "lineWidth": 1, "barAlignment": 0, "showPoints": "never",
                   "axisPlacement": "auto", "axisLabel": "", "spanNulls": False}}
    p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "none"}}
    return p


def barchart(title, sql, x, y, w, h, desc="", color="semi-dark-red", unit="none", horizontal=False):
    p = base("barchart", title, sql, x, y, w, h, desc)
    p["fieldConfig"]["defaults"] = {"color": {"mode": "fixed", "fixedColor": color}, "unit": unit,
                                    "custom": {"fillOpacity": 80, "lineWidth": 1, "axisPlacement": "auto"}}
    p["options"] = {"orientation": "horizontal" if horizontal else "auto", "xTickLabelRotation": -45,
                    "showValue": "auto", "stacking": "none", "barWidth": 0.9, "groupWidth": 0.8,
                    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
                    "tooltip": {"mode": "single", "sort": "none"}}
    return p


def bargauge(title, sql, x, y, w, h, desc="", color="semi-dark-red"):
    p = base("bargauge", title, sql, x, y, w, h, desc)
    p["fieldConfig"]["defaults"] = {"color": {"mode": "fixed", "fixedColor": color}, "min": 0,
                                    "thresholds": {"mode": "absolute", "steps": [{"color": color, "value": None}]}}
    p["options"] = {"displayMode": "gradient", "orientation": "horizontal", "showUnfilled": True, "valueMode": "color",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True}}
    return p


def table(title, sql, x, y, w, h, desc="", hide=(), units=None, links=None, widths=None):
    """links: {column: [{"title":..., "url":...}]}; units: {column: grafana unit}; widths: {column: px}."""
    p = base("table", title, sql, x, y, w, h, desc)
    p["fieldConfig"]["defaults"] = {"custom": {"align": "auto", "cellOptions": {"type": "auto"}, "filterable": True}}
    ov = []
    for col in hide:
        ov.append({"matcher": {"id": "byName", "options": col}, "properties": [{"id": "custom.hidden", "value": True}]})
    for col, unit in (units or {}).items():
        ov.append({"matcher": {"id": "byName", "options": col}, "properties": [{"id": "unit", "value": unit}]})
    for col, link in (links or {}).items():
        ov.append({"matcher": {"id": "byName", "options": col}, "properties": [{"id": "links", "value": link}]})
    for col, px in (widths or {}).items():
        ov.append({"matcher": {"id": "byName", "options": col}, "properties": [{"id": "custom.width", "value": px}]})
    p["fieldConfig"]["overrides"] = ov
    p["options"] = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    return p


def _geomap(title, sql, x, y, w, h, desc, layer):
    p = base("geomap", title, sql, x, y, w, h, desc)
    p["fieldConfig"]["defaults"] = {
        "color": {"mode": "thresholds"},
        "thresholds": {"mode": "absolute", "steps": [
            {"color": "yellow", "value": None}, {"color": "orange", "value": 10},
            {"color": "red", "value": 20}, {"color": "dark-red", "value": 30}]}}
    p["options"] = {"view": {"id": "fit", "allLayers": True, "lat": 48, "lon": 8, "zoom": 6},
                    "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True,
                                 "showScale": True},
                    "basemap": {"type": "osm-standard", "name": "Basemap"},
                    "layers": [layer], "tooltip": {"mode": "details"}}
    return p


COORDS = {"mode": "coords", "latitude": "latitude", "longitude": "longitude"}


def geomap_markers(title, sql, x, y, w, h, desc="", size_field="over", min_size=4, max_size=16):
    layer = {"type": "markers", "name": title, "tooltip": True, "location": COORDS,
             "config": {"showLegend": True, "style": {
                 "size": {"field": size_field, "fixed": 6, "min": min_size, "max": max_size},
                 "color": {"field": size_field, "fixed": "dark-red"}, "opacity": 0.75,
                 "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"}}}}
    return _geomap(title, sql, x, y, w, h, desc, layer)


def geomap_route(title, sql, x, y, w, h, desc=""):
    layer = {"type": "route", "name": title, "tooltip": True, "location": COORDS,
             "config": {"style": {"size": {"fixed": 5, "min": 2, "max": 10}, "color": {"fixed": "red"},
                                  "opacity": 0.9, "arrow": 1}}}
    return _geomap(title, sql, x, y, w, h, desc, layer)


# ---------------------------------------------------------------- variables
def query_var(name, label, sql, hide=2, refresh=1, sort=0):
    return {"name": name, "label": label, "type": "query", "datasource": DS, "definition": sql, "query": sql,
            "hide": hide, "refresh": refresh, "includeAll": False, "multi": False, "options": [], "regex": "",
            "sort": sort, "current": {}}


CAR = query_var(
    "car_id", "Car",
    "SELECT\n    id as __value,\n    CASE WHEN COUNT(id) OVER (PARTITION BY name) > 1 AND name IS NOT NULL "
    "THEN CONCAT(name, ' - ', RIGHT(vin, 6)) ELSE COALESCE(name, CONCAT('VIN ', vin)) end as __text \n"
    "FROM cars\nORDER BY display_priority ASC, name ASC, vin ASC;", hide=0, refresh=1)
LENGTH_UNIT = query_var("length_unit", "", "select unit_of_length from settings limit 1;")
SPEED_UNIT = query_var("speed_unit", "",
                       "SELECT CASE WHEN '$length_unit' = 'km' THEN 'km/h' WHEN '$length_unit' = 'mi' THEN 'mph' END")
BASE_URL = query_var("base_url", "", "select base_url from settings limit 1;")


def custom_var(name, label, options, default, description):
    return {"name": name, "label": label, "type": "custom", "hide": 0, "description": description,
            "query": ",".join(str(o) for o in options), "includeAll": False, "multi": False,
            "current": {"text": str(default), "value": str(default), "selected": True},
            "options": [{"text": str(o), "value": str(o), "selected": o == default} for o in options]}


def min_over_var():
    return custom_var("min_over", "Only over by at least", [0, 5, 10, 15, 20, 30], 0,
                      "Hide incidents that exceeded the limit by less than this (km/h). The pipeline already "
                      "ignores anything within max(5 km/h, 5 %) of the limit.")


def dashboard(uid, title, desc, panels, variables, tags):
    return {"annotations": {"list": []}, "description": desc, "editable": True, "fiscalYearStartMonth": 0,
            "graphTooltip": 1, "links": [{"title": "Speeding dashboards", "type": "dashboards", "tags": ["speeding"],
                                          "asDropdown": True, "includeVars": True, "keepTime": True}],
            "panels": panels, "preload": False, "refresh": "", "schemaVersion": 40, "tags": tags,
            "templating": {"list": variables}, "time": {"from": "now-30d", "to": "now"}, "timepicker": {},
            "timezone": "", "title": title, "uid": uid, "version": 1, "weekStart": ""}


def link_path(uid):
    return [{"title": "Show this incident's path", "url":
             "/d/" + uid + "?${__url_time_range}&var-car_id=${car_id}&var-min_over=${min_over}"
             "&var-incident=${__data.fields.id}"}]


def path_query(var, by_drive=False):
    """The snapped path of one incident: chosen directly ($var = incident id) or, for a drive, its worst incident."""
    if by_drive:
        pick = "(SELECT id FROM speeding_incidents WHERE drive_id = NULLIF('$" + var + "', '')::int ORDER BY max_over DESC LIMIT 1)"
    else:
        pick = "NULLIF('$" + var + "', '')::int"
    return ("SELECT c.ord::int AS idx, (c.coord->>1)::float AS latitude, (c.coord->>0)::float AS longitude, "
            "i.max_over AS over FROM speeding_incidents i, "
            "jsonb_array_elements(i.path->'coordinates') WITH ORDINALITY AS c(coord, ord) "
            "WHERE i.id = " + pick + " ORDER BY c.ord")


# ================================================================ 1. Real Speed Limit Violations
def build_violations():
    uid = "real_speed_violations"
    incident_var = query_var(
        "incident", "Incident on path map",
        "SELECT id AS __value, concat(to_char(start_ts, 'YYYY-MM-DD HH24:MI'), '  ', "
        "COALESCE(road_name, road_numbers, 'road'), '  ', max_speed, ' in ', speed_limit) AS __text "
        "FROM speeding_incidents WHERE car_id = $car_id AND $__timeFilter(start_ts) "
        "ORDER BY max_over DESC, start_ts DESC LIMIT 300", hide=0, refresh=2)
    q = {}
    q["count"] = T("SELECT count(*) AS incidents FROM speeding_incidents i WHERE @FILT@")
    q["per100"] = T("SELECT round((100.0 * (SELECT count(*) FROM speeding_incidents i WHERE @FILT@) / "
                    "NULLIF((SELECT sum(distance) FROM drives WHERE car_id = $car_id AND $__timeFilter(start_date)), 0))::numeric, 2) "
                    "AS per_100_km")
    q["worst"] = T("SELECT convert_km(max(i.max_over)::numeric, '$length_unit') AS \"worst_$length_unit\" "
                   "FROM speeding_incidents i WHERE @FILT@")
    q["avg"] = T("SELECT convert_km(avg(i.max_over)::numeric, '$length_unit') AS \"avg_$length_unit\" "
                 "FROM speeding_incidents i WHERE @FILT@")
    q["time"] = T("SELECT coalesce(sum(i.duration_s), 0) AS seconds FROM speeding_incidents i WHERE @FILT@")
    q["saved"] = T("SELECT coalesce(sum(i.time_saved_s), 0) AS seconds FROM speeding_incidents i WHERE @FILT@")
    q["distover"] = T("SELECT convert_km((coalesce(sum(i.distance_m), 0) / 1000.0)::numeric, '$length_unit') AS \"over_$length_unit\" "
                      "FROM speeding_incidents i WHERE @FILT@")
    q["avgsaved"] = T("SELECT round(coalesce(avg(i.time_saved_s), 0)::numeric, 1) AS seconds FROM speeding_incidents i WHERE @FILT@")
    q["bestsaved"] = T("SELECT coalesce(max(i.time_saved_s), 0) AS seconds FROM speeding_incidents i WHERE @FILT@")
    q["coverage"] = T("SELECT round(100.0 * count(*) FILTER (WHERE s.status = 'done') / NULLIF(count(*), 0), 1) AS checked "
                      "FROM drives d LEFT JOIN speeding_drive_status s ON s.drive_id = d.id "
                      "WHERE d.car_id = $car_id AND $__timeFilter(d.start_date) AND d.distance >= 0.3")
    q["map"] = T("SELECT i.id, i.start_ts AS time, i.start_lat::float AS latitude, i.start_lon::float AS longitude, "
                 "convert_km(i.max_over::numeric, '$length_unit') AS over, @ROAD@ AS road, @PLACE@ AS place, "
                 "i.speed_limit AS \"limit\", i.max_speed AS speed, i.duration_s AS seconds "
                 "FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ ORDER BY i.max_over ASC LIMIT 5000")
    q["path"] = path_query("incident")
    q["perday"] = T("SELECT date_trunc('day', i.start_ts) AS time, count(*) AS \"Incidents\" FROM speeding_incidents i "
                    "WHERE @FILT@ GROUP BY 1 ORDER BY 1")
    q["dist"] = T("SELECT convert_km((ceil(i.max_over / 5.0) * 5)::numeric, '$length_unit') AS \"Over the limit by (up to)\", "
                  "count(*) AS \"Incidents\" FROM speeding_incidents i WHERE @FILT@ GROUP BY ceil(i.max_over / 5.0) "
                  "ORDER BY ceil(i.max_over / 5.0)")
    q["table"] = T("SELECT i.id, i.drive_id, i.start_ts AS time, @ROAD@ AS road, @PLACE@ AS place, "
                   "i.speed_limit AS \"limit_$length_unit\", i.max_speed AS \"speed_$length_unit\", "
                   "convert_km(i.max_over::numeric, '$length_unit') AS \"over_$length_unit\", i.duration_s AS \"seconds\", i.time_saved_s AS \"saved_s\", "
                   "CASE WHEN i.roadworks_nearby THEN 'roadworks reported' WHEN i.roadworks_nearby IS NULL THEN '' "
                   "ELSE 'no roadworks' END AS roadworks, @TRAFFIC@ AS traffic, i.weather_main AS weather, "
                   "i.confidence, i.start_lat::float AS latitude, i.start_lon::float AS longitude "
                   "FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ ORDER BY i.start_ts DESC LIMIT 2000")
    q["zones"] = T("SELECT convert_km(i.speed_limit::numeric, '$length_unit') AS \"limit_$length_unit\", count(*) AS incidents, "
                   "round(avg(i.max_over), 1) AS \"avg_over\", max(i.max_over) AS \"max_over\", "
                   "coalesce(sum(i.duration_s), 0) AS seconds FROM speeding_incidents i WHERE @FILT@ "
                   "GROUP BY i.speed_limit ORDER BY incidents DESC")
    q["places"] = T("SELECT @WHERE_NAME@ AS place, count(*) AS incidents FROM speeding_incidents i @PLACE_JOIN@ "
                    "WHERE @FILT@ GROUP BY 1 ORDER BY incidents DESC LIMIT 15")
    q["traffic"] = T("SELECT @TRAFFIC@ AS \"Traffic\", count(*) AS \"Incidents\" FROM speeding_incidents i WHERE @FILT@ "
                     "GROUP BY 1 ORDER BY 1")
    q["weather"] = T("SELECT coalesce(i.weather_main, 'unknown') AS \"Weather\", count(*) AS \"Incidents\" "
                     "FROM speeding_incidents i WHERE @FILT@ GROUP BY 1 ORDER BY 2 DESC")
    q["hour"] = T("WITH inc AS (SELECT i.hour_utc AS h, count(*) AS n FROM speeding_incidents i WHERE @FILT@ GROUP BY 1), "
                  "km AS (SELECT extract(hour FROM start_date)::int AS h, sum(distance) AS km FROM drives "
                  "WHERE car_id = $car_id AND $__timeFilter(start_date) GROUP BY 1) "
                  "SELECT km.h AS \"Hour of day (UTC)\", round((100.0 * coalesce(inc.n, 0) / NULLIF(km.km, 0))::numeric, 2) AS "
                  "\"Incidents per 100 km\" FROM km LEFT JOIN inc ON inc.h = km.h ORDER BY 1")
    q["daytype"] = T("WITH inc AS (SELECT CASE WHEN i.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS d, count(*) AS n "
                     "FROM speeding_incidents i WHERE @FILT@ GROUP BY 1), "
                     "km AS (SELECT CASE WHEN extract(isodow FROM start_date) >= 6 THEN 'Weekend' ELSE 'Weekday' END AS d, "
                     "sum(distance) AS km FROM drives WHERE car_id = $car_id AND $__timeFilter(start_date) GROUP BY 1) "
                     "SELECT km.d AS \"Day type\", round((100.0 * coalesce(inc.n, 0) / NULLIF(km.km, 0))::numeric, 2) AS "
                     "\"Incidents per 100 km\" FROM km LEFT JOIN inc ON inc.d = km.d ORDER BY 1")

    panels = [
        stat("Incidents", q["count"], 0, 0, desc="Speeding incidents in the selected time range: one continuous stretch "
             "above the limit on one road (gaps under 10 s are merged, anything within max(5 km/h, 5 %) of the limit "
             "is ignored). Limits come from TomTom Snap to Roads."),
        stat("Per 100 km driven", q["per100"], 4, 0, color="orange", decimals=2),
        stat("Worst overage", q["worst"], 8, 0, unit="velocitykmh", color="dark-red",
             desc="Highest speed over the limit within one incident."),
        stat("Average overage", q["avg"], 12, 0, unit="velocitykmh", color="orange", decimals=1),
        stat("Time spent speeding", q["time"], 16, 0, unit="s", color="yellow"),
        stat("Drives checked", q["coverage"], 20, 0, unit="percent", color="blue", decimals=1,
             desc="Share of drives in range already matched to the road network. Unchecked drives are filled in by "
                  "the nightly job and by the month-end backfill that spends the unused monthly TomTom quota."),
        geomap_markers("Where it happened", q["map"], 0, 4, 14, 13,
                       desc="One marker per incident at its start; size and colour show how far over the limit. "
                            "Positions are snapped to the road, so they sit on roads."),
        geomap_route("Incident path", q["path"], 14, 4, 10, 13,
                     desc="The snapped path of the incident chosen in 'Incident on path map' above, or click "
                          "'Show this incident's path' on a row of the table."),
        stat("Time saved by speeding", q["saved"], 0, 17, w=6, unit="s", color="green",
             desc="For each incident: the time the same stretch would have taken at exactly the limit, minus the time it took, "
                  "added up over the range. It is what speeding actually bought you."),
        stat("Distance driven over the limit", q["distover"], 6, 17, w=6, color="orange", decimals=0,
             desc="Total length of the stretches where you were over the limit."),
        stat("Saved per incident (average)", q["avgsaved"], 12, 17, w=6, unit="s", color="blue", decimals=1,
             desc="Usually a few seconds: speeding rarely saves much time over a short stretch."),
        stat("Biggest single saving", q["bestsaved"], 18, 17, w=6, unit="s", color="yellow",
             desc="The longest stretch gave the largest saving."),
        bars_over_time("Incidents per day", q["perday"], 0, 21, 12, 8,
                       desc="Drag across bars to zoom the whole dashboard to those days; the map and table follow."),
        barchart("How far over the limit", q["dist"], 12, 21, 12, 8, desc="Incidents by overage, in 5-unit steps."),
        table("Incidents", q["table"], 0, 29, 24, 12,
              desc="Newest first. Click a time to show its path on the map, or the road to open the drive in TeslaMate.",
              hide=("id", "drive_id", "latitude", "longitude"), units={"seconds": "s", "saved_s": "s"},
              links={"time": link_path(uid), "road": [{"title": "Open drive in TeslaMate", "url": DRIVE_DETAILS}]},
              widths={"time": 160}),
        table("Violations by speed limit", q["zones"], 0, 41, 12, 8, units={"seconds": "s"},
              desc="Which limits you exceed most often."),
        bargauge("Top places", q["places"], 12, 41, 12, 8,
                 desc="Most frequent incident locations (about 100 m cells), named with TomTom Reverse Geocoding."),
        barchart("Traffic when speeding", q["traffic"], 0, 49, 6, 8,
                 desc="From the drive's congestion estimate (free-flowing to slow). Counts, not rates.", color="orange"),
        barchart("Weather when speeding", q["weather"], 6, 49, 6, 8,
                 desc="Weather at the time (counts). Older drives use the nearest stored weather reading.",
                 color="blue"),
        barchart("Rate by hour of day", q["hour"], 12, 49, 6, 8,
                 desc="Incidents per 100 km driven, by the hour the drive started (UTC), so busy hours are not "
                      "mistaken for risky ones.", color="purple"),
        barchart("Weekday vs weekend", q["daytype"], 18, 49, 6, 8,
                 desc="Incidents per 100 km driven.", color="green"),
    ]
    return "safety/real-speed-limit-violations.json", q, dashboard(
        uid, "Real Speed Limit Violations",
        "Speeding incidents found by snapping each drive to the road network (TomTom Snap to Roads) and comparing "
        "your speed with the limit of the road you were on. Data: speeding_incidents.",
        panels, [CAR, LENGTH_UNIT, SPEED_UNIT, BASE_URL, min_over_var(), incident_var], ["tesla", "safety", "speeding"])


# ================================================================ 2. High Speed Tracking
def build_high_speed():
    uid = "speed_threshold_tracking"
    thr = custom_var("speed_threshold", "Speed threshold", [50, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160], 130,
                     "A drive counts as high-speed when its top speed is above this (km/h or mph, per your unit setting).")
    fast_var = query_var(
        "fast_drive", "Drive on path map",
        T("SELECT d.id AS __value, concat(to_char(d.start_date, 'YYYY-MM-DD HH24:MI'), '  top ', "
          "convert_km(d.speed_max::numeric, '$length_unit'), ' ', '$speed_unit') AS __text FROM drives d "
          "WHERE @DR@ AND @OVERTHR@ AND EXISTS (SELECT 1 FROM speeding_incidents s WHERE s.drive_id = d.id) "
          "ORDER BY d.speed_max DESC LIMIT 200"), hide=0, refresh=2)
    q = {}
    q["path"] = path_query("fast_drive", True)
    q["drives"] = T("SELECT count(*) AS drives FROM drives d WHERE @DR@ AND @OVERTHR@")
    q["confirmed"] = T("@CTE@SELECT count(*) AS confirmed FROM drives d JOIN worst w ON w.drive_id = d.id "
                       "WHERE @DR@ AND @OVERTHR@ AND convert_km(w.max_speed::numeric, '$length_unit') > $speed_threshold")
    q["max"] = T("SELECT convert_km(max(d.speed_max)::numeric, '$length_unit') AS \"max_$length_unit\" FROM drives d WHERE @DR@")
    q["maxover"] = T("@CTE@SELECT convert_km(max(w.max_speed)::numeric, '$length_unit') AS \"max_over_limit_$length_unit\" "
                     "FROM drives d JOIN worst w ON w.drive_id = d.id WHERE @DR@")
    q["pct"] = T("SELECT round(100.0 * count(*) FILTER (WHERE @OVERTHR@) / NULLIF(count(*), 0), 1) AS pct "
                 "FROM drives d WHERE @DR@")
    q["unchecked"] = T("SELECT count(*) AS unchecked FROM drives d LEFT JOIN speeding_drive_status s ON s.drive_id = d.id "
                       "WHERE @DR@ AND @OVERTHR@ AND s.drive_id IS NULL")
    q["perday"] = T("@CTE@SELECT date_trunc('day', d.start_date) AS time, count(*) AS \"High-speed drives\", "
                    "count(*) FILTER (WHERE convert_km(w.max_speed::numeric, '$length_unit') > $speed_threshold) "
                    "AS \"of which over the limit\" FROM drives d LEFT JOIN worst w ON w.drive_id = d.id "
                    "WHERE @DR@ AND @OVERTHR@ GROUP BY 1 ORDER BY 1")
    q["dist"] = T("SELECT round(convert_km(d.speed_max::numeric, '$length_unit') / 10, 0) * 10 AS \"Top speed of the drive\", "
                  "count(*) AS \"Drives\" FROM drives d WHERE @DR@ AND d.speed_max IS NOT NULL GROUP BY 1 HAVING "
                  "round(convert_km(d.speed_max::numeric, '$length_unit') / 10, 0) * 10 > 0 ORDER BY 1")
    q["map"] = T("@CTE@SELECT d.id AS drive_id, d.start_date AS time, w.start_lat::float AS latitude, "
                 "w.start_lon::float AS longitude, convert_km(w.max_over::numeric, '$length_unit') AS over, "
                 "w.max_speed AS speed, w.speed_limit AS \"limit\", COALESCE(w.road_name, w.road_numbers) AS road "
                 "FROM drives d JOIN worst w ON w.drive_id = d.id WHERE @DR@ AND @OVERTHR@ ORDER BY w.max_over LIMIT 2000")
    q["table"] = T("@CTE@SELECT d.id AS drive_id, d.start_date AS time, "
                   "convert_km(d.speed_max::numeric, '$length_unit') AS \"top_speed_$length_unit\", "
                   "convert_km(d.distance::numeric, '$length_unit') AS \"distance_$length_unit\", "
                   "CASE WHEN s.drive_id IS NULL THEN 'not checked yet' WHEN s.status <> 'done' THEN s.status "
                   "WHEN w.drive_id IS NULL THEN 'within the limits' ELSE 'over the limit' END AS status, "
                   "w.speed_limit AS \"limit_at_worst\", w.max_speed AS \"speed_at_worst\", "
                   "convert_km(w.max_over::numeric, '$length_unit') AS \"over_$length_unit\", "
                   "coalesce(c.n, 0) AS incidents, COALESCE(w.road_name, w.road_numbers) AS road, "
                   "COALESCE(sg.name, CONCAT_WS(', ', COALESCE(sa.name, NULLIF(CONCAT_WS(' ', sa.road, sa.house_number), '')), sa.city)) AS start_address, "
                   "COALESCE(eg.name, CONCAT_WS(', ', COALESCE(ea.name, NULLIF(CONCAT_WS(' ', ea.road, ea.house_number), '')), ea.city)) AS end_address "
                   "FROM drives d LEFT JOIN speeding_drive_status s ON s.drive_id = d.id "
                   "LEFT JOIN worst w ON w.drive_id = d.id LEFT JOIN cnt c ON c.drive_id = d.id "
                   "LEFT JOIN addresses sa ON sa.id = d.start_address_id LEFT JOIN addresses ea ON ea.id = d.end_address_id "
                   "LEFT JOIN geofences sg ON sg.id = d.start_geofence_id LEFT JOIN geofences eg ON eg.id = d.end_geofence_id "
                   "WHERE @DR@ AND @OVERTHR@ ORDER BY d.speed_max DESC LIMIT 200")
    q["dest"] = T("SELECT COALESCE(eg.name, COALESCE(ea.name, NULLIF(CONCAT_WS(' ', ea.road, ea.house_number), '')), "
                  "ea.city, 'Unknown') AS location, count(*) AS high_speed_drives FROM drives d "
                  "LEFT JOIN addresses ea ON ea.id = d.end_address_id LEFT JOIN geofences eg ON eg.id = d.end_geofence_id "
                  "WHERE @DR@ AND @OVERTHR@ GROUP BY 1 ORDER BY 2 DESC LIMIT 20")
    q["hour"] = T("SELECT extract(hour FROM d.start_date)::int AS \"Hour of day (UTC)\", count(*) AS \"High-speed drives\" "
                  "FROM drives d WHERE @DR@ AND @OVERTHR@ GROUP BY 1 ORDER BY 1")
    q["weather"] = T("@CTE@SELECT coalesce(w.weather_main, 'unknown') AS \"Weather\", count(*) AS \"High-speed drives\" "
                     "FROM drives d LEFT JOIN worst w ON w.drive_id = d.id WHERE @DR@ AND @OVERTHR@ GROUP BY 1 ORDER BY 2 DESC")
    q["traffic"] = T("@CTE@SELECT CASE WHEN w.congestion_ratio IS NULL THEN 'unknown' WHEN w.congestion_ratio < 0.2 "
                     "THEN '1 free-flowing' WHEN w.congestion_ratio < 0.5 THEN '2 some traffic' ELSE '3 slow / congested' "
                     "END AS \"Traffic\", count(*) AS \"High-speed drives\" FROM drives d LEFT JOIN worst w ON w.drive_id = d.id "
                     "WHERE @DR@ AND @OVERTHR@ GROUP BY 1 ORDER BY 1")
    panels = [
        stat("Drives over threshold", q["drives"], 0, 0, desc="Drives whose top speed was above the threshold."),
        stat("...and over the limit", q["confirmed"], 4, 0, color="dark-red",
             desc="Of those, drives where you were also above the road's speed limit at that speed. The rest were "
                  "legally fast (for example 130 on a 130 road) or are not checked yet."),
        stat("Top speed", q["max"], 8, 0, unit="velocitykmh", color="orange"),
        stat("Top speed over the limit", q["maxover"], 12, 0, unit="velocitykmh", color="dark-red",
             desc="Highest speed reached during an incident (above the limit)."),
        stat("% of drives over threshold", q["pct"], 16, 0, unit="percent", color="yellow", decimals=1),
        stat("Not checked yet", q["unchecked"], 20, 0, color="blue",
             desc="High-speed drives not yet matched to the road network. The nightly job and the month-end backfill "
                  "work through them."),
        bars_over_time("High-speed drives by day", q["perday"], 0, 4, 12, 8,
                       desc="All drives above the threshold, and how many of them were also over the limit. "
                            "Drag across bars to zoom."),
        geomap_markers("Where the worst moment of each drive was", q["map"], 12, 4, 12, 8,
                       desc="One marker per high-speed drive at its worst over-the-limit moment (snapped to the road)."),
        geomap_route("Worst incident of the chosen drive", q["path"], 0, 12, 12, 10,
                     desc="The exact snapped stretch where the chosen drive was furthest over the limit. Choose the "
                          "drive in 'Drive on path map' above, or click a time in the table below."),
        barchart("Top speed of drives", q["dist"], 12, 12, 12, 10, color="orange"),
        table("Top high-speed drives", q["table"], 0, 22, 24, 12,
              desc="Fastest drives first. Status tells you whether the speed was actually above the limit.",
              hide=("drive_id",),
              links={"time": [{"title": "Show the worst incident on the map", "url":
                              "/d/" + uid + "?${__url_time_range}&var-car_id=${car_id}&var-speed_threshold=${speed_threshold}"
                              "&var-fast_drive=${__data.fields.drive_id}"},
                             {"title": "Open drive in TeslaMate", "url": DRIVE_DETAILS}]}, widths={"time": 160}),
        bargauge("Top destinations of high-speed drives", q["dest"], 0, 34, 12, 8, color="orange"),
        barchart("By hour of day", q["hour"], 12, 34, 12, 8, desc="Start hour (UTC) of high-speed drives.", color="purple"),
        barchart("Traffic on high-speed drives", q["traffic"], 0, 42, 12, 7, color="orange",
                 desc="Congestion estimate of the drive's worst moment (counts)."),
        barchart("Weather on high-speed drives", q["weather"], 12, 42, 12, 7, color="blue"),
    ]
    return "safety/speed-threshold-violations.json", q, dashboard(
        uid, "High Speed Tracking",
        "Drives whose top speed passed your threshold, checked against the real speed limit: a drive only counts as "
        "speeding when an incident above the road's limit backs it up.",
        panels, [CAR, LENGTH_UNIT, SPEED_UNIT, BASE_URL, thr, fast_var], ["tesla", "safety", "speeding"])


# ================================================================ 3. Location-Based Speed Limits
def build_places():
    uid = "location_speed_limits"
    spot_var = query_var(
        "spot", "Spot on path map",
        T("SELECT (array_agg(i.id ORDER BY i.max_over DESC))[1] AS __value, "
          "concat(coalesce(max(@PLACE@), concat(round(i.start_lat, 3), ', ', round(i.start_lon, 3))), '  (', count(*), ' incidents)') AS __text "
          "FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ GROUP BY round(i.start_lat, 3), round(i.start_lon, 3) "
          "ORDER BY count(*) DESC, max(i.max_over) DESC LIMIT 100"), hide=0, refresh=2)
    q = {}
    q["path"] = path_query("spot")
    q["places"] = T("SELECT count(*) AS places FROM (SELECT 1 FROM speeding_incidents i WHERE @FILT@ "
                    "GROUP BY round(i.start_lat, 3), round(i.start_lon, 3) HAVING count(*) >= 3) t")
    q["top"] = T("SELECT max(n) AS visits FROM (SELECT count(*) AS n FROM speeding_incidents i WHERE @FILT@ "
                 "GROUP BY round(i.start_lat, 3), round(i.start_lon, 3)) t")
    q["countries"] = T("SELECT count(DISTINCT p.country) AS countries FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@")
    q["named"] = T("SELECT round(100.0 * count(p.label) / NULLIF(count(*), 0), 1) AS named FROM speeding_incidents i "
                   "@PLACE_JOIN@ WHERE @FILT@")
    q["roadworks"] = T("SELECT count(*) AS roadworks FROM speeding_incidents i WHERE @FILT@ AND i.roadworks_nearby")
    q["map"] = T("SELECT round(i.start_lat, 3)::float AS latitude, round(i.start_lon, 3)::float AS longitude, "
                 "count(*) AS incidents, max(convert_km(i.max_over::numeric, '$length_unit')) AS over, "
                 "coalesce(max(@PLACE@), '') AS place FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ "
                 "GROUP BY round(i.start_lat, 3), round(i.start_lon, 3) ORDER BY incidents ASC LIMIT 3000")
    q["hot"] = T("SELECT coalesce(max(@PLACE@), concat(round(i.start_lat, 3), ', ', round(i.start_lon, 3))) AS place, "
                 "max(p.municipality) AS town, max(p.country) AS country, count(*) AS incidents, "
                 "count(DISTINCT i.drive_id) AS drives, round(avg(i.max_over), 1) AS \"avg_over\", max(i.max_over) AS \"max_over\", "
                 "string_agg(DISTINCT i.speed_limit::text, ', ') AS \"limits\", max(i.start_ts) AS \"last_seen\", "
                 "count(*) FILTER (WHERE i.roadworks_nearby) AS \"with_roadworks\", (array_agg(i.id ORDER BY i.max_over DESC))[1] AS incident_id, "
                 "round(i.start_lat, 3)::float AS latitude, round(i.start_lon, 3)::float AS longitude "
                 "FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ GROUP BY round(i.start_lat, 3), round(i.start_lon, 3) "
                 "ORDER BY incidents DESC, max(i.max_over) DESC LIMIT 200")
    q["zones"] = T("SELECT coalesce(p.country, 'unknown') AS country, convert_km(i.speed_limit::numeric, '$length_unit') AS \"limit_$length_unit\", "
                   "count(*) AS incidents, round(avg(i.max_over), 1) AS \"avg_over\", max(i.max_over) AS \"max_over\" "
                   "FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ GROUP BY 1, i.speed_limit ORDER BY 1, i.speed_limit")
    q["bycountry"] = T("SELECT coalesce(p.country, 'unknown') AS country, count(*) AS incidents "
                       "FROM speeding_incidents i @PLACE_JOIN@ WHERE @FILT@ GROUP BY 1 ORDER BY 2 DESC")
    q["overrides"] = T("SELECT o.name AS place, o.limit_kmh AS \"your_limit\", count(i.id) AS \"incidents_here\", "
                       "count(i.id) FILTER (WHERE i.max_speed > o.limit_kmh + greatest(5, round(0.05 * o.limit_kmh))) "
                       "AS \"over_your_limit\", max(i.max_speed) AS \"top_speed\", "
                       "string_agg(DISTINCT i.speed_limit::text, ', ') AS \"tomtom_limit\", o.note "
                       "FROM speed_limit_overrides o LEFT JOIN speeding_incidents i ON i.car_id = $car_id "
                       "AND $__timeFilter(i.start_ts) AND @DIST_TO_OVERRIDE@ <= o.radius_m GROUP BY o.id ORDER BY 4 DESC")
    panels = [
        stat("Repeat spots", q["places"], 0, 0, w=5, desc="Places (about 100 m) with at least 3 incidents."),
        stat("Most visits to one spot", q["top"], 5, 0, w=5, color="dark-red"),
        stat("Countries", q["countries"], 10, 0, w=5, color="blue",
             desc="Countries with incidents (from Reverse Geocoding; fills in over the first days)."),
        stat("Incidents with a place name", q["named"], 15, 0, w=5, unit="percent", color="yellow", decimals=1,
             desc="Place names are added by a nightly job, hotspots first."),
        stat("During roadworks", q["roadworks"], 20, 0, w=4, color="orange",
             desc="Incidents where TomTom reported roadworks or a lane closure nearby at that time. The limit there "
                  "may be temporary. Only known for times we have traffic incident data."),
        geomap_markers("Hotspots", q["map"], 0, 4, 12, 13, size_field="incidents", min_size=5, max_size=26,
                       desc="One marker per ~100 m spot; size = number of incidents, colour = worst overage."),
        table("Hotspot list", q["hot"], 12, 4, 12, 13,
              desc="Most repeated spots first. 'limits' are the limits TomTom reports there.",
              hide=("latitude", "longitude", "incident_id"), widths={"place": 220},
              links={"place": [{"title": "Show the worst incident here on the map", "url":
                                "/d/" + uid + "?${__url_time_range}&var-car_id=${car_id}&var-min_over=${min_over}"
                                "&var-spot=${__data.fields.incident_id}"}]}),
        geomap_route("Incident path at the chosen spot", q["path"], 0, 17, 12, 11,
                     desc="The exact snapped stretch of the worst incident at the spot chosen in 'Spot on path map' "
                          "above, or click a place in the hotspot list."),
        table("Limits by country", q["zones"], 12, 17, 12, 11,
              desc="Where each speed limit gets exceeded, by country."),
        barchart("Incidents by country", q["bycountry"], 0, 28, 10, 8, color="orange"),
        table("Your custom limits", q["overrides"], 10, 28, 14, 8,
              desc="Add your own limits for places where you know better than the map, for example:\n"
                   "INSERT INTO speed_limit_overrides (name, latitude, longitude, radius_m, limit_kmh, note) VALUES "
                   "('School street', 48.5933, 7.7531, 150, 30, 'zone 30');\n"
                   "Only incidents (over TomTom's limit) are counted, so a custom limit lower than TomTom's "
                   "undercounts."),
    ]
    return "location/location-speed-limits.json", q, dashboard(
        uid, "Location-Based Speed Limits",
        "Where speeding happens: repeat hotspots, limits by country and your own custom limits. Real limits come from "
        "TomTom Snap to Roads; places are named with TomTom Reverse Geocoding.",
        panels, [CAR, LENGTH_UNIT, SPEED_UNIT, BASE_URL, min_over_var(), spot_var], ["tesla", "location", "speeding"])


BUILDERS = (build_violations, build_high_speed, build_places)

# ---------------------------------------------------------------- query dump for testing
MACROS = [
    ("$__timeFilter(i.start_ts)", "i.start_ts BETWEEN now() - interval '90 days' AND now()"),
    ("$__timeFilter(d.start_date)", "d.start_date BETWEEN now() - interval '90 days' AND now()"),
    ("$__timeFilter(start_date)", "start_date BETWEEN now() - interval '90 days' AND now()"),
    ("$__timeFilter(start_ts)", "start_ts BETWEEN now() - interval '90 days' AND now()"),
    ("$__timeFrom()", "(now() - interval '90 days')"),
    ("$__timeTo()", "now()"),
    ("$car_id", "1"), ("$length_unit", "km"), ("$speed_unit", "km/h"), ("$min_over", "0"),
    ("$speed_threshold", "100"), ("$incident", "0"), ("$fast_drive", "4010"), ("$spot", "18"),
]


def fill(sql):
    for k, v in MACROS:
        sql = sql.replace(k, v)
    return sql


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    ap.add_argument("--emit-sql")
    a = ap.parse_args()
    emitted = []
    for build in BUILDERS:
        path, q, dash = build()
        for name, sql in q.items():
            emitted.append((dash["uid"] + "." + name, fill(sql)))
        if a.out:
            full = os.path.join(a.out, path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w") as f:
                json.dump(dash, f, indent=2, ensure_ascii=False)
                f.write("\n")
            print("wrote", full, "-", len(dash["panels"]), "panels")
    if a.emit_sql:
        with open(a.emit_sql, "w") as f:
            for name, sql in emitted:
                f.write("\\echo === " + name + "\n" + sql.replace("\n", " ") + ";\n")
        print("wrote", a.emit_sql, "-", len(emitted), "queries")


if __name__ == "__main__":
    main()
