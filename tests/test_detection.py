import datetime as dt

import speeding_incidents as si

T0 = dt.datetime(2026, 1, 1, 12, 0, 0)


def rec(t, speed, limit, advisory=None):
    return dict(ts=T0 + dt.timedelta(seconds=t), speed=speed, limit=limit, advisory=advisory,
                road="Main St", numbers=None, coord=[4.0 + t * 1e-4, 52.0])


def run(speeds, limit=100, start=0, step=1):
    return [rec(start + i * step, s, limit) for i, s in enumerate(speeds)]


# ---- detect -------------------------------------------------------------------------------------------

def test_within_tolerance_is_not_an_incident():
    # limit 100: tolerance is max(5 km/h, 5 % = 5) = 5, so 105 is allowed
    assert si.detect(run([105] * 10)) == []


def test_over_tolerance_is_one_incident():
    found = si.detect(run([106] * 10))
    assert len(found) == 1 and len(found[0]) == 10


def test_percentage_tolerance_applies_on_fast_roads():
    # limit 130: 5 % is 6, so 136 is allowed and 137 is not
    assert si.detect(run([136] * 10, limit=130)) == []
    assert len(si.detect(run([137] * 10, limit=130))) == 1


def test_short_blip_is_dropped_and_five_seconds_is_kept():
    assert si.detect(run([120] * 5)) == []          # 4 s between first and last sample
    assert len(si.detect(run([120] * 6))) == 1      # 5 s


def test_gap_up_to_merge_limit_joins_incidents():
    a = run([120] * 6)
    b = run([120] * 6, start=5 + 10)
    assert len(si.detect(a + b)) == 1


def test_longer_gap_splits_incidents():
    a = run([120] * 6)
    b = run([120] * 6, start=5 + 11)
    assert len(si.detect(a + b)) == 2


def test_slowing_down_in_between_splits_incidents():
    assert len(si.detect(run([120] * 6 + [90] + [120] * 6))) == 2


def test_limit_change_starts_a_new_incident():
    first = run([70] * 6, limit=50)
    second = run([100] * 6, limit=80, start=6)
    assert len(si.detect(first + second)) == 2


def test_huge_overage_is_treated_as_a_bad_match():
    assert si.detect(run([160] * 10)) == []         # 60 over a 100 limit


def test_no_limit_means_no_incident():
    assert si.detect(run([200] * 10, limit=None)) == []


def test_point_without_snapped_coordinate_is_ignored():
    recs = run([120] * 10)
    for r in recs:
        r["coord"] = None
    assert si.detect(recs) == []


# ---- detect_unlimited (advisory speed only, e.g. German autobahn) -------------------------------------

def test_advisory_stretch_is_not_a_violation_but_is_kept():
    recs = [rec(i, 150, None, advisory=130) for i in range(10)]
    assert si.detect(recs) == []
    assert len(si.detect_unlimited(recs)) == 1


# ---- annotate -----------------------------------------------------------------------------------------

def snap_json(limit_type="Statutory", value=50, matched=True):
    return {
        "projectedPoints": [{"type": "Feature", "geometry": {"coordinates": [4.0, 52.0]},
                             "properties": {"routeIndex": 0, "snapResult": "Matched" if matched else "Unmatched"}}],
        "route": [{"properties": {"speedLimits": {"value": value, "unit": "KPH", "type": limit_type},
                                  "address": {"roadName": "Main St", "roadNumbers": ["A1", "E19"]}}}],
    }


def point(t=0, speed=60):
    return dict(ts=T0 + dt.timedelta(seconds=t), lat=52.0, lon=4.0, speed=speed)


def test_annotate_reads_limit_and_road():
    out = si.annotate([([point()], snap_json())])
    assert out[0]["limit"] == 50 and out[0]["road"] == "Main St" and out[0]["numbers"] == "A1,E19"


def test_annotate_treats_recommended_speed_as_advisory():
    out = si.annotate([([point()], snap_json(limit_type="Recommended", value=130))])
    assert out[0]["limit"] is None and out[0]["advisory"] == 130


def test_annotate_unmatched_point_has_no_limit():
    out = si.annotate([([point()], snap_json(matched=False))])
    assert out[0]["limit"] is None and out[0]["coord"] is None


def test_annotate_skips_points_seen_in_two_chunks():
    p = point()
    out = si.annotate([([p], snap_json()), ([p], snap_json())])
    assert len(out) == 1


# ---- geometry and chunking ----------------------------------------------------------------------------

def test_haversine_one_degree_of_latitude():
    assert abs(si.hav(0, 0, 1, 0) - 111195) < 100


def pts(n, step_deg=0.0003):
    return [dict(ts=T0 + dt.timedelta(seconds=i), lat=52.0 + i * step_deg, lon=4.0, speed=50) for i in range(n)]


def test_chunks_split_at_chunk_size_and_share_one_point():
    parts = list(si.chunks(pts(si.CHUNK + 10)))
    assert len(parts) == 2
    assert len(parts[0]) == si.CHUNK
    assert parts[1][0] is parts[0][-1]


def test_chunks_split_at_large_gaps():
    a, b = pts(5), pts(5)
    for p in b:
        p["lat"] += 1.0            # about 111 km away
    assert len(list(si.chunks(a + b))) == 2


def test_chunks_drop_a_single_leftover_point():
    assert list(si.chunks(pts(1))) == []
