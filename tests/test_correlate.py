import speeding_incidents as si


class FakeCursor:
    def __init__(self):
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append(" ".join(sql.split()))

    def fetchall(self):
        return [(1,)]


def run(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("SPEEDING_CORRELATE", raising=False)
    else:
        monkeypatch.setenv("SPEEDING_CORRELATE", value)
    cur = FakeCursor()
    si.correlate(cur, [1, 2])
    return "\n".join(cur.sql)


def test_default_joins_no_optional_table(monkeypatch):
    sql = run(monkeypatch, None)
    assert "weather_data" not in sql and "traffic_" not in sql and "inferred_traffic" not in sql
    assert "correlated_at = now()" in sql


def test_zero_joins_nothing(monkeypatch):
    assert "weather_data" not in run(monkeypatch, "0")


def test_weather_mode_reads_only_weather_data(monkeypatch):
    sql = run(monkeypatch, "weather")
    assert "JOIN weather_data" in sql
    assert "traffic_flow_snapshots" not in sql and "traffic_incidents" not in sql and "inferred_traffic" not in sql


def test_traffic_mode_does_not_read_weather_data(monkeypatch):
    sql = run(monkeypatch, "traffic")
    assert "JOIN weather_data" not in sql
    assert "traffic_flow_snapshots" in sql and "traffic_incidents" in sql


def test_one_and_all_enable_both(monkeypatch):
    for value in ("1", "all", "weather,traffic"):
        sql = run(monkeypatch, value)
        assert "JOIN weather_data" in sql and "traffic_flow_snapshots" in sql
