#!/usr/bin/env python3
"""PagerDuty metrics for Grafana.

Copies PagerDuty services, incidents and notification log entries into the `pagerduty` schema of
the TeslaMate database, so the "PagerDuty" Grafana dashboard can chart them with the existing
TeslaMate data source (no Grafana plugin, no Grafana restart).

Run it every 5 minutes, for example from cron:
  */5 * * * * cd /opt/teslamate-speeding && python3 pagerduty/pagerduty_metrics.py >> pagerduty.log 2>&1

Uses PAGERDUTY_API_KEY_RO from the .env in the repository root: a READ-ONLY REST key. It only ever sends GET
requests. Each run re-reads the last LOOKBACK_DAYS days (incidents change state: acknowledged,
resolved), and the first run back-fills BACKFILL_DAYS.
"""
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')   # the .env in the repository root
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger('pagerduty_metrics')

API = 'https://api.pagerduty.com'
LOOKBACK_DAYS = int(os.getenv('PD_LOOKBACK_DAYS', '14'))
BACKFILL_DAYS = int(os.getenv('PD_BACKFILL_DAYS', '180'))   # PagerDuty allows at most ~6 months per query

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS pagerduty;
CREATE TABLE IF NOT EXISTS pagerduty.services (
  id text PRIMARY KEY, name text NOT NULL, status text, description text, html_url text,
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS pagerduty.incidents (
  id text PRIMARY KEY, number int, title text, status text, urgency text,
  service_id text, service_name text, created_at timestamptz, acknowledged_at timestamptz,
  resolved_at timestamptz, resolved_by text, html_url text, updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS incidents_created_idx ON pagerduty.incidents (created_at);
CREATE TABLE IF NOT EXISTS pagerduty.notifications (
  id text PRIMARY KEY, incident_id text, created_at timestamptz, channel text, user_name text, summary text);
CREATE INDEX IF NOT EXISTS notifications_created_idx ON pagerduty.notifications (created_at);
CREATE TABLE IF NOT EXISTS pagerduty.collector_runs (
  ran_at timestamptz PRIMARY KEY DEFAULT now(), ok boolean, incidents int, notifications int, message text);
"""


def ts(v):
    return datetime.fromisoformat(v.replace('Z', '+00:00')) if v else None


def channel_of(entry):
    """How a notification was delivered: notification.type is e.g. 'phone_notification' or 'sms_notification'
    (channel.type is only 'auto', meaning sent by the escalation rules)."""
    t = ((entry.get('notification') or {}).get('type') or '').replace('_notification', '')
    if t in ('phone', 'sms', 'email', 'push'):
        return 'push_notification' if t == 'push' else t
    s = (entry.get('summary') or '').lower()
    for word, ch in (('phone', 'phone'), ('sms', 'sms'), ('email', 'email'), ('push', 'push_notification')):
        if word in s:
            return ch
    return t or 'unknown'


class PD:
    def __init__(self, key):
        self.s = requests.Session()
        self.s.headers.update({'Authorization': f'Token token={key}', 'Accept': 'application/vnd.pagerduty+json;version=2'})

    def get_all(self, path, key, params):
        """GET with classic offset pagination; read-only."""
        params = dict(params, limit=100, offset=0)
        while True:
            for attempt in range(4):
                r = self.s.get(API + path, params=params, timeout=30)
                if r.status_code == 429:
                    time.sleep(int(r.headers.get('Retry-After', '5')) + 1)
                    continue
                break
            if r.status_code != 200:
                raise RuntimeError(f'GET {path}: HTTP {r.status_code} {r.text[:200]}')
            d = r.json()
            yield from d.get(key, [])
            if not d.get('more'):
                return
            params['offset'] += params['limit']


def main():
    key = os.getenv('PAGERDUTY_API_KEY_RO', '')
    if not key:
        sys.exit('PAGERDUTY_API_KEY_RO is not set (put it in .env)')
    db = psycopg2.connect(host=os.getenv('DB_HOST', 'localhost'), port=int(os.getenv('DB_PORT', '5432')),
                          dbname=os.getenv('DB_NAME', 'teslamate'), user=os.getenv('DB_USER', 'teslamate'),
                          password=os.getenv('DB_PASSWORD', ''), connect_timeout=10)
    pd = PD(key)
    with db, db.cursor() as cur:
        cur.execute(SCHEMA)
        cur.execute('SELECT count(*) FROM pagerduty.incidents')
        first = cur.fetchone()[0] == 0
    since = datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS if first else LOOKBACK_DAYS)
    until = datetime.now(timezone.utc) + timedelta(minutes=5)
    window = {'since': since.isoformat(), 'until': until.isoformat()}
    n_inc = n_not = 0
    try:
        services = list(pd.get_all('/services', 'services', {}))
        incidents = list(pd.get_all('/incidents', 'incidents', {**window, 'statuses[]': ['triggered', 'acknowledged', 'resolved'],
                                                                  'time_zone': 'UTC'}))
        entries = list(pd.get_all('/log_entries', 'log_entries', {**window, 'is_overview': 'false', 'time_zone': 'UTC'}))
        first_ack, resolved_by = {}, {}
        for e in entries:
            inc = (e.get('incident') or {}).get('id')
            if e['type'] == 'acknowledge_log_entry' and inc:
                first_ack[inc] = min(first_ack.get(inc, e['created_at']), e['created_at'])
            if e['type'] == 'resolve_log_entry' and inc:
                resolved_by[inc] = (e.get('channel') or {}).get('type') or (e.get('agent') or {}).get('summary')
        with db, db.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO pagerduty.services (id, name, status, description, html_url) VALUES %s
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, status = EXCLUDED.status,
                  description = EXCLUDED.description, html_url = EXCLUDED.html_url, updated_at = now()""",
                [(s['id'], s['name'], s.get('status'), s.get('description'), s.get('html_url')) for s in services])
            rows = []
            for i in incidents:
                resolved_at = i.get('resolved_at') or (i.get('last_status_change_at') if i['status'] == 'resolved' else None)
                rows.append((i['id'], i.get('incident_number'), i.get('title'), i['status'], i.get('urgency'),
                             i['service']['id'], i['service'].get('summary'), ts(i['created_at']),
                             ts(first_ack.get(i['id'])), ts(resolved_at), resolved_by.get(i['id']), i.get('html_url')))
            if rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO pagerduty.incidents (id, number, title, status, urgency, service_id, service_name, created_at,
                      acknowledged_at, resolved_at, resolved_by, html_url) VALUES %s
                    ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, status = EXCLUDED.status, urgency = EXCLUDED.urgency,
                      service_name = EXCLUDED.service_name,
                      acknowledged_at = COALESCE(EXCLUDED.acknowledged_at, pagerduty.incidents.acknowledged_at),
                      resolved_at = COALESCE(EXCLUDED.resolved_at, pagerduty.incidents.resolved_at),
                      resolved_by = COALESCE(EXCLUDED.resolved_by, pagerduty.incidents.resolved_by), updated_at = now()""", rows)
            notes = [(e['id'], (e.get('incident') or {}).get('id'), ts(e['created_at']), channel_of(e),
                      (e.get('user') or {}).get('summary'), e.get('summary'))
                     for e in entries if e['type'] == 'notify_log_entry']
            if notes:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO pagerduty.notifications (id, incident_id, created_at, channel, user_name, summary) VALUES %s
                    ON CONFLICT (id) DO UPDATE SET channel = EXCLUDED.channel""", notes)
            n_inc, n_not = len(rows), len(notes)
            cur.execute('INSERT INTO pagerduty.collector_runs (ok, incidents, notifications, message) VALUES (true, %s, %s, %s)',
                        (n_inc, n_not, f'{len(services)} services, window {since:%Y-%m-%d} to now'))
            cur.execute("DELETE FROM pagerduty.collector_runs WHERE ran_at < now() - interval '30 days'")
        log.info(f'ok: {len(services)} services, {n_inc} incidents, {n_not} notifications (since {since:%Y-%m-%d})')
    except Exception as e:
        with db, db.cursor() as cur:
            cur.execute('INSERT INTO pagerduty.collector_runs (ok, message) VALUES (false, %s)', (str(e)[:500],))
        log.error(f'failed: {e}')
        sys.exit(1)
    finally:
        db.close()


if __name__ == '__main__':
    main()
