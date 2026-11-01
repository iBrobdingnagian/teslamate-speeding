#!/usr/bin/env python3
"""
Shared TomTom API client for this project's scripts: handles multi-key
rotation on quota/rate-limit rejection (403/429) and logs every call to
api_usage_log, so usage can be tracked locally and reported via Grafana / a
Telegram digest.

TomTom's freemium tier has no self-service usage API - the Developer
Portal's Analytics tab is the only authoritative source. This log is a local
ESTIMATE: it only sees calls made through this project's own scripts (not
any other use of the same keys), and buckets by calendar month rather than
TomTom's actual billing cycle - treat it as a sanity check, not ground truth.
"""

import os
import requests
import psycopg2
from contextlib import closing
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'teslamate')
DB_USER = os.getenv('DB_USER', 'teslamate')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

TOMTOM_API_KEYS = [k for k in (
    os.getenv('TOMTOM_API_KEY'),
    os.getenv('TOMTOM_API_KEY_2'),
    os.getenv('TOMTOM_API_KEY_3'),
) if k]

TLG_API = os.getenv('TLG_API')
TLG_CHAT = os.getenv('TLG_CHAT')

# Monthly quota per TomTom product, as shown on the Developer Portal
# Analytics tab. Update here if the plan changes - these are not fetched
# from anywhere, TomTom doesn't expose them via API.
TOMTOM_QUOTAS = {
    'Reverse Geocoding API': 20000,
    'Reachable Range API': 2500,
    'Snap to Roads API': 2500,
    'Traffic Flow Segment Data': 20000,
    'Traffic Incident Details': 2500,
    'Routing API': 20000,
}

_current_key_index = 0
_log_conn = None


def _get_log_connection():
    """Lazily-opened, reused connection dedicated to usage logging, kept
    separate from each script's own data connection so a logging hiccup
    never affects the caller's main DB transaction."""
    global _log_conn
    if _log_conn is None or _log_conn.closed:
        _log_conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
        with closing(_log_conn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS api_usage_log (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    product TEXT NOT NULL,
                    called_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    success BOOLEAN,
                    status_code INTEGER,
                    key_index INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_api_usage_provider_product_time
                    ON api_usage_log(provider, product, called_at);
            """)
            _log_conn.commit()
    return _log_conn


def log_api_call(provider, product, success, status_code=None, key_index=None):
    try:
        conn = _get_log_connection()
        with closing(conn.cursor()) as cur:
            cur.execute("""
                INSERT INTO api_usage_log (provider, product, success, status_code, key_index)
                VALUES (%s, %s, %s, %s, %s)
            """, (provider, product, success, status_code, key_index))
            conn.commit()
    except psycopg2.Error as e:
        print(f"  ⚠ Failed to log API usage (non-fatal): {e}")


MONTH_START_SQL = "date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'"
SWITCH_AT = 0.9     # move to the next key once a key has used this share of a product's monthly quota


def month_usage(product):
    """{key_index: calls this month} for one product, from the usage log. {} if the log cannot be read."""
    try:
        conn = _get_log_connection()
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT key_index, count(*) FROM api_usage_log WHERE provider = 'tomtom' AND product = %s "
                        "AND called_at >= " + MONTH_START_SQL + " GROUP BY key_index", (product,))
            return {int(k): int(n) for k, n in cur.fetchall() if k is not None}
    except psycopg2.Error:
        return {}


def usage_fraction(product):
    """Share of a product's monthly quota used across all keys (0..1). Extra jobs check this before spending."""
    quota = TOMTOM_QUOTAS.get(product)
    if not quota:
        return 0.0
    return sum(month_usage(product).values()) / float(quota * max(len(TOMTOM_API_KEYS), 1))


def _pick_key(product):
    """First key still under SWITCH_AT of the product's quota, or None to keep the current key."""
    quota = TOMTOM_QUOTAS.get(product)
    if not quota or len(TOMTOM_API_KEYS) < 2:
        return None
    used = month_usage(product)
    for idx in range(len(TOMTOM_API_KEYS)):
        if used.get(idx, 0) < SWITCH_AT * quota:
            return idx
    return None


def tomtom_request(product, method, url, params=None, data=None, timeout=20, headers=None):
    """
    Perform a TomTom API call with automatic key rotation on 403/429,
    logging every attempt. Returns the successful requests.Response.
    Raises RuntimeError if every configured key is exhausted/rejected.
    """
    global _current_key_index

    if not TOMTOM_API_KEYS:
        raise RuntimeError("No TomTom API key configured")

    params = dict(params or {})
    tried = 0
    pick = _pick_key(product)
    if pick is not None:
        _current_key_index = pick
    while tried < len(TOMTOM_API_KEYS):
        idx = _current_key_index % len(TOMTOM_API_KEYS)
        params['key'] = TOMTOM_API_KEYS[idx]
        response = requests.request(method, url, params=params, data=data, timeout=timeout, headers=headers)

        if response.status_code in (403, 429):
            log_api_call('tomtom', product, success=False, status_code=response.status_code, key_index=idx)
            print(f"  ⚠ TomTom key #{idx + 1} rejected ({response.status_code}) - rotating to next key")
            _current_key_index += 1
            tried += 1
            continue

        log_api_call('tomtom', product, success=response.ok, status_code=response.status_code, key_index=idx)
        response.raise_for_status()
        return response

    raise RuntimeError(f"All TomTom keys exhausted or rejected for product '{product}'")


def send_telegram(text):
    """Send a message to the configured Telegram chat, logging the attempt."""
    if not TLG_API or not TLG_CHAT:
        print("  ⚠ TLG_API/TLG_CHAT not set, skipping Telegram send")
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TLG_API}/sendMessage",
            data={'chat_id': TLG_CHAT, 'text': text, 'parse_mode': 'Markdown'},
            timeout=10,
        )
        success = response.ok and response.json().get('ok', False)
        log_api_call('telegram', 'sendMessage', success=success, status_code=response.status_code)
        return success
    except (ValueError, requests.exceptions.RequestException) as exc:
        print(f"Telegram send failed: {type(exc).__name__}")
        return False


def send_telegram_document(path, caption):
    """Send a route file only to the already configured notification chat."""
    if not TLG_API or not TLG_CHAT:
        return False
    try:
        with open(path, 'rb') as stream:
            response = requests.post(f'https://api.telegram.org/bot{TLG_API}/sendDocument',
                                     data={'chat_id': TLG_CHAT, 'caption': caption},
                                     files={'document': (os.path.basename(path), stream, 'application/gpx+xml')},
                                     timeout=30)
        success = response.ok and response.json().get('ok', False)
        log_api_call('telegram', 'sendDocument', success=success, status_code=response.status_code)
        return success
    except (OSError, ValueError, requests.exceptions.RequestException) as exc:
        print(f'Telegram route attachment failed: {type(exc).__name__}')
        return False
