"""Service mode: while the car is at the workshop, mute every push (Telegram + PagerDuty).

A flag file (SERVICE_MODE_FILE, default service-mode/service_mode.json in the repository) that the alert services read
on every send, never cached. Switch it with:
    python3 -c "import service_mode; service_mode.set_service_mode(True)"      # False to switch off
"""
import json
import os
import time
from pathlib import Path

FLAG_FILE = Path(os.getenv('SERVICE_MODE_FILE',
                           str(Path(__file__).resolve().parent.parent / 'service-mode' / 'service_mode.json')))


def service_mode_since():
    """Epoch seconds service mode was switched on, or None when off. A broken flag file counts as off."""
    try:
        return float(json.loads(FLAG_FILE.read_text())['since'])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def is_service_mode():
    return service_mode_since() is not None


def set_service_mode(on):
    """Switch service mode on/off. Returns the previous 'since' (None if it was off)."""
    previous = service_mode_since()
    if on:
        if previous is None:
            FLAG_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = FLAG_FILE.with_suffix('.tmp')
            tmp.write_text(json.dumps({'since': time.time()}))
            os.replace(tmp, FLAG_FILE)
    else:
        FLAG_FILE.unlink(missing_ok=True)
    return previous
