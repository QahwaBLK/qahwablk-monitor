"""Waitlist webhook handshake URL, built from the waitlist's own .env.

The probe sends the same VERIFY_TOKEN Meta sends, so it must match the live
value. It is read on every call (the health check runs every 5 minutes), so a
token rotation in /srv/qahwablk/qahwablk-waitlist/.env is picked up with no
restart and no change here. The token is never written to logs or alerts:
use DISPLAY_URL for those.
"""

from __future__ import annotations

import urllib.parse

from dotenv import dotenv_values

WAITLIST_ENV_FILE = "/srv/qahwablk/qahwablk-waitlist/.env"
BASE_URL = "http://localhost:8099/webhook"
DISPLAY_URL = BASE_URL + "?hub.mode=subscribe&hub.verify_token=<VERIFY_TOKEN from waitlist .env>&hub.challenge=health"
MISSING = "VERIFY_TOKEN missing from " + WAITLIST_ENV_FILE


def health_url(env_file: str = WAITLIST_ENV_FILE) -> str | None:
    """The handshake URL, or None when the token cannot be read (missing file or key)."""
    try:
        token = (dotenv_values(env_file).get("VERIFY_TOKEN") or "").strip()
    except OSError:
        return None
    if not token:
        return None
    query = urllib.parse.urlencode(
        {"hub.mode": "subscribe", "hub.verify_token": token, "hub.challenge": "health"}
    )
    return BASE_URL + "?" + query
