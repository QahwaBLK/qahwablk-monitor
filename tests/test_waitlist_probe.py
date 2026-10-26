#!/usr/bin/env python3
"""Tests for waitlist_probe: the handshake URL is built from the waitlist .env,
fails closed when the token is missing, and the display URL never carries it.

Run:  python3 tests/test_waitlist_probe.py
"""

from __future__ import annotations

import os
import sys
import tempfile
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import waitlist_probe as wp  # noqa: E402

FAILS: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'OK  ' if ok else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}")
    if not ok:
        FAILS.append(label)


def env_file(text: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".env")
    with os.fdopen(fd, "w") as fh:
        fh.write(text)
    return path


def main() -> int:
    print("waitlist_probe")

    check("missing file -> None", wp.health_url("/nonexistent/waitlist/.env") is None)
    check("no VERIFY_TOKEN key -> None", wp.health_url(env_file("APP_SECRET=x\n")) is None)
    check("blank VERIFY_TOKEN -> None", wp.health_url(env_file("VERIFY_TOKEN=   \n")) is None)

    token = "t0k&en=with spaces"
    url = wp.health_url(env_file(f'VERIFY_TOKEN="{token}"\nAPP_SECRET=x\n'))
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url or "").query)
    check("token read from the file", query.get("hub.verify_token") == [token], str(query.get("hub.verify_token")))
    check("handshake params", query.get("hub.mode") == ["subscribe"] and query.get("hub.challenge") == ["health"])
    check("points at the local waitlist", (url or "").startswith("http://localhost:8099/webhook?"))
    check("display URL carries no token", token not in wp.DISPLAY_URL and "verify_token=<" in wp.DISPLAY_URL)

    unreadable = env_file("VERIFY_TOKEN=x\n")
    os.chmod(unreadable, 0)
    if os.geteuid() != 0:
        check("unreadable file -> None", wp.health_url(unreadable) is None)

    print(f"{len(FAILS)} failed")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
