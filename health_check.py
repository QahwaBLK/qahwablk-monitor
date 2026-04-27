#!/usr/bin/env python3
"""
BLK Server Health Monitor
Runs every 5 minutes via systemd timer as qahwablk user.
Sends Telegram alerts on first failure; suppresses repeats until recovery.
"""

import json
import logging
import socket
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENV_FILE       = "/srv/qahwablk/cashier-dashboard/.env"
LOG_FILE       = "/srv/qahwablk/monitor/health.log"
STATE_FILE     = "/srv/qahwablk/monitor/state.json"

SERVICES = [
    "cashier-api", "cashier-frontend",

    "pulse-api",   "pulse-frontend",
]

ENDPOINTS = [
    ("cashier", "https://cashier.blk.jo"),
    ("pulse",   "https://pulse.blk.jo"),
]

# (table, date_column, max_age_hours, human_label)
FRESHNESS_CHECKS = [
    ("cashier_daily_metrics", "date",            36, "cashier pipeline"),
    ("daily_sales",           "date",            36, "sales pipeline"),
    ("zenhr_attendance",      "attendance_date", 36, "ZenHR attendance sync"),
    ("operate_daily_tasks",   "date",             6, "operate pipeline"),
    ("shop_daily_health",     "date",            36, "beat health score"),
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State  (persist across runs to prevent alert spam)
# ---------------------------------------------------------------------------

def load_state():
    try:
        return json.loads(Path(STATE_FILE).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    Path(STATE_FILE).write_text(json.dumps(state, indent=2))

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(token, chat_id, message):
    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:
        log.error("Telegram send failed: %s", exc)
        return False

# ---------------------------------------------------------------------------
# Checks — each returns (ok: bool, detail: str)
# ---------------------------------------------------------------------------

def check_service(name):
    r = subprocess.run(["systemctl", "is-active", name], capture_output=True, text=True)
    active = r.stdout.strip() == "active"
    return active, r.stdout.strip()


def check_endpoint(url):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "BLK-HealthCheck/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            return True, "HTTP {}".format(resp.status)
    except urllib.error.HTTPError as exc:
        if exc.code < 500:
            return True, "HTTP {}".format(exc.code)
        return False, "HTTP {}".format(exc.code)
    except Exception as exc:
        return False, str(exc)


def check_database():
    # Peer auth: connects as qahwablk linux user → qahwablk PG role
    try:
        conn = psycopg2.connect(dbname="qahwablk", connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return True, "SELECT 1 ok"
    except Exception as exc:
        return False, str(exc)


def check_freshness(table, date_col, max_age_hours, label):
    try:
        conn = psycopg2.connect(dbname="qahwablk", connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT MAX({}) FROM {}".format(date_col, table))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row or row[0] is None:
            return False, "{}: no rows".format(table)

        latest = row[0]
        if isinstance(latest, datetime):
            latest_date = latest.date()
        else:
            latest_date = latest

        today = datetime.now(timezone.utc).date()
        age_days = (today - latest_date).days

        if age_days * 24 > max_age_hours:
            return False, "{}: latest {} ({} days old, threshold {}h)".format(
                table, latest_date, age_days, max_age_hours)
        return True, "{}: latest {} (ok)".format(table, latest_date)
    except Exception as exc:
        return False, "{}: {}".format(table, exc)


def check_odoo(odoo_url):
    try:
        host = odoo_url.replace("https://", "").replace("http://", "").split("/")[0]
        sock = socket.create_connection((host, 443), timeout=8)
        sock.close()
        return True, "TCP 443 ok ({})".format(host)
    except Exception as exc:
        return False, "Odoo unreachable: {}".format(exc)


def check_zenhr():
    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(
            "https://app.zenhr.com",
            headers={"User-Agent": "BLK-HealthCheck/1.0"},
        )
        with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
            return True, "ZenHR HTTP {}".format(resp.status)
    except urllib.error.HTTPError as exc:
        if exc.code < 500:
            return True, "ZenHR HTTP {}".format(exc.code)
        return False, "ZenHR HTTP {}".format(exc.code)
    except Exception as exc:
        return False, "ZenHR unreachable: {}".format(exc)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    env = dotenv_values(ENV_FILE)
    token   = env.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = env.get("TELEGRAM_CHAT_ID", "")
    odoo_url = env.get("ODOO_URL", "")

    if not token or not chat_id:
        log.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing — alerts disabled")

    state      = load_state()
    failures   = []   # new failures this run
    recoveries = []   # newly recovered this run
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def evaluate(key, ok, detail, label, min_failures=1):
        prev = state.get(key, {})
        was_failing = prev.get("failing", False)
        consecutive = prev.get("consecutive_failures", 0)
        if ok:
            log.info("OK   %s — %s", label, detail)
            if was_failing:
                recoveries.append("\u2705 RECOVERED: {} \u2014 {}".format(label, detail))
            state[key] = {"failing": False, "consecutive_failures": 0}
        else:
            consecutive += 1
            log.warning("FAIL %s — %s (consecutive: %d/%d)", label, detail, consecutive, min_failures)
            if consecutive >= min_failures and not was_failing:
                failures.append("\U0001F534 DOWN: {} \u2014 {}".format(label, detail))
                state[key] = {"failing": True, "since": now_str, "consecutive_failures": consecutive}
            elif was_failing:
                state[key] = {"failing": True, "since": prev.get("since", now_str), "consecutive_failures": consecutive}
                log.info("     (still failing since %s)", prev.get("since", "?"))
            else:
                state[key] = {"failing": False, "consecutive_failures": consecutive}
                log.info("     (failure %d/%d, not alerting yet)", consecutive, min_failures)

    # 1. Services
    for svc in SERVICES:
        ok, detail = check_service(svc)
        evaluate("service:" + svc, ok, detail, "service/" + svc)

    # 2. Endpoints
    for label, url in ENDPOINTS:
        ok, detail = check_endpoint(url)
        evaluate("endpoint:" + label, ok, detail, "endpoint/" + url)

    # 3. Database
    ok, detail = check_database()
    evaluate("db:qahwablk", ok, detail, "database/qahwablk")

    # 4. Data freshness (skip if DB is down)
    if state.get("db:qahwablk", {}).get("failing"):
        log.info("SKIP freshness checks — DB is down")
    else:
        for table, date_col, max_age_h, label in FRESHNESS_CHECKS:
            ok, detail = check_freshness(table, date_col, max_age_h, label)
            evaluate("freshness:" + table, ok, detail, "freshness/" + label)

    # 5. External services
    if odoo_url:
        ok, detail = check_odoo(odoo_url)
        evaluate("ext:odoo", ok, detail, "external/Odoo")

    ok, detail = check_zenhr()
    evaluate("ext:zenhr", ok, detail, "external/ZenHR", min_failures=2)

    # 6. Save state and send alerts
    save_state(state)

    if token and chat_id:
        for msg in failures:
            text = "<b>BLK SERVER ALERT</b>\n{}\n\n\U0001F550 {}".format(msg, now_str)
            if send_telegram(token, chat_id, text):
                log.info("Alert sent: %s", msg)

        for msg in recoveries:
            text = "<b>BLK SERVER ALERT</b>\n{}\n\n\U0001F550 {}".format(msg, now_str)
            if send_telegram(token, chat_id, text):
                log.info("Recovery sent: %s", msg)

    fail_count = sum(1 for v in state.values() if v.get("failing"))
    log.info("Done — %d new alerts, %d recoveries, %d currently failing",
             len(failures), len(recoveries), fail_count)


if __name__ == "__main__":
    main()
