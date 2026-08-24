#!/usr/bin/env python3
"""
BLK Server Health Monitor
Runs every 5 minutes via systemd timer as qahwablk user.
Sends Telegram alerts on first failure; suppresses repeats until recovery.
"""

import json
import logging
import shutil
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
]

ENDPOINTS = [
    ("cashier", "https://cashier.blk.jo"),
    ("shareeb", "https://shareeb.blk.jo"),
]

# (table, date_column, max_age_hours, human_label)
FRESHNESS_CHECKS = [
    ("cashier_daily_metrics", "date",            36, "cashier pipeline"),
    ("daily_sales",           "date",            36, "sales pipeline"),
    ("zenhr_attendance",      "attendance_date", 36, "ZenHR attendance sync"),
    ("operate_daily_tasks",   "date",             6, "operate pipeline"),
    ("shop_daily_health",     "date",            36, "beat health score"),
    ("shop_compliance_scores", "created_at",     36, "shop compliance calculator"),
]

# (mount point, alert threshold % used)
DISK_CHECKS = [
    ("/",                        90),
    ("/mnt/HC_Volume_105265098", 90),
]

# Internal health endpoints (healthy means exactly HTTP 200)
INTERNAL_ENDPOINTS = [
    ("shareeb-api-internal", "http://localhost:8097/healthz"),
]

# Waitlist webhook handshake (qahwablk-waitlist.service on :8099). Meta-style
# hub.challenge echo: healthy = HTTP 200 with body exactly "health".
# min_failures=3 in main() = alert only after ~15 min down at the 5-min cadence.
WAITLIST_VERIFY_TOKEN = "qahwablk_waitlist_2026"
WAITLIST_HEALTH_URL = (
    "http://localhost:8099/webhook"
    "?hub.mode=subscribe&hub.verify_token={}&hub.challenge=health".format(WAITLIST_VERIFY_TOKEN)
)

# Nginx config drift: live file vs known-good baseline. Baseline must be
# refreshed (cp live -> baseline) whenever a config change is deliberate.
# Pulse omitted: service sunset. Ported from service_health_check.py.
NGINX_BACKUP_DIR = Path("/srv/shared/server-docs/nginx-backups")
NGINX_CONFIGS = {
    "cashier": "/etc/nginx/sites-enabled/cashier.blk.jo",
}

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


def check_disk(path, threshold_pct):
    try:
        usage = shutil.disk_usage(path)
        pct = usage.used / usage.total * 100
        detail = "{:.0f}% used, {:.1f}G free (threshold {}%)".format(
            pct, usage.free / 2**30, threshold_pct)
        return pct < threshold_pct, detail
    except Exception as exc:
        return False, "{}: {}".format(path, exc)


def check_internal(url):
    """Internal health endpoint: healthy means exactly HTTP 200."""
    req = urllib.request.Request(url, headers={"User-Agent": "BLK-HealthCheck/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200, "HTTP {}".format(resp.status)
    except urllib.error.HTTPError as exc:
        return False, "HTTP {}".format(exc.code)
    except Exception as exc:
        return False, str(exc)


def check_waitlist():
    """Webhook handshake: healthy = HTTP 200 and body exactly 'health'."""
    req = urllib.request.Request(WAITLIST_HEALTH_URL, headers={"User-Agent": "BLK-HealthCheck/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read(64).decode("utf-8", errors="replace").strip()
            if resp.status == 200 and body == "health":
                return True, "handshake ok"
            return False, "HTTP {} body {!r}".format(resp.status, body[:32])
    except urllib.error.HTTPError as exc:
        return False, "HTTP {}".format(exc.code)
    except Exception as exc:
        return False, str(exc)


def check_nginx_drift(name, live_path):
    """Diff live nginx config vs known-good baseline; seed baseline if absent."""
    backup = NGINX_BACKUP_DIR / "{}.blk.jo.conf".format(name)
    try:
        if not backup.exists():
            NGINX_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(live_path, backup)
            return True, "baseline seeded from live config"
        r = subprocess.run(["diff", str(backup), live_path],
                           capture_output=True, text=True, timeout=5)
        drift = r.stdout.strip()
        if drift:
            return False, "{} differs from baseline ({} diff lines)".format(
                live_path, len(drift.splitlines()))
        return True, "matches baseline"
    except Exception as exc:
        return False, "{}: {}".format(name, exc)


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

    # 2b. Internal endpoints (exact 200)
    for label, url in INTERNAL_ENDPOINTS:
        ok, detail = check_internal(url)
        evaluate("endpoint:" + label, ok, detail, "endpoint/" + label)

    # 2c. Waitlist webhook handshake — 3 strikes before alerting
    ok, detail = check_waitlist()
    evaluate("endpoint:waitlist-handshake", ok, detail, "endpoint/waitlist :8099", min_failures=3)

    # 2d. Nginx config drift
    for name, live_path in NGINX_CONFIGS.items():
        ok, detail = check_nginx_drift(name, live_path)
        evaluate("nginx:" + name, ok, detail, "nginx/" + name)

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

    # 4b. Disk usage thresholds
    for mount, threshold in DISK_CHECKS:
        ok, detail = check_disk(mount, threshold)
        evaluate("disk:" + mount, ok, detail, "disk/" + mount)

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
