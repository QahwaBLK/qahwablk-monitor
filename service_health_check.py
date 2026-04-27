#!/usr/bin/env python3
"""
Service Health Check — runs every 5 minutes.
Checks internal ports and external URLs. Detects nginx mismatches.
Sends Telegram alerts with 30-minute deduplication.
Logs to cashier_audit_log.
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

# ── Config ──────────────────────────────────────────────────────────────────

ENV_FILE  = "/srv/qahwablk/cashier-dashboard/.env"
LOG_FILE  = "/var/log/qahwablk/service_health_check.log"
BACKUP_DIR = Path("/srv/shared/server-docs/nginx-backups")

# (check_name, label, url, expected_ok)
# expected_ok: callable(status_code) -> bool
INTERNAL_CHECKS = [
    ("cashier_frontend_internal", "Cashier Frontend (internal)", "http://localhost:3000", lambda s: s == 200),
    ("pulse_frontend_internal",   "Pulse Frontend (internal)",   "http://localhost:3003", lambda s: s == 200),
    ("cashier_api_internal",      "Cashier API (internal)",      "http://localhost:8001/health", lambda s: s == 200),
    ("pulse_api_internal",        "Pulse API (internal)",        "http://localhost:8003/api/health", lambda s: s == 200),
]

EXTERNAL_CHECKS = [
    ("cashier_external", "Cashier (external)", "https://cashier.blk.jo"),
    ("pulse_external",   "Pulse (external)",   "https://pulse.blk.jo"),
]

# Map external check name → internal check name (for mismatch detection)
EXTERNAL_TO_INTERNAL = {
    "cashier_external": "cashier_frontend_internal",
    "pulse_external":   "pulse_frontend_internal",
}

NGINX_CONFIGS = {
    "cashier": "/etc/nginx/sites-enabled/cashier.blk.jo",
    "pulse":   "/etc/nginx/sites-enabled/pulse.blk.jo",
}

REMINDER_INTERVAL_SECONDS = 30 * 60  # 30 minutes

# ── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── DB ───────────────────────────────────────────────────────────────────────

def db_connect():
    env = dotenv_values(ENV_FILE)
    cfg = {"dbname": env.get("PG_DBNAME", "qahwablk"), "user": env.get("PG_USER", "qahwablk")}
    host = env.get("PG_HOST")
    if host:
        cfg["host"] = host
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    return conn


# ── Telegram ─────────────────────────────────────────────────────────────────

def send_telegram(bot_token: str, chat_id: str, message: str) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


# ── HTTP Check ───────────────────────────────────────────────────────────────

def http_check(url: str, timeout: int = 10) -> tuple[int | None, float]:
    """Returns (status_code_or_None, latency_ms)."""
    t0 = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "qahwablk-health-check/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status
    except urllib.error.HTTPError as e:
        status = e.code
    except Exception:
        status = None
    latency_ms = int((time.monotonic() - t0) * 1000)
    return status, latency_ms


# ── Nginx backup ─────────────────────────────────────────────────────────────

def ensure_nginx_backups():
    """Copy nginx configs to backup dir if not already there (first run)."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for name, src_path in NGINX_CONFIGS.items():
        dest = BACKUP_DIR / f"{name}.blk.jo.conf"
        if not dest.exists():
            try:
                shutil.copy2(src_path, dest)
                log.info(f"Nginx backup created: {dest}")
            except Exception as e:
                log.warning(f"Failed to backup {src_path}: {e}")


def nginx_diff(service_name: str) -> str | None:
    """Diff current nginx config vs known-good backup. Returns diff or None."""
    key = service_name.replace("_external", "").replace("_internal", "")
    # Map check name to nginx key
    if "cashier" in service_name:
        key = "cashier"
    elif "pulse" in service_name:
        key = "pulse"
    else:
        return None

    src = NGINX_CONFIGS.get(key)
    backup = BACKUP_DIR / f"{key}.blk.jo.conf"
    if not src or not backup.exists():
        return None
    try:
        result = subprocess.run(
            ["diff", str(backup), src],
            capture_output=True, text=True, timeout=5,
        )
        diff = result.stdout.strip()
        return diff if diff else None
    except Exception:
        return None


# ── Alert deduplication ───────────────────────────────────────────────────────

def should_alert(cur, alert_key: str) -> tuple[bool, str]:
    """
    Returns (should_send, alert_type) where alert_type is 'new'|'reminder'|'skip'.
    Side effect: upserts alert state.
    """
    cur.execute(
        "SELECT id, first_detected_at, last_alerted_at, alert_count, is_active FROM cashier_alert_state WHERE alert_key = %s",
        (alert_key,),
    )
    row = cur.fetchone()
    now = datetime.now(timezone.utc)

    if not row or not row["is_active"]:
        # New alert
        cur.execute(
            """
            INSERT INTO cashier_alert_state (alert_key, first_detected_at, last_detected_at, last_alerted_at, alert_count, is_active)
            VALUES (%s, NOW(), NOW(), NOW(), 1, true)
            ON CONFLICT (alert_key) DO UPDATE SET
                last_detected_at = NOW(),
                last_alerted_at  = NOW(),
                alert_count      = cashier_alert_state.alert_count + 1,
                resolved_at      = NULL,
                is_active        = true
            """,
            (alert_key,),
        )
        return True, "new"
    else:
        last_alerted = row["last_alerted_at"]
        if last_alerted.tzinfo is None:
            last_alerted = last_alerted.replace(tzinfo=timezone.utc)
        elapsed = (now - last_alerted).total_seconds()

        cur.execute(
            "UPDATE cashier_alert_state SET last_detected_at = NOW(), alert_count = alert_count + 1 WHERE alert_key = %s",
            (alert_key,),
        )

        if elapsed >= REMINDER_INTERVAL_SECONDS:
            cur.execute(
                "UPDATE cashier_alert_state SET last_alerted_at = NOW() WHERE alert_key = %s",
                (alert_key,),
            )
            return True, "reminder"
        return False, "skip"


def resolve_alert(cur, alert_key: str):
    """Mark an alert as resolved if it was active."""
    cur.execute(
        """
        UPDATE cashier_alert_state
        SET resolved_at = NOW(), is_active = false
        WHERE alert_key = %s AND is_active = true
        RETURNING id
        """,
        (alert_key,),
    )
    return cur.rowcount > 0


def log_audit(cur, check_name: str, status: str, details: dict):
    cur.execute(
        "INSERT INTO cashier_audit_log (audit_type, check_name, status, details) VALUES ('service_health', %s, %s, %s)",
        (check_name, status, json.dumps(details)),
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== service_health_check start ===")
    env = dotenv_values(ENV_FILE)
    bot_token = env.get("TELEGRAM_BOT_TOKEN", "")
    chat_id   = env.get("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        log.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")

    ensure_nginx_backups()

    try:
        conn = db_connect()
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        sys.exit(1)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ── Internal checks ──────────────────────────────────────────────────────
    internal_results: dict[str, dict] = {}
    for check_name, label, url, ok_fn in INTERNAL_CHECKS:
        status_code, latency_ms = http_check(url)
        ok = status_code is not None and ok_fn(status_code)
        internal_results[check_name] = {"ok": ok, "status_code": status_code, "latency_ms": latency_ms}
        audit_status = "pass" if ok else "fail"
        details = {"url": url, "status_code": status_code, "latency_ms": latency_ms}
        log_audit(cur, check_name, audit_status, details)
        log.info(f"[{check_name}] {status_code} {latency_ms}ms — {audit_status}")

        alert_key = f"service_health:{check_name}"
        if not ok:
            should_send, alert_type = should_alert(cur, alert_key)
            if should_send and bot_token:
                prefix = "🔴 SERVICE DOWN" if alert_type == "new" else "🟡 REMINDER: SERVICE DOWN"
                msg = (
                    f"{prefix}\n"
                    f"<b>{label}</b> is not responding\n"
                    f"URL: {url}\n"
                    f"Response: {status_code or 'timeout'} ({latency_ms}ms)\n"
                    f"Server: RhythmOS (89.167.18.87)\n"
                    f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                )
                send_telegram(bot_token, chat_id, msg)
        else:
            if resolve_alert(cur, alert_key):
                if bot_token:
                    msg = (
                        f"🟢 RESOLVED: SERVICE RECOVERED\n"
                        f"<b>{label}</b> is back up\n"
                        f"URL: {url}\n"
                        f"Server: RhythmOS (89.167.18.87)"
                    )
                    send_telegram(bot_token, chat_id, msg)

    # ── External checks + mismatch detection ─────────────────────────────────
    for check_name, label, url in EXTERNAL_CHECKS:
        status_code, latency_ms = http_check(url)
        is_502 = status_code == 502
        ok = status_code is not None and status_code != 502

        # Check for nginx mismatch
        internal_key = EXTERNAL_TO_INTERNAL[check_name]
        internal_ok = internal_results.get(internal_key, {}).get("ok", False)
        is_mismatch = internal_ok and is_502

        audit_status = "pass" if ok else "fail"
        details = {
            "url": url,
            "status_code": status_code,
            "latency_ms": latency_ms,
            "mismatch": is_mismatch,
        }

        if is_mismatch:
            diff = nginx_diff(check_name)
            if diff:
                details["nginx_diff"] = diff[:2000]  # cap size
            audit_status = "fail"

        log_audit(cur, check_name, audit_status, details)
        log.info(f"[{check_name}] {status_code} {latency_ms}ms mismatch={is_mismatch} — {audit_status}")

        alert_key = f"service_health:{check_name}"
        mismatch_key = f"service_health:nginx_mismatch:{check_name}"

        if is_mismatch:
            # Nginx mismatch alert
            should_send, alert_type = should_alert(cur, mismatch_key)
            if should_send and bot_token:
                prefix = "🔴 NGINX MISMATCH" if alert_type == "new" else "🟡 REMINDER: NGINX MISMATCH"
                diff_text = details.get("nginx_diff", "No diff available")
                msg = (
                    f"{prefix}\n"
                    f"<b>{label}</b> responds on localhost but public URL returns 502\n"
                    f"Check nginx config for {url.split('//')[1]}\n"
                    f"Diff vs known-good:\n<pre>{diff_text[:800]}</pre>\n"
                    f"Server: RhythmOS (89.167.18.87)\n"
                    f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                )
                send_telegram(bot_token, chat_id, msg)
        else:
            resolve_alert(cur, mismatch_key)

        if not ok and not is_mismatch:
            should_send, alert_type = should_alert(cur, alert_key)
            if should_send and bot_token:
                prefix = "🔴 EXTERNAL URL DOWN" if alert_type == "new" else "🟡 REMINDER: EXTERNAL URL DOWN"
                msg = (
                    f"{prefix}\n"
                    f"<b>{label}</b> returned {status_code or 'timeout'}\n"
                    f"URL: {url}\n"
                    f"Server: RhythmOS (89.167.18.87)\n"
                    f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                )
                send_telegram(bot_token, chat_id, msg)
        else:
            resolve_alert(cur, alert_key)

    conn.close()
    log.info("=== service_health_check done ===")


if __name__ == "__main__":
    main()
