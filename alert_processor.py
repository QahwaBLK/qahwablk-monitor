#!/usr/bin/env python3
"""alert_processor.py — runs every 30 minutes (+90s offset).

Checks all alert sources, deduplicates, and sends Telegram messages.
Alert sources:
  - cashier_audit_log   (fail checks)
  - cashier_compliance_scores (fail commits, last 35 min)
  - cashier_build_status (stale builds)
  - systemctl is-active (service up/down)
  - df /               (disk > 85%)
  - pg_stat_activity   (idle in transaction > 1h)
  - ss -tlnp           (orphan processes on monitored ports)
"""

import os
import sys
import json
import subprocess
import urllib.request
import re
from datetime import datetime, timezone
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv("/srv/qahwablk/cashier-dashboard/.env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
REMINDER_INTERVAL_HOURS = 24
SERVER_ID = "RhythmOS (89.167.18.87)"

MONITORED_SERVICES = [
    "cashier-api",
    "cashier-frontend",
    "pulse-api",
    "pulse-frontend",
    "nginx",
    "postgresql",
]

MANAGED_PROC_NAMES = {"node", "python3", "python", "nginx", "postgres", "uvicorn", "next-server"}
MONITORED_PORTS = {3000, 3001, 3002, 3003, 8001, 8002, 8003}


def db_connect():
    cfg = {
        "dbname": os.getenv("PG_DBNAME", "qahwablk"),
        "user": os.getenv("PG_USER", "qahwablk"),
    }
    if os.getenv("PG_HOST"):
        cfg["host"] = os.getenv("PG_HOST")
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    return conn


def now_utc():
    return datetime.now(tz=timezone.utc)


def fmt_ts(dt):
    if dt is None:
        return "unknown"
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[alert_processor] Telegram not configured", file=sys.stderr)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        print(f"[alert_processor] Telegram send failed: {e}", file=sys.stderr)


def process_alert(cur, alert_key: str, description: str, details: str, category: str) -> str:
    """Handle dedup and send Telegram. Returns: new|recurred|reminder|suppressed."""
    cur.execute(
        "SELECT id, first_detected_at, last_alerted_at, is_active FROM cashier_alert_state WHERE alert_key = %s",
        (alert_key,),
    )
    row = cur.fetchone()
    now = now_utc()

    if row is None:
        cur.execute(
            """
            INSERT INTO cashier_alert_state
                (alert_key, first_detected_at, last_detected_at, last_alerted_at, alert_count, is_active)
            VALUES (%s, %s, %s, %s, 1, true)
            """,
            (alert_key, now, now, now),
        )
        send_telegram(
            f"🔴 <b>{category} ALERT</b>\n"
            f"{description}\n"
            f"Details: {details}\n"
            f"Server: {SERVER_ID}\n"
            f"Time: {fmt_ts(now)}"
        )
        print(f"[alert_processor] NEW: {alert_key}")
        return "new"

    row_id = row["id"]
    first_detected_at = row["first_detected_at"]
    last_alerted_at = row["last_alerted_at"]
    is_active = row["is_active"]

    if not is_active:
        cur.execute(
            """
            UPDATE cashier_alert_state SET
                first_detected_at = %s,
                last_detected_at  = %s,
                last_alerted_at   = %s,
                resolved_at       = NULL,
                alert_count       = 1,
                is_active         = true
            WHERE id = %s
            """,
            (now, now, now, row_id),
        )
        send_telegram(
            f"🔴 <b>{category} ALERT (RECURRED)</b>\n"
            f"{description}\n"
            f"Details: {details}\n"
            f"Server: {SERVER_ID}\n"
            f"Time: {fmt_ts(now)}"
        )
        print(f"[alert_processor] RECURRED: {alert_key}")
        return "recurred"

    # Active — update count, check reminder
    cur.execute(
        "UPDATE cashier_alert_state SET last_detected_at = %s, alert_count = alert_count + 1 WHERE id = %s",
        (now, row_id),
    )
    hours_since_alert = (now - last_alerted_at).total_seconds() / 3600
    if hours_since_alert >= REMINDER_INTERVAL_HOURS:
        cur.execute(
            "UPDATE cashier_alert_state SET last_alerted_at = %s WHERE id = %s",
            (now, row_id),
        )
        send_telegram(
            f"🟡 <b>REMINDER: {category}</b>\n"
            f"Still active since {fmt_ts(first_detected_at)}\n"
            f"{description}\n"
            f"Server: {SERVER_ID}"
        )
        print(f"[alert_processor] REMINDER: {alert_key}")
        return "reminder"

    print(f"[alert_processor] suppressed (within 24h): {alert_key}")
    return "suppressed"


def resolve_alert(cur, alert_key: str, description: str, category: str) -> bool:
    """Mark resolved, send Telegram. Returns True if was active."""
    cur.execute(
        "SELECT id, first_detected_at, is_active FROM cashier_alert_state WHERE alert_key = %s",
        (alert_key,),
    )
    row = cur.fetchone()
    if row is None or not row["is_active"]:
        return False

    row_id = row["id"]
    first_detected_at = row["first_detected_at"]
    now = now_utc()
    duration = now - first_detected_at
    hours = int(duration.total_seconds() / 3600)
    minutes = int((duration.total_seconds() % 3600) / 60)
    duration_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"

    cur.execute(
        "UPDATE cashier_alert_state SET resolved_at = %s, is_active = false WHERE id = %s",
        (now, row_id),
    )
    send_telegram(
        f"🟢 <b>RESOLVED: {category}</b>\n"
        f"{description}\n"
        f"Active for: {duration_str}\n"
        f"Server: {SERVER_ID}"
    )
    print(f"[alert_processor] RESOLVED: {alert_key}")
    return True


# ── Alert source checks ─────────────────────────────────────────────────────

def check_audit_alerts(cur):
    """Alert on latest system audit failures."""
    cur.execute("""
        SELECT DISTINCT ON (check_name) check_name, status, details
        FROM cashier_audit_log
        ORDER BY check_name, created_at DESC
    """)
    rows = cur.fetchall()

    active_keys = set()
    for row in rows:
        check_name = row["check_name"]
        status = row["status"]
        details = row["details"] or {}
        alert_key = f"audit:{check_name}"

        if status == "fail":
            active_keys.add(alert_key)
            count = details.get("count", 0) if isinstance(details, dict) else 0
            candidates = (
                details.get("matches") or details.get("failures") or
                details.get("stale") or details.get("stuck") or []
            ) if isinstance(details, dict) else []
            first_item = str(candidates[0])[:200] if candidates else str(details)[:200]
            description = f"System audit failure: {check_name.replace('_', ' ')}"
            detail_str = f"{count} issue(s). First: {first_item}" if count else first_item
            process_alert(cur, alert_key, description, detail_str, "AUDIT")

    # Resolve audit alerts now passing
    cur.execute(
        "SELECT alert_key FROM cashier_alert_state WHERE is_active = true AND alert_key LIKE 'audit:%'"
    )
    active_in_db = {r["alert_key"] for r in cur.fetchall()}
    for alert_key in active_in_db - active_keys:
        check_name = alert_key[len("audit:"):]
        resolve_alert(cur, alert_key, f"Audit check now passing: {check_name.replace('_', ' ')}", "AUDIT")


def check_compliance_alerts(cur):
    """Alert on failing compliance commits in the last 35 minutes."""
    cur.execute("""
        SELECT cs.score, cs.findings, cc.commit_hash, cc.commit_message, cc.repo_name
        FROM cashier_compliance_scores cs
        JOIN cashier_code_changes cc ON cc.id = cs.commit_id
        WHERE cs.overall_status = 'fail'
          AND cs.scored_at >= NOW() - INTERVAL '35 minutes'
    """)
    for row in cur.fetchall():
        alert_key = f"compliance:{row['commit_hash']}"
        findings = row["findings"] or []
        failed_rules = list({f.get("rule_key") for f in findings if isinstance(f, dict) and f.get("severity") == "fail"})
        rules_str = ", ".join(failed_rules) if failed_rules else "unknown rules"
        description = f"Non-compliant commit in {row['repo_name']}: score {row['score']}/100"
        detail_str = f"Rules: {rules_str}. Commit: {row['commit_message'][:100]}"
        process_alert(cur, alert_key, description, detail_str, "COMPLIANCE")


def check_build_alerts(cur):
    """Alert on stale builds."""
    cur.execute("SELECT service_name, repo_name, last_restart_at, latest_commit_at, is_stale FROM cashier_build_status")
    rows = cur.fetchall()

    active_keys = set()
    for row in rows:
        alert_key = f"build:{row['service_name']}:stale"
        if row["is_stale"]:
            active_keys.add(alert_key)
            description = f"Stale build: {row['service_name']} ({row['repo_name']})"
            detail_str = (
                f"Restarted: {fmt_ts(row['last_restart_at'])}, "
                f"latest commit: {fmt_ts(row['latest_commit_at'])}"
            )
            process_alert(cur, alert_key, description, detail_str, "BUILD")

    cur.execute("SELECT alert_key FROM cashier_alert_state WHERE is_active = true AND alert_key LIKE 'build:%:stale'")
    active_in_db = {r["alert_key"] for r in cur.fetchall()}
    for alert_key in active_in_db - active_keys:
        svc = alert_key[len("build:"):-len(":stale")]
        resolve_alert(cur, alert_key, f"Build now current: {svc}", "BUILD")


def check_service_alerts(cur):
    """Alert on non-active systemd services."""
    active_keys = set()
    for svc in MONITORED_SERVICES:
        result = subprocess.run(["systemctl", "is-active", svc], capture_output=True, text=True)
        state = result.stdout.strip()
        alert_key = f"service:{svc}:down"
        if state != "active":
            active_keys.add(alert_key)
            process_alert(cur, alert_key, f"Service not active: {svc}", f"systemctl state: {state or 'unknown'}", "SERVICE")

    cur.execute("SELECT alert_key FROM cashier_alert_state WHERE is_active = true AND alert_key LIKE 'service:%:down'")
    active_in_db = {r["alert_key"] for r in cur.fetchall()}
    for alert_key in active_in_db - active_keys:
        svc = alert_key[len("service:"):-len(":down")]
        resolve_alert(cur, alert_key, f"Service restored: {svc}", "SERVICE")


def check_disk_alerts(cur):
    """Alert if disk usage > 85%."""
    result = subprocess.run(["df", "/"], capture_output=True, text=True)
    lines = result.stdout.strip().splitlines()
    if len(lines) < 2:
        return
    parts = lines[1].split()
    if len(parts) < 5:
        return
    try:
        use_pct = int(parts[4].rstrip("%"))
    except ValueError:
        return
    alert_key = "system:disk:high"
    if use_pct > 85:
        process_alert(cur, alert_key, f"High disk usage: {use_pct}%", f"{parts[2]} used of {parts[1]} blocks", "SYSTEM")
    else:
        resolve_alert(cur, alert_key, f"Disk usage normal: {use_pct}%", "SYSTEM")


def check_postgres_alerts(cur):
    """Alert on idle in transaction connections > 1 hour."""
    cur.execute("""
        SELECT pid, datname, usename,
               EXTRACT(EPOCH FROM (NOW() - state_change))::int AS idle_seconds
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND state_change < NOW() - INTERVAL '1 hour'
    """)
    rows = cur.fetchall()

    active_keys = set()
    for row in rows:
        alert_key = f"postgres:idle_txn:{row['pid']}"
        active_keys.add(alert_key)
        idle_h = row["idle_seconds"] // 3600
        idle_m = (row["idle_seconds"] % 3600) // 60
        description = f"PostgreSQL idle in transaction: PID {row['pid']}"
        detail_str = f"DB: {row['datname']}, User: {row['usename']}, Idle: {idle_h}h {idle_m}m"
        process_alert(cur, alert_key, description, detail_str, "POSTGRES")

    cur.execute("SELECT alert_key FROM cashier_alert_state WHERE is_active = true AND alert_key LIKE 'postgres:idle_txn:%'")
    active_in_db = {r["alert_key"] for r in cur.fetchall()}
    for alert_key in active_in_db - active_keys:
        pid = alert_key.split(":")[-1]
        resolve_alert(cur, alert_key, f"Idle transaction resolved: PID {pid}", "POSTGRES")


def check_orphan_alerts(cur):
    """Alert on unrecognized processes on monitored ports."""
    result = subprocess.run(["ss", "-tlnp"], capture_output=True, text=True)
    active_keys = set()
    for line in result.stdout.splitlines():
        if "LISTEN" not in line:
            continue
        for port in MONITORED_PORTS:
            if f":{port} " in line or line.rstrip().endswith(f":{port}"):
                m = re.search(r'users:\(\("([^"]+)"', line)
                if m:
                    proc_name = m.group(1).split("/")[-1]
                    if not any(proc_name.startswith(m) for m in MANAGED_PROC_NAMES):
                        alert_key = f"process:orphan:{port}"
                        active_keys.add(alert_key)
                        process_alert(cur, alert_key, f"Unrecognized process on port {port}: {proc_name}", f"Process: {proc_name}", "PROCESS")

    cur.execute("SELECT alert_key FROM cashier_alert_state WHERE is_active = true AND alert_key LIKE 'process:orphan:%'")
    active_in_db = {r["alert_key"] for r in cur.fetchall()}
    for alert_key in active_in_db - active_keys:
        port_str = alert_key.split(":")[-1]
        resolve_alert(cur, alert_key, f"Port {port_str} no longer has orphan process", "PROCESS")


def main():
    started = now_utc()
    print(f"[alert_processor] Starting at {fmt_ts(started)}")

    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    checks = [
        ("audit", check_audit_alerts),
        ("compliance", check_compliance_alerts),
        ("build", check_build_alerts),
        ("service", check_service_alerts),
        ("disk", check_disk_alerts),
        ("postgres", check_postgres_alerts),
        ("orphan", check_orphan_alerts),
    ]

    for name, fn in checks:
        try:
            fn(cur)
        except Exception as e:
            import traceback
            print(f"[alert_processor] {name} check ERROR: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

    cur.close()
    conn.close()
    print(f"[alert_processor] Done in {int((now_utc() - started).total_seconds())}s")


if __name__ == "__main__":
    main()
