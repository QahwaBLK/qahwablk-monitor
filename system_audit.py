#!/usr/bin/env python3
"""
System Audit — full codebase and server scan. Runs every 6 hours.
Inserts one row per check into cashier_audit_log.
"""

import glob
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil
import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

ENV_FILE  = "/srv/qahwablk/cashier-dashboard/.env"
LOG_FILE  = "/var/log/qahwablk/system_audit.log"
REPO_BASE = "/srv/qahwablk"
REPOS = [
    "cashier-dashboard", "pulse", "beat",
    "cashier-intelligence", "sales-intelligence",
    "inventory-intelligence", "zenhr-pipeline", "operate-pipeline",
]

# beat-api and beat-frontend services are inactive; beat/ is deprecated.
# Its scripts still exist on disk but nothing runs them. Exclude from security
# scans to avoid false positives on dead code.
SECURITY_REPOS = [r for r in REPOS if r != "beat"]
MONITORED_PORTS = set(range(3000, 3004)) | set(range(8001, 8004))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ── DB ───────────────────────────────────────────────────────────────────────

def db_connect():
    env = dotenv_values(ENV_FILE)
    cfg = {"dbname": env.get("PG_DBNAME", "qahwablk"), "user": env.get("PG_USER", "qahwablk")}
    if env.get("PG_HOST"):
        cfg["host"] = env["PG_HOST"]
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    return conn


def insert_check(cur, audit_type: str, check_name: str, status: str, details: dict):
    cur.execute(
        "INSERT INTO cashier_audit_log (audit_type, check_name, status, details) VALUES (%s, %s, %s, %s)",
        (audit_type, check_name, status, json.dumps(details)),
    )
    icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}.get(status, "?")
    log.info(f"  [{audit_type}] {check_name}: {icon} {status}")


def sh(*args, cwd=None, timeout=30) -> str:
    try:
        r = subprocess.run(list(args), cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""


# ── SECURITY ─────────────────────────────────────────────────────────────────

def audit_security(cur):
    log.info("[SECURITY]")

    # ssl_cert_none — git grep scans only tracked working tree files, never .git/
    matches = []
    for repo in SECURITY_REPOS:
        rp = f"{REPO_BASE}/{repo}"
        if not os.path.isdir(f"{rp}/.git"):
            continue
        out = sh("git", "grep", "-n", "-E", r"ssl\.CERT_NONE|verify=False|verify\s*=\s*False",
                 "--", "*.py", cwd=rp)
        if out:
            matches.extend([f"{repo}: {l}" for l in out.splitlines()[:5]])
    insert_check(cur, "security", "ssl_cert_none",
                 "fail" if matches else "pass",
                 {"matches": matches, "count": len(matches)})

    # hardcoded_credentials
    cred_rx = re.compile(r'(password|passwd|secret|api_key)\s*=\s*[\'"][^\'"]{4,}[\'"]', re.IGNORECASE)
    safe_rx = re.compile(r'os\.(environ|getenv)', re.IGNORECASE)
    cred_matches = []
    for repo in SECURITY_REPOS:
        rp = f"{REPO_BASE}/{repo}"
        for py in Path(rp).rglob("*.py"):
            if ".git" in py.parts or "__pycache__" in py.parts:
                continue
            try:
                for lineno, line in enumerate(py.read_text(errors="ignore").splitlines(), 1):
                    if cred_rx.search(line) and not safe_rx.search(line):
                        rel = str(py.relative_to(REPO_BASE))
                        cred_matches.append(f"{rel}:{lineno}: {line.strip()[:100]}")
                        if len(cred_matches) >= 10:
                            break
            except Exception:
                pass
    insert_check(cur, "security", "hardcoded_credentials",
                 "fail" if cred_matches else "pass",
                 {"matches": cred_matches[:10], "count": len(cred_matches)})

    # root_db_user — only flag literal user="root" assignments, not env-var defaults
    # e.g. psycopg2.connect(user="root") is flagged; os.environ.get("DB_USER", "root") is NOT
    root_matches = []
    for repo in SECURITY_REPOS:
        rp = f"{REPO_BASE}/{repo}"
        if not os.path.isdir(f"{rp}/.git"):
            continue
        # Pattern matches user="root" or user='root' only when assigned a literal string
        for py in Path(rp).rglob("*.py"):
            if ".git" in py.parts or "__pycache__" in py.parts:
                continue
            try:
                text = py.read_text(errors="ignore")
                for lineno, line in enumerate(text.splitlines(), 1):
                    # Must contain user="root" or user='root' as a literal value
                    if re.search(r'\buser\s*=\s*["\'\']root["\'\']\b', line, re.IGNORECASE):
                        # Skip lines where the assignment goes through os.environ/os.getenv
                        if re.search(r'os\.(environ|getenv)', line, re.IGNORECASE):
                            continue
                        rel = str(py.relative_to(REPO_BASE))
                        root_matches.append(f"{rel}:{lineno}: {line.strip()[:100]}")
            except Exception:
                pass
    insert_check(cur, "security", "root_db_user",
                 "fail" if root_matches else "pass",
                 {"matches": root_matches[:10], "count": len(root_matches)})

    # env_permissions
    env_perm_issues = []
    for repo in SECURITY_REPOS:
        env_path = Path(f"{REPO_BASE}/{repo}/.env")
        if env_path.exists():
            mode = oct(env_path.stat().st_mode & 0o777)
            if mode not in ("0o600", "0o400"):
                env_perm_issues.append({"path": str(env_path), "mode": mode})
    insert_check(cur, "security", "env_permissions",
                 "fail" if env_perm_issues else "pass",
                 {"issues": env_perm_issues})

    # env_in_git
    env_in_git = []
    for repo in SECURITY_REPOS:
        rp = f"{REPO_BASE}/{repo}"
        if not os.path.isdir(f"{rp}/.git"):
            continue
        tracked = sh("git", "ls-files", ".env", cwd=rp)
        if tracked:
            env_in_git.append({"repo": repo, "file": tracked})
    insert_check(cur, "security", "env_in_git",
                 "fail" if env_in_git else "pass",
                 {"tracked": env_in_git})


# ── CODE HYGIENE ─────────────────────────────────────────────────────────────

def audit_code_hygiene(cur):
    log.info("[CODE_HYGIENE]")

    # uncommitted_files
    dirty_repos = []
    for repo in REPOS:
        rp = f"{REPO_BASE}/{repo}"
        if not os.path.isdir(f"{rp}/.git"):
            continue
        out = sh("git", "status", "--porcelain", cwd=rp)
        count = len([l for l in out.splitlines() if l.strip()])
        if count > 0:
            dirty_repos.append({"repo": repo, "count": count})
    insert_check(cur, "code_hygiene", "uncommitted_files",
                 "warn" if dirty_repos else "pass",
                 {"dirty_repos": dirty_repos})

    # unpushed_commits
    unpushed_repos = []
    for repo in REPOS:
        rp = f"{REPO_BASE}/{repo}"
        if not os.path.isdir(f"{rp}/.git"):
            continue
        out = sh("git", "log", "origin/main..HEAD", "--oneline", cwd=rp)
        count = len([l for l in out.splitlines() if l.strip()])
        if count > 0:
            unpushed_repos.append({"repo": repo, "count": count})
    insert_check(cur, "code_hygiene", "unpushed_commits",
                 "warn" if unpushed_repos else "pass",
                 {"unpushed_repos": unpushed_repos})

    # root_path_refs
    root_refs = []
    for repo in REPOS:
        rp = f"{REPO_BASE}/{repo}"
        for py in Path(rp).rglob("*.py"):
            if ".git" in py.parts or "__pycache__" in py.parts:
                continue
            try:
                for lineno, line in enumerate(py.read_text(errors="ignore").splitlines(), 1):
                    if "/root/" in line and not line.strip().startswith("#"):
                        rel = str(py.relative_to(REPO_BASE))
                        root_refs.append(f"{rel}:{lineno}: {line.strip()[:100]}")
            except Exception:
                pass
    insert_check(cur, "code_hygiene", "root_path_refs",
                 "warn" if root_refs else "pass",
                 {"matches": root_refs[:15], "count": len(root_refs)})

    # ruff_violations
    ruff_totals = {}
    for repo in REPOS:
        rp = f"{REPO_BASE}/{repo}"
        if not any(Path(rp).rglob("*.py")):
            continue
        out = sh("/usr/local/bin/ruff", "check", "--statistics", "--quiet", rp, timeout=60)
        if out:
            lines = [l for l in out.splitlines() if l.strip()]
            try:
                total_violations = sum(int(l.split()[0]) for l in lines if l.split()[0].isdigit())
            except Exception:
                total_violations = len(lines)
            ruff_totals[repo] = {"violations": total_violations, "summary": "\n".join(lines[:10])}
    total_all = sum(v["violations"] for v in ruff_totals.values())
    insert_check(cur, "code_hygiene", "ruff_violations",
                 "warn" if total_all > 0 else "pass",
                 {"total": total_all, "by_repo": ruff_totals})


# ── PIPELINE HEALTH ───────────────────────────────────────────────────────────

# Only pipelines that actually log to pipeline_run_log (verified 2026-03-13).
# Scripts that don't use pipeline_run_log (zenhr-pipeline/*, scrap-variance, etc.)
# are monitored via log file freshness instead.
PIPELINE_WINDOWS = {
    "cashier-intelligence/main":             30 * 60,   # every 15 min during business hours
    "cashier-intelligence/grubtech-recheck": 60 * 60,   # every 30 min
    "cashier-intelligence/pending-pickings": 20 * 60,   # every 10 min
    "cashier-intelligence/stock-levels":     2 * 3600,  # hourly
    "inventory-intelligence/main":           36 * 3600, # daily overnight
    "sales-intelligence/main":               36 * 3600, # daily overnight
    "sales-intelligence/ceo-dashboard":      36 * 3600, # daily overnight
    "operate-pipeline/sync-tasks":           20 * 60,   # every 10 min during hours
    "zenhr-pipeline/attendance":             4 * 3600,  # every 30 min business hours; 4h for off-hours slack
    "pulse/daily-points":                    30 * 60,   # every 15 min during business hours
    "pulse/streak-calculator":               36 * 3600, # daily
    "pulse/pos-attribution":                 30 * 60,   # every 15 min
    "pulse/cups-served":                     30 * 60,   # every 15 min
    "pulse/score-calculator":                60 * 60,   # every 30 min
    "pulse/health-calculator":               36 * 3600, # daily
}


def audit_pipeline_health(cur, db_conn):
    log.info("[PIPELINE_HEALTH]")
    db_cur = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    now = datetime.now(timezone.utc)

    # stale_pipelines: expected to run within their window but haven't
    db_cur.execute("""
        SELECT DISTINCT ON (pipeline_name)
            pipeline_name, started_at, status, finished_at
        FROM pipeline_run_log
        ORDER BY pipeline_name, started_at DESC
    """)
    latest_runs = {r["pipeline_name"]: dict(r) for r in db_cur.fetchall()}

    stale = []
    for name, window in PIPELINE_WINDOWS.items():
        row = latest_runs.get(name)
        if not row or not row["started_at"]:
            stale.append({"pipeline": name, "reason": "never run"})
            continue
        ts = row["started_at"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (now - ts).total_seconds()
        if age > window * 2:  # 2x window = stale
            stale.append({"pipeline": name, "age_hours": round(age / 3600, 1), "last_run": ts.isoformat()})

    insert_check(cur, "pipeline_health", "stale_pipelines",
                 "fail" if stale else "pass",
                 {"stale": stale[:20], "count": len(stale)})

    # failed_pipelines: errors in last 24 hours
    db_cur.execute("""
        SELECT pipeline_name, started_at, LEFT(error_message, 200) as error_message
        FROM pipeline_run_log
        WHERE status = 'error' AND started_at >= NOW() - INTERVAL '24 hours'
        ORDER BY started_at DESC
        LIMIT 20
    """)
    failed = [dict(r) for r in db_cur.fetchall()]
    for f in failed:
        if f["started_at"]:
            f["started_at"] = f["started_at"].isoformat()
    insert_check(cur, "pipeline_health", "failed_pipelines",
                 "fail" if failed else "pass",
                 {"failures": failed, "count": len(failed)})

    # stuck_pipelines: running for > 2 hours with no finish
    db_cur.execute("""
        SELECT pipeline_name, started_at
        FROM pipeline_run_log
        WHERE status = 'running'
          AND started_at < NOW() - INTERVAL '2 hours'
          AND finished_at IS NULL
        ORDER BY started_at ASC
    """)
    stuck = []
    for r in db_cur.fetchall():
        ts = r["started_at"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        stuck.append({"pipeline": r["pipeline_name"], "stuck_since": ts.isoformat(),
                      "hours": round((now - ts).total_seconds() / 3600, 1)})
    insert_check(cur, "pipeline_health", "stuck_pipelines",
                 "fail" if stuck else "pass",
                 {"stuck": stuck})


# ── DATA FRESHNESS ────────────────────────────────────────────────────────────

def audit_data_freshness(cur, db_conn):
    log.info("[DATA_FRESHNESS]")
    db_cur = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    now = datetime.now(timezone.utc)

    checks = [
        ("cashier_daily_metrics", "date",          "Cashier Daily Metrics", 36 * 3600, "warn"),
        ("shop_daily_health",     "date",           "Shop Daily Health",     36 * 3600, "warn"),
        ("zenhr_attendance",      "updated_at",     "ZenHR Attendance",      48 * 3600, "fail"),
        ("zenhr_employees",       "updated_at",     "ZenHR Employees",       72 * 3600, "fail"),
        ("operate_daily_tasks",   "synced_at",      "Operate Tasks",         24 * 3600, "warn"),
    ]

    for table, col, label, max_age_sec, fail_sev in checks:
        try:
            db_cur.execute(f"SELECT MAX({col}) AS ts FROM {table}")
            row = db_cur.fetchone()
            ts = row["ts"] if row else None
            if not ts:
                insert_check(cur, "data_freshness", f"freshness:{table}",
                             fail_sev, {"label": label, "age_seconds": None, "reason": "no records"})
                continue
            if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            # For date-only values, interpret as start of day UTC
            if not hasattr(ts, "hour"):
                ts = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)
            age = (now - ts).total_seconds()
            status = "pass" if age <= max_age_sec else ("warn" if fail_sev == "warn" else "fail")
            insert_check(cur, "data_freshness", f"freshness:{table}",
                         status,
                         {"label": label, "age_hours": round(age / 3600, 1),
                          "max_age_hours": round(max_age_sec / 3600, 1),
                          "last_record": ts.isoformat()})
        except Exception as e:
            insert_check(cur, "data_freshness", f"freshness:{table}",
                         "warn", {"label": label, "error": str(e)})


# ── PROCESS HEALTH ────────────────────────────────────────────────────────────

def audit_process_health(cur):
    log.info("[PROCESS_HEALTH]")

    # orphan_processes
    orphans = []
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if conn.laddr and conn.laddr.port in MONITORED_PORTS and conn.status == "LISTEN" and conn.pid:
                try:
                    unit = None
                    with open(f"/proc/{conn.pid}/cgroup") as f:
                        for line in f:
                            if ".service" in line:
                                parts = line.strip().split("/")
                                for part in reversed(parts):
                                    if part.endswith(".service"):
                                        unit = part
                                        break
                                break
                    if unit:
                        r = subprocess.run(["systemctl", "is-active", unit],
                                           capture_output=True, text=True, timeout=2)
                        if r.stdout.strip() != "active":
                            orphans.append({"port": conn.laddr.port, "pid": conn.pid, "unit": unit})
                    else:
                        orphans.append({"port": conn.laddr.port, "pid": conn.pid, "unit": None})
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"net_connections failed: {e}")
    insert_check(cur, "process_health", "orphan_processes",
                 "fail" if orphans else "pass",
                 {"orphans": orphans})

    # restart_always_services
    always_restart = []
    for pat in ["/etc/systemd/system/*.service", "/lib/systemd/system/*.service"]:
        for path in glob.glob(pat):
            try:
                with open(path) as f:
                    content = f.read()
                if "Restart=always" in content:
                    always_restart.append(os.path.basename(path))
            except Exception:
                pass
    insert_check(cur, "process_health", "restart_always_services",
                 "warn" if always_restart else "pass",
                 {"services": always_restart, "count": len(always_restart)})

    # zombie_processes
    zombies = []
    for proc in psutil.process_iter(["pid", "name", "status"]):
        try:
            if proc.info["status"] == psutil.STATUS_ZOMBIE:
                zombies.append({"pid": proc.info["pid"], "name": proc.info["name"]})
        except Exception:
            pass
    insert_check(cur, "process_health", "zombie_processes",
                 "warn" if zombies else "pass",
                 {"zombies": zombies, "count": len(zombies)})


# ── POSTGRES HEALTH ───────────────────────────────────────────────────────────

def audit_postgres_health(cur, db_conn):
    log.info("[POSTGRES_HEALTH]")
    db_cur = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # idle_in_transaction
    db_cur.execute("""
        SELECT pid, datname, usename, LEFT(query, 200) as query,
               EXTRACT(EPOCH FROM (NOW() - state_change))::int AS duration_seconds
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND state_change < NOW() - INTERVAL '1 hour'
    """)
    idle_txn = [dict(r) for r in db_cur.fetchall()]
    insert_check(cur, "postgres_health", "idle_in_transaction",
                 "fail" if idle_txn else "pass",
                 {"idle": idle_txn, "count": len(idle_txn)})

    # connection_saturation
    db_cur.execute("SELECT setting::int AS max_conn FROM pg_settings WHERE name = 'max_connections'")
    max_conn = db_cur.fetchone()["max_conn"]
    db_cur.execute("SELECT COUNT(*) AS active FROM pg_stat_activity WHERE state IS NOT NULL")
    active_conn = db_cur.fetchone()["active"]
    pct = round(active_conn / max_conn * 100, 1)
    insert_check(cur, "postgres_health", "connection_saturation",
                 "fail" if pct >= 90 else "warn" if pct >= 80 else "pass",
                 {"active": active_conn, "max": max_conn, "percent": pct})

    # long_running_queries
    db_cur.execute("""
        SELECT pid, datname, usename,
               LEFT(query, 200) AS query,
               EXTRACT(EPOCH FROM (NOW() - query_start))::int AS duration_seconds
        FROM pg_stat_activity
        WHERE state = 'active'
          AND query_start < NOW() - INTERVAL '5 minutes'
          AND query NOT ILIKE '%pg_stat_activity%'
    """)
    long_queries = [dict(r) for r in db_cur.fetchall()]
    insert_check(cur, "postgres_health", "long_running_queries",
                 "fail" if long_queries else "pass",
                 {"queries": long_queries[:10], "count": len(long_queries)})


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== system_audit start ===")
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        audit_security(cur)
    except Exception as e:
        log.error(f"Security audit failed: {e}")

    try:
        audit_code_hygiene(cur)
    except Exception as e:
        log.error(f"Code hygiene audit failed: {e}")

    try:
        audit_pipeline_health(cur, conn)
    except Exception as e:
        log.error(f"Pipeline health audit failed: {e}")
        # Insert fallback check
        cur.execute(
            "INSERT INTO cashier_audit_log (audit_type, check_name, status, details) VALUES ('pipeline_health', 'pipeline_health_error', 'warn', %s)",
            (json.dumps({"error": str(e)}),),
        )

    try:
        audit_data_freshness(cur, conn)
    except Exception as e:
        log.error(f"Data freshness audit failed: {e}")

    try:
        audit_process_health(cur)
    except Exception as e:
        log.error(f"Process health audit failed: {e}")

    try:
        audit_postgres_health(cur, conn)
    except Exception as e:
        log.error(f"Postgres health audit failed: {e}")

    conn.close()
    log.info("=== system_audit done ===")


if __name__ == "__main__":
    main()
