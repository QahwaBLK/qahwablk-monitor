#!/usr/bin/env python3
"""
Build Freshness — compares running service start times vs latest git commits.
Flags stale builds (new commit deployed but service not restarted).
"""

import logging
import subprocess
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

# ── Config ──────────────────────────────────────────────────────────────────

ENV_FILE = "/srv/qahwablk/cashier-dashboard/.env"
LOG_FILE = "/var/log/qahwablk/build_freshness.log"

SERVICE_REPO_MAP = {
    "cashier-api":      "cashier-dashboard",
    "cashier-frontend": "cashier-dashboard",
    "pulse-api":        "pulse",
    "pulse-frontend":   "pulse",
    "beat-api":         "beat",
}

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


# ── Database ─────────────────────────────────────────────────────────────────

def db_connect():
    env = dotenv_values(ENV_FILE)
    cfg = {"dbname": env.get("PG_DBNAME", "qahwablk"), "user": env.get("PG_USER", "qahwablk")}
    host = env.get("PG_HOST")
    if host:
        cfg["host"] = host
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    return conn


# ── Systemctl ────────────────────────────────────────────────────────────────

def get_service_restart_time(service_name: str) -> datetime | None:
    """Get the ActiveEnterTimestamp from systemctl show."""
    try:
        result = subprocess.run(
            ["systemctl", "show", service_name, "-p", "ActiveEnterTimestamp,LoadState"],
            capture_output=True, text=True, timeout=5,
        )
        props = {}
        for line in result.stdout.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                props[k.strip()] = v.strip()

        if props.get("LoadState") == "not-found":
            return None

        ts_str = props.get("ActiveEnterTimestamp", "").strip()
        if not ts_str:
            return None

        # Format: "Mon 2026-03-09 06:32:29 UTC"
        try:
            dt = datetime.strptime(ts_str, "%a %Y-%m-%d %H:%M:%S %Z")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    except Exception as e:
        log.warning(f"systemctl show failed for {service_name}: {e}")
        return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== build_freshness start ===")
    try:
        conn = db_connect()
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        sys.exit(1)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    now = datetime.now(timezone.utc)

    for service_name, repo_name in SERVICE_REPO_MAP.items():
        try:
            last_restart = get_service_restart_time(service_name)
            if last_restart is None:
                log.info(f"[{service_name}] Service not found or no restart timestamp — skipping")
                continue

            # Latest commit for this repo
            cur.execute(
                """
                SELECT commit_hash, committed_at
                FROM cashier_code_changes
                WHERE repo_name = %s
                ORDER BY committed_at DESC
                LIMIT 1
                """,
                (repo_name,),
            )
            row = cur.fetchone()
            if not row:
                log.info(f"[{service_name}] No commits found for repo {repo_name}")
                latest_hash = None
                latest_at = None
                is_stale = False
            else:
                latest_hash = row["commit_hash"]
                latest_at = row["committed_at"]
                if latest_at.tzinfo is None:
                    latest_at = latest_at.replace(tzinfo=timezone.utc)
                is_stale = latest_at > last_restart

            log.info(
                f"[{service_name}] last_restart={last_restart.isoformat()}, "
                f"latest_commit={latest_at.isoformat() if latest_at else 'none'}, "
                f"stale={is_stale}"
            )

            cur.execute(
                """
                INSERT INTO cashier_build_status
                    (service_name, repo_name, last_restart_at, latest_commit_hash, latest_commit_at, is_stale, checked_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (service_name) DO UPDATE SET
                    repo_name          = EXCLUDED.repo_name,
                    last_restart_at    = EXCLUDED.last_restart_at,
                    latest_commit_hash = EXCLUDED.latest_commit_hash,
                    latest_commit_at   = EXCLUDED.latest_commit_at,
                    is_stale           = EXCLUDED.is_stale,
                    checked_at         = NOW()
                """,
                (service_name, repo_name, last_restart, latest_hash, latest_at, is_stale),
            )
        except Exception as e:
            log.error(f"[{service_name}] Error: {e}")

    conn.close()
    log.info("=== build_freshness done ===")


if __name__ == "__main__":
    main()
