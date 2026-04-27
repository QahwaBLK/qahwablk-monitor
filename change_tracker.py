#!/usr/bin/env python3
"""
Change Tracker — scans all RhythmOS repos for new git commits every 30 min.
Stores results in cashier_code_changes.
"""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

# ── Config ──────────────────────────────────────────────────────────────────

ENV_FILE = "/srv/qahwablk/cashier-dashboard/.env"
LOG_FILE = "/var/log/qahwablk/change_tracker.log"

REPOS = [
    "cashier-dashboard",
    "pulse",
    "beat",
    "cashier-intelligence",
    "sales-intelligence",
    "inventory-intelligence",
    "zenhr-pipeline",
    "operate-pipeline",
]

REPO_BASE = "/srv/qahwablk"

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


# ── Git helpers ──────────────────────────────────────────────────────────────

def git(repo_path: str, *args, timeout=30) -> str:
    """Run a git command in the given repo dir; return stdout."""
    result = subprocess.run(
        ["git"] + list(args),
        cwd=repo_path,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.stdout.strip()


def get_unpushed_hashes(repo_path: str) -> set:
    """Return set of commit hashes that exist locally but not on origin/main."""
    try:
        out = git(repo_path, "log", "origin/main..HEAD", "--pretty=%H")
        return set(out.splitlines()) if out else set()
    except Exception:
        try:
            out = git(repo_path, "log", "origin/HEAD..HEAD", "--pretty=%H")
            return set(out.splitlines()) if out else set()
        except Exception:
            return set()


def generate_diff_summary(message: str, files: list[dict]) -> str:
    """
    Generate a short plain-English summary from the commit message and file list.
    No LLM — just heuristics.
    """
    msg = message.strip().split("\n")[0]  # first line only

    # Expand conventional commit prefixes
    prefixes = {
        "feat:": "Added", "feature:": "Added",
        "fix:": "Fixed", "bugfix:": "Fixed",
        "refactor:": "Refactored",
        "chore:": "Updated",
        "docs:": "Updated documentation for",
        "style:": "Styled",
        "test:": "Added tests for",
        "perf:": "Improved performance of",
        "revert:": "Reverted",
    }
    summary_start = ""
    lowmsg = msg.lower()
    for prefix, verb in prefixes.items():
        if lowmsg.startswith(prefix):
            summary_start = verb + " " + msg[len(prefix):].strip()
            break

    if not summary_start:
        summary_start = msg

    # If only 1-2 files changed, mention what was touched
    if files and len(files) <= 3:
        filenames = [os.path.basename(f["filename"]) for f in files[:3]]
        return f"{summary_start} ({', '.join(filenames)})"

    return summary_start


def parse_commits(repo_path: str, since_hash: str | None, limit: int = 50) -> list[dict]:
    """
    Parse commits from git log. Returns list of commit dicts.
    Each dict: hash, message, author, committed_at, numstat_lines.
    """
    if since_hash:
        rev_range = f"{since_hash}..HEAD"
    else:
        rev_range = f"HEAD~{limit}..HEAD"

    # Get commits with metadata
    sep = "\x1f"
    rec_sep = "\x1e"
    fmt = f"%H{sep}%an{sep}%aI{sep}%s{rec_sep}"
    commits_raw = git(repo_path, "log", rev_range, f"--pretty=format:{fmt}")
    if not commits_raw:
        return []

    commits = []
    for record in commits_raw.split(rec_sep):
        record = record.strip()
        if not record:
            continue
        parts = record.split(sep)
        if len(parts) < 4:
            continue
        hash_, author, date_str, subject = parts[0], parts[1], parts[2], parts[3]
        try:
            committed_at = datetime.fromisoformat(date_str)
        except ValueError:
            committed_at = None
        commits.append({
            "hash": hash_,
            "author": author,
            "committed_at": committed_at,
            "message": subject,
        })

    # Get numstat per commit
    for commit in commits:
        numstat_out = git(repo_path, "show", "--numstat", "--format=", commit["hash"])
        files = []
        insertions_total = 0
        deletions_total = 0
        for line in numstat_out.splitlines():
            parts = line.split("\t")
            if len(parts) == 3:
                ins_str, del_str, fname = parts
                ins = int(ins_str) if ins_str.isdigit() else 0
                dels = int(del_str) if del_str.isdigit() else 0
                files.append({"filename": fname, "insertions": ins, "deletions": dels})
                insertions_total += ins
                deletions_total += dels
        commit["files"] = files
        commit["files_changed"] = len(files)
        commit["insertions"] = insertions_total
        commit["deletions"] = deletions_total

    return commits


# ── Main ─────────────────────────────────────────────────────────────────────

def process_repo(conn, repo_name: str):
    repo_path = os.path.join(REPO_BASE, repo_name)
    if not os.path.isdir(os.path.join(repo_path, ".git")):
        log.warning(f"[{repo_name}] Not a git repo, skipping")
        return 0

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch latest tracked hash for this repo
    cur.execute(
        "SELECT commit_hash FROM cashier_code_changes WHERE repo_name = %s ORDER BY committed_at DESC LIMIT 1",
        (repo_name,),
    )
    row = cur.fetchone()
    since_hash = row["commit_hash"] if row else None
    is_first_run = since_hash is None

    log.info(f"[{repo_name}] since_hash={since_hash or 'first run'}")

    # Fetch origin to update remote refs
    try:
        subprocess.run(
            ["git", "fetch", "origin", "--quiet"],
            cwd=repo_path, capture_output=True, timeout=30,
        )
    except Exception as e:
        log.warning(f"[{repo_name}] git fetch failed: {e}")

    commits = parse_commits(repo_path, since_hash, limit=50 if is_first_run else 200)
    if not commits:
        log.info(f"[{repo_name}] No new commits")
        return 0

    unpushed = get_unpushed_hashes(repo_path)

    inserted = 0
    for commit in commits:
        diff_summary = generate_diff_summary(commit["message"], commit["files"])
        is_pushed = commit["hash"] not in unpushed
        try:
            cur.execute(
                """
                INSERT INTO cashier_code_changes
                    (repo_name, commit_hash, commit_message, author, committed_at,
                     files_changed, insertions, deletions, files_list, diff_summary, is_pushed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (repo_name, commit_hash) DO NOTHING
                """,
                (
                    repo_name,
                    commit["hash"],
                    commit["message"],
                    commit["author"],
                    commit["committed_at"],
                    commit["files_changed"],
                    commit["insertions"],
                    commit["deletions"],
                    json.dumps(commit["files"]),
                    diff_summary,
                    is_pushed,
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            log.error(f"[{repo_name}] Insert failed for {commit['hash'][:8]}: {e}")

    log.info(f"[{repo_name}] Inserted {inserted} new commits")
    return inserted


def main():
    log.info("=== change_tracker start ===")
    try:
        conn = db_connect()
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        sys.exit(1)

    total = 0
    for repo in REPOS:
        try:
            total += process_repo(conn, repo)
        except Exception as e:
            log.error(f"[{repo}] Unhandled error: {e}")

    conn.close()
    log.info(f"=== change_tracker done — {total} total new commits ===")


if __name__ == "__main__":
    main()
