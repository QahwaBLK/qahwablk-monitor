#!/usr/bin/env python3
"""
Compliance Scorer — scores new commits against cashier_compliance_rules.
Runs every 30 min after change_tracker.py. Checks only added lines in diffs.
"""

import json
import logging
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values

ENV_FILE = "/srv/qahwablk/cashier-dashboard/.env"
LOG_FILE = "/var/log/qahwablk/compliance_scorer.log"
REPO_BASE = "/srv/qahwablk"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def db_connect():
    env = dotenv_values(ENV_FILE)
    cfg = {"dbname": env.get("PG_DBNAME", "qahwablk"), "user": env.get("PG_USER", "qahwablk")}
    if env.get("PG_HOST"):
        cfg["host"] = env["PG_HOST"]
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    return conn


def git_added_lines(repo_path: str, commit_hash: str) -> list[tuple[str, str]]:
    """Return [(filename, added_line), ...] — only lines starting with + in the diff."""
    try:
        result = subprocess.run(
            ["git", "show", "--unified=0", commit_hash],
            cwd=repo_path, capture_output=True, text=True, timeout=30,
        )
        lines = result.stdout.splitlines()
        current_file = ""
        added = []
        for line in lines:
            if line.startswith("+++ b/"):
                current_file = line[6:]
            elif line.startswith("+") and not line.startswith("+++"):
                added.append((current_file, line[1:]))
        return added
    except Exception as e:
        log.warning(f"git show failed for {commit_hash[:8]}: {e}")
        return []


def git_tracked_files(repo_path: str) -> list[str]:
    """List all files tracked by git in the repo."""
    try:
        result = subprocess.run(
            ["git", "ls-files"], cwd=repo_path, capture_output=True, text=True, timeout=10,
        )
        return result.stdout.splitlines()
    except Exception:
        return []


def check_env_in_git(repo_path: str) -> list[dict]:
    """Return findings if any .env file is tracked by git."""
    tracked = git_tracked_files(repo_path)
    findings = []
    for f in tracked:
        if f == ".env" or f.endswith("/.env"):
            findings.append({"file": f, "line_number": None, "snippet": f".env tracked: {f}"})
    return findings


def apply_rule(pattern: str | None, added_lines: list[tuple[str, str]], rule_key: str, repo_name: str) -> list[dict]:
    """Apply a regex pattern to added lines. Returns list of finding dicts."""
    if not pattern:
        return []
    # For direct_xmlrpc — skip odoo_client.py files
    findings = []
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        log.warning(f"Bad regex for rule {rule_key}: {e}")
        return []

    seen_files = set()
    for filename, line in added_lines:
        # direct_xmlrpc: skip odoo_client.py
        if rule_key == "direct_xmlrpc" and "odoo_client" in filename:
            continue
        # print_debugging: skip pipeline scripts (they legitimately print)
        if rule_key == "print_debugging":
            pipeline_dirs = ["pipeline", "pipelines", "cashier-intelligence", "sales-intelligence",
                             "inventory-intelligence", "operate-pipeline", "zenhr-pipeline"]
            if any(d in filename for d in pipeline_dirs):
                continue
        if rx.search(line):
            # Deduplicate per file (one finding per file per rule)
            file_key = filename
            if file_key not in seen_files:
                seen_files.add(file_key)
                findings.append({
                    "file": filename,
                    "line_number": None,
                    "snippet": line.strip()[:150],
                })
    return findings


def score_commit(conn, commit_id: int, repo_name: str, commit_hash: str, rules: list[dict]) -> dict:
    """Score a single commit. Returns the score record dict."""
    repo_path = f"{REPO_BASE}/{repo_name}"
    added_lines = git_added_lines(repo_path, commit_hash)

    all_findings = []
    has_fail = False
    has_warn = False
    score = 100

    for rule in rules:
        rule_key = rule["rule_key"]
        severity = rule["severity"]
        pattern = rule.get("pattern")

        if rule_key == "env_in_git":
            rule_findings = check_env_in_git(repo_path)
        else:
            rule_findings = apply_rule(pattern, added_lines, rule_key, repo_name)

        if rule_findings:
            for f in rule_findings:
                all_findings.append({
                    "rule_key": rule_key,
                    "severity": severity,
                    **f,
                })
            if severity == "fail":
                has_fail = True
                score = max(0, score - 25)
            else:
                has_warn = True
                score = max(0, score - 10)

    if has_fail:
        overall_status = "fail"
    elif has_warn:
        overall_status = "warn"
    else:
        overall_status = "pass"

    return {
        "commit_id": commit_id,
        "overall_status": overall_status,
        "score": score,
        "findings": all_findings,
    }


def main():
    log.info("=== compliance_scorer start ===")
    conn = db_connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Load active rules
    cur.execute("SELECT rule_key, category, severity, description, pattern FROM cashier_compliance_rules WHERE is_active = true")
    rules = [dict(r) for r in cur.fetchall()]
    log.info(f"Loaded {len(rules)} active rules")

    # Fetch unscored commits made ON OR AFTER 2026-03-13 (engineering standards cutoff).
    # Commits before this date are known technical debt and are not scored.
    CUTOFF_DATE = "2026-03-13"
    cur.execute("""
        SELECT cc.id, cc.repo_name, cc.commit_hash, cc.commit_message
        FROM cashier_code_changes cc
        LEFT JOIN cashier_compliance_scores cs ON cs.commit_id = cc.id
        WHERE cs.id IS NULL
          AND cc.committed_at >= %s
        ORDER BY cc.committed_at DESC
        LIMIT 100
    """, (CUTOFF_DATE,))
    unscored = cur.fetchall()
    log.info(f"Found {len(unscored)} unscored commits")

    scored = 0
    for row in unscored:
        try:
            result = score_commit(conn, row["id"], row["repo_name"], row["commit_hash"], rules)
            cur.execute("""
                INSERT INTO cashier_compliance_scores (commit_id, overall_status, score, findings)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (commit_id) DO NOTHING
            """, (result["commit_id"], result["overall_status"], result["score"], json.dumps(result["findings"])))
            status_icon = {"pass": "✓", "warn": "⚠", "fail": "✗"}.get(result["overall_status"], "?")
            log.info(f"  [{row['repo_name']}] {row['commit_hash'][:8]} {status_icon} score={result['score']} findings={len(result['findings'])}")
            scored += 1
        except Exception as e:
            log.error(f"  Error scoring {row['commit_hash'][:8]}: {e}")

    conn.close()
    log.info(f"=== compliance_scorer done — {scored}/{len(unscored)} commits scored ===")


if __name__ == "__main__":
    main()
