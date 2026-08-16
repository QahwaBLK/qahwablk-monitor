#!/usr/bin/env python3
"""
pipeline_watch.py — the scheduled reader for ``pipeline_run_log``.

WHY THIS EXISTS
---------------
37 pipelines write run records to ``pipeline_run_log``. Until this script,
exactly one scheduled process read that table: ``zenhr-pipeline/zenhr_monitor.py``,
and it filters ``WHERE pipeline_name LIKE 'zenhr-pipeline/%'``. The other 30
names wrote into a table nothing opened.

``monitor/system_audit.py`` was the intended reader. It has never been
scheduled — not in any crontab, not in /etc/cron.d, not in any systemd unit.
Its only invoker is an on-demand API route. Its output table
``cashier_audit_log`` was last written 2026-04-28; its ``pipeline_health`` rows
last written 2026-03-18. See audits/2026-08-16-detection-estate-findings.md in
qahwablk-knowledge.

This script is deliberately NOT an extension of system_audit.py. Scheduling
that module would also switch on five unrelated audit domains (security scans,
ruff across eight repos, process health, postgres health), several of which
point at repos that no longer exist on disk. It also has no alert path at all.
Reading one table did not justify turning all of that on.

WHAT IT CHECKS
--------------
  freshness  — the newest run for a pipeline is older than its window
  status     — the newest run ended in a failure state
  stuck      — a run is still 'running' well past its stuck threshold
  silent     — a registered pipeline has no run recorded at all
  drift      — a name is writing rows but is not registered here

WHAT IT DOES NOT CHECK — read this before trusting it
-----------------------------------------------------
It does NOT assert breadth or plausibility. It cannot. ``details`` is null for
32 of the 37 active pipelines, so there is no per-shop / per-date / per-step
dimension in the table to assert against.

It also does NOT alert on ``rows_affected``. That column is populated by 36 of
37 pipelines, so it is tempting, but a flat "zero rows is bad" rule is provably
wrong here: zero is the NORMAL steady state for four pipelines
(zenhr-pipeline/work-locations 100% of runs, zenhr-pipeline/work-shifts 100%,
zenhr-pipeline/attendance 99.6%, shareeb/sync-visits 78.5%). Row counts are
printed for context and never alerted on.

The practical consequence, stated plainly because a monitor that flatters
itself is worse than none: a job that runs on time, exits 0, and writes nothing
will NOT be caught by this script. ``pipeline/finance_anomalies.py`` has been in
exactly that state for 94 days and ``pipeline/sync_received.py`` for 43 days.
Neither writes to pipeline_run_log at all, so both are invisible here twice
over. Closing that needs a per-pipeline output assertion, which needs ``details``
to be populated first. That is not this script.

ALERT CADENCE
-------------
Deliberately NOT alert-once-then-suppress. ``zenhr_monitor.py`` uses that model
("Problem set unchanged since last alert — not re-sending") and the result is
that a problem which persists goes quiet after its first night. The opposite
failure is also documented in this estate: cron-staleness-alert.sh re-sent an
identical talabat_sync line every day for ~8 weeks and it was ignored.

So: every finding re-fires, but the text always carries how long it has been
unresolved, and it re-fires harder as it ages. A finding is re-sent when it is
new, when its age crosses an escalation boundary, or when MAX_QUIET_H has passed
since it was last sent. Nothing is ever permanently suppressed.

USAGE
-----
    python3 pipeline_watch.py                 # DRY RUN (default) — prints, sends nothing
    python3 pipeline_watch.py --send          # actually send to Telegram
    python3 pipeline_watch.py --explain       # dry run + per-pipeline table of every check
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import dotenv_values

# Same env file and Telegram identity as health_check.py, its sibling in this
# repo and the closest working analogue. Overridable for testing.
ENV_FILE = os.environ.get("PIPELINE_WATCH_ENV", "/srv/qahwablk/cashier-dashboard/.env")
STATE_FILE = os.environ.get("PIPELINE_WATCH_STATE", "/srv/qahwablk/monitor/pipeline-watch-state.json")
LOG_FILE = os.environ.get("PIPELINE_WATCH_LOG", "/var/log/qahwablk/pipeline-watch.log")

# Re-fire policy. See ALERT CADENCE above.
ESCALATION_BOUNDS_H = (1, 6, 24, 72, 168)
MAX_QUIET_H = 24

# A run still 'running' this long after it started is treated as stuck unless
# the pipeline overrides it. 2h matches the rule system_audit.py used.
DEFAULT_STUCK_MIN = 120

# StreamHandler is not optional: a cron script that logs only to a file goes
# silent in the mail/redirect trail. The FileHandler is best-effort — a dry run
# by a user who cannot write /var/log must still work rather than die at import.
_handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
try:
    _handlers.insert(0, logging.FileHandler(LOG_FILE))
except OSError:
    print(f"note: cannot write {LOG_FILE}; logging to stdout only", file=sys.stderr)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    handlers=_handlers,
)
log = logging.getLogger("pipeline-watch")


# ── Status vocabulary ────────────────────────────────────────────────────────
#
# Three mechanisms write this table and they do not agree on the words:
#   rhythmos.pipeline_logger        -> 'success' | 'running' | 'error'
#   cashier scoring pipeline_logging-> 'success' | 'running' | 'error'
#   hand-rolled SQL (sync_gl,
#     scorecard_compute)            -> 'success' | 'running' | 'failure'
#   sync_inventory, post-hoc UPDATE -> 'partial_failure'
#
# Normalising here rather than asking 37 pipelines to change means this script
# does not touch any pipeline's behaviour. An UNKNOWN status is treated as a
# failure on purpose: a word we have not seen before is not something to
# silently pass.

STATUS_OK = "OK"
STATUS_RUNNING = "RUNNING"
STATUS_FAILED = "FAILED"
STATUS_DEGRADED = "DEGRADED"

_STATUS_MAP = {
    "success": STATUS_OK,
    "running": STATUS_RUNNING,
    "error": STATUS_FAILED,
    "failure": STATUS_FAILED,
    "partial_failure": STATUS_DEGRADED,
}


def normalise_status(raw: str | None) -> str:
    if raw is None:
        return STATUS_FAILED
    return _STATUS_MAP.get(raw.strip().lower(), STATUS_FAILED)


def namespace_of(name: str) -> str:
    """Group key for display only. Never rewrites what a pipeline writes.

    27 of 37 names use 'namespace/name'; 10 use bare snake_case. Rather than
    renaming pipelines (which would change their behaviour and orphan their
    history), the bare ones are grouped under a single explicit bucket so the
    inconsistency stays visible instead of being papered over.
    """
    return name.split("/", 1)[0] if "/" in name else "(unnamespaced)"


# ── Expectations ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Expect:
    """How often a pipeline should appear, and how long a run may legitimately take.

    window_min is derived from the DECLARED schedule (crontab / systemd timer),
    not learned from observed history. Learning it from history would bake any
    current outage into the baseline — the KROM pipelines, for instance, show a
    p95 gap of 3.5 days purely because they were deliberately paused in July.
    Observed p95/max over 14 days to 2026-08-16 is recorded in `note` as a
    cross-check, and every window here clears its observed max.
    """

    window_min: int
    stuck_min: int = DEFAULT_STUCK_MIN
    note: str = ""


# 31 watched pipelines. Windows set from declared cadence, cross-checked against
# observed max gap over the 14 days to 2026-08-16 (shown as obs=).
PIPELINES: dict[str, Expect] = {
    # ── odoo-pipeline ────────────────────────────────────────────────────────
    "odoo-pipeline/sync-pos":            Expect(180, note="2h chain + */15 narrow; obs max 90m"),
    "odoo-pipeline/sync-inventory":      Expect(480, note="*/15 2-20h, legit ~6h overnight gap; obs max 360m"),
    "odoo-pipeline/fast-lane-sales":     Expect(120, note="1-56/5; obs max 80m"),
    "odoo-pipeline/sync-reference":      Expect(2160, note="daily 01:30; obs max 24h"),
    "odoo-pipeline/daily-shop-metrics":  Expect(2160, note="daily 04:00 timer; instrumented 2026-08-16, no run yet"),
    "reconcile_open_records":            Expect(1080, note="0 4,10,16 = 6h; obs max 720m"),
    # ── calculators (loyalty chain) ──────────────────────────────────────────
    "calculators/calc-consumption":      Expect(480, note="~15m chain; obs max 341m"),
    "calculators/calc-inventory":        Expect(480, note="~15m chain; obs max 358m"),
    "calculators/calc-scrap":            Expect(480, note="~15m chain; obs max 315m"),
    "calculators/calc-sales":            Expect(360, note="2h chain; obs max 240m"),
    "calculators/calc-session-closing":  Expect(360, note="2h chain; obs max 240m"),
    "calculators/calc-shop-health":      Expect(300, note="~2h; obs max 127m"),
    # ── operate ──────────────────────────────────────────────────────────────
    "operate-pipeline/sync-tasks":       Expect(480, note="*/10 1-19h, legit ~6h overnight gap; obs max 310m"),
    # ── zenhr (zenhr_monitor also covers failures for these; freshness is new) ─
    "zenhr-pipeline/attendance":         Expect(480, note="*/30 5-20h + nightly passes; obs max 330m"),
    "zenhr-pipeline/employees":          Expect(2160, note="daily 02:05"),
    "zenhr-pipeline/timeoff":            Expect(2160, note="daily 02:10"),
    "zenhr-pipeline/work-shifts":        Expect(2160, note="daily 02:01"),
    "zenhr-pipeline/work-shifts-dict":   Expect(2160, note="daily 02:00"),
    "zenhr-pipeline/work-locations":     Expect(2160, note="daily 02:15"),
    "zenhr-pipeline/shop-assignment":    Expect(2160, note="daily 02:15 (mapping leg)"),
    # ── cashier-dashboard ────────────────────────────────────────────────────
    "sync_gl":                           Expect(2160, note="daily 03:45"),
    "scorecard_compute":                 Expect(2160, note="daily 04:15"),
    "cashier_user_activity_retention":   Expect(2160, note="daily 00:00"),
    "complaints/sync":                   Expect(2160, note="daily 05:30"),
    "sync_deposit_image_times":          Expect(2160, note="daily 07:10"),
    "krom_calculate_individual":         Expect(2160, note="daily 05:35"),
    "krom_shop_score":                   Expect(2160, note="daily 05:45"),
    "krom_area_leader_score":            Expect(2160, note="daily 05:55"),
    "krom_schedule_compliance":          Expect(2160, note="daily 06:00"),
    "krom_punch_open_rate":              Expect(2160, note="daily 06:15"),
    # ── shareeb ──────────────────────────────────────────────────────────────
    # 180 not 120: there is a RECURRING ~123m quiet window every night around
    # 00:12-02:15 (5 of 7 nights in the week to 2026-08-16), which a 120m window
    # clips and would fire on nightly. 180 still catches the real 9h outage of
    # 2026-08-13 22:12 -> 08-14 07:12.
    "shareeb/sync-visits":               Expect(180, note="*/3 flock; nightly ~123m quiet window; real 540m outage 2026-08-13"),
    "shareeb/offer-sweep":               Expect(2160, note="daily 00:20"),
}

# Names that appear in pipeline_run_log history but are NOT watched, with the
# reason. Listed so they show up as a deliberate decision rather than an
# oversight — this is where sales-intelligence/fast-lane would otherwise hide.
RETIRED: dict[str, str] = {
    # The three *-intelligence repos are gone from disk and have zero cron
    # lines. Verified 2026-08-16.
    "cashier-intelligence/main":             "repo removed from disk; last run 2026-03-18",
    "cashier-intelligence/grubtech-recheck": "repo removed from disk; last run 2026-03-18",
    "cashier-intelligence/pending-pickings": "repo removed from disk; last run 2026-03-18",
    "cashier-intelligence/stock-levels":     "repo removed from disk; last run 2026-03-18",
    "inventory-intelligence/main":           "repo removed from disk; last run 2026-03-13",
    "sales-intelligence/main":               "repo removed from disk; last run 2026-03-17",
    "sales-intelligence/ceo-dashboard":      "repo removed from disk; last run 2026-03-18",
    # Transient name from the fast-lane build; 6 runs, all on one day, before
    # odoo-pipeline/fast-lane-sales became canonical.
    "sales-intelligence/fast-lane":          "dev-only name, 6 runs on 2026-08-11; superseded by odoo-pipeline/fast-lane-sales",
    # Pulse sunset. monitor#4 already dropped pulse from health_check and audit;
    # odoo-pipeline dropped the calc_pulse_scores tail call. No chain, no timer.
    "pulse/cups-served":                     "pulse sunset; last run 2026-08-10",
    "pulse/daily-points":                    "pulse sunset; last run 2026-08-10",
    "pulse/health-calculator":               "pulse sunset; last run 2026-08-10",
    "pulse/pos-attribution":                 "pulse sunset; last run 2026-08-10",
    "pulse/score-calculator":                "pulse sunset; last run 2026-08-10",
}


# ── Findings ─────────────────────────────────────────────────────────────────


@dataclass
class Finding:
    key: str          # stable identity for dedup across runs
    kind: str         # STALE | FAILED | DEGRADED | STUCK | SILENT | DRIFT
    pipeline: str
    text: str
    alerting: bool = True   # DRIFT is reported but does not page
    extra: dict = field(default_factory=dict)


# ── DB ───────────────────────────────────────────────────────────────────────


def db_connect(env: dict):
    cfg = {"dbname": env.get("PG_DBNAME", "qahwablk"), "user": env.get("PG_USER", "qahwablk")}
    if env.get("PG_HOST"):
        cfg["host"] = env["PG_HOST"]
    conn = psycopg2.connect(**cfg)
    conn.set_session(readonly=True, autocommit=True)
    return conn


def latest_run(cur, name: str) -> dict | None:
    """One indexed lookup on idx_prl_pipeline_name.

    Deliberately not a single DISTINCT ON over the whole table: that plans as a
    seq scan plus an external merge sort (142ms, 6.7MB temp on 102k rows),
    whereas this is 0.166ms per name. 32 lookups beats one full scan.
    """
    cur.execute(
        """SELECT pipeline_name, started_at, finished_at, status,
                  rows_affected, duration_seconds,
                  LEFT(COALESCE(error_message, ''), 200) AS err
           FROM pipeline_run_log
           WHERE pipeline_name = %s
           ORDER BY started_at DESC
           LIMIT 1""",
        (name,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def active_names(cur, days: int = 30) -> set[str]:
    """Names that wrote at least one row recently. Index scan on started_at."""
    cur.execute(
        """SELECT DISTINCT pipeline_name
           FROM pipeline_run_log
           WHERE started_at > now() - make_interval(days => %s)""",
        (days,),
    )
    return {r["pipeline_name"] for r in cur.fetchall()}


# ── Checks ───────────────────────────────────────────────────────────────────


def humanise(minutes: float) -> str:
    if minutes < 90:
        return f"{minutes:.0f}m"
    if minutes < 60 * 48:
        return f"{minutes / 60:.1f}h"
    return f"{minutes / 1440:.1f}d"


def evaluate(cur, now: datetime) -> tuple[list[Finding], list[dict]]:
    findings: list[Finding] = []
    table: list[dict] = []

    for name, exp in sorted(PIPELINES.items()):
        run = latest_run(cur, name)

        if run is None:
            findings.append(Finding(
                key=f"SILENT::{name}", kind="SILENT", pipeline=name,
                text=f"{name}: no run has ever been recorded (registered, expected every {humanise(exp.window_min)})",
            ))
            table.append({"pipeline": name, "verdict": "SILENT", "age": "-", "status": "-",
                          "rows": "-", "window": humanise(exp.window_min)})
            continue

        started = run["started_at"]
        age_min = (now - started).total_seconds() / 60.0
        status = normalise_status(run["status"])
        rows = run["rows_affected"]
        verdicts: list[str] = []

        # stuck — a run that never closed. Checked before freshness because a
        # stuck run keeps started_at fresh and would otherwise mask the problem.
        if run["finished_at"] is None and status == STATUS_RUNNING and age_min > exp.stuck_min:
            verdicts.append("STUCK")
            findings.append(Finding(
                key=f"STUCK::{name}", kind="STUCK", pipeline=name,
                text=f"{name}: run started {humanise(age_min)} ago is still 'running' "
                     f"(stuck threshold {humanise(exp.stuck_min)})",
            ))

        # status of the newest finished run
        if status in (STATUS_FAILED, STATUS_DEGRADED):
            verdicts.append(status)
            err = (run["err"] or "").replace("\n", " ").strip()
            suffix = f" — {err[:160]}" if err else ""
            findings.append(Finding(
                key=f"{status}::{name}", kind=status, pipeline=name,
                text=f"{name}: last run ended {status} ({run['status']}), {humanise(age_min)} ago{suffix}",
            ))

        # freshness
        if age_min > exp.window_min:
            verdicts.append("STALE")
            findings.append(Finding(
                key=f"STALE::{name}", kind="STALE", pipeline=name,
                text=f"{name}: last run {humanise(age_min)} ago, window is {humanise(exp.window_min)} "
                     f"({exp.note})" if exp.note else
                     f"{name}: last run {humanise(age_min)} ago, window is {humanise(exp.window_min)}",
            ))

        table.append({
            "pipeline": name,
            "verdict": "+".join(verdicts) if verdicts else "ok",
            "age": humanise(age_min),
            "status": f"{status}({run['status']})" if status != STATUS_OK else STATUS_OK,
            "rows": "null" if rows is None else str(rows),
            "window": humanise(exp.window_min),
        })

    # drift — writing rows but unregistered and not retired
    seen = active_names(cur)
    unknown = sorted(seen - set(PIPELINES) - set(RETIRED))
    for name in unknown:
        findings.append(Finding(
            key=f"DRIFT::{name}", kind="DRIFT", pipeline=name, alerting=False,
            text=f"{name}: writing to pipeline_run_log but not registered in pipeline_watch.py "
                 f"(add it to PIPELINES or RETIRED)",
        ))

    return findings, table


# ── Re-fire state ────────────────────────────────────────────────────────────


def load_state() -> dict:
    try:
        return json.loads(Path(STATE_FILE).read_text())
    except (OSError, ValueError):
        return {}


def save_state(state: dict) -> None:
    try:
        Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(STATE_FILE).write_text(json.dumps(state, indent=2, sort_keys=True))
    except OSError as exc:
        log.error("could not write state file %s: %s", STATE_FILE, exc)


def _bound_index(age_h: float) -> int:
    """How many escalation boundaries this age has crossed."""
    return sum(1 for b in ESCALATION_BOUNDS_H if age_h >= b)


def decide_sends(findings: list[Finding], state: dict, now: datetime) -> tuple[list[Finding], dict, list[str]]:
    """Return (findings to send, new state, recovered keys).

    Never permanently suppresses. A finding is sent when it is new, when its age
    crosses a new escalation boundary, or when MAX_QUIET_H has elapsed since it
    was last sent.
    """
    new_state: dict = {}
    to_send: list[Finding] = []
    now_iso = now.isoformat()

    for f in findings:
        prev = state.get(f.key)
        if prev is None:
            first_seen = now_iso
            last_sent = None
            prev_bound = -1
        else:
            first_seen = prev.get("first_seen", now_iso)
            last_sent = prev.get("last_sent")
            prev_bound = prev.get("bound", -1)

        age_h = (now - datetime.fromisoformat(first_seen)).total_seconds() / 3600.0
        bound = _bound_index(age_h)

        send = False
        if prev is None:
            send = True
        elif bound > prev_bound:
            send = True
        elif last_sent is None:
            send = True
        else:
            quiet_h = (now - datetime.fromisoformat(last_sent)).total_seconds() / 3600.0
            if quiet_h >= MAX_QUIET_H:
                send = True

        f.extra["age_h"] = age_h
        f.extra["first_seen"] = first_seen
        if send and f.alerting:
            to_send.append(f)

        new_state[f.key] = {
            "first_seen": first_seen,
            "last_sent": now_iso if (send and f.alerting) else last_sent,
            "bound": bound,
            "kind": f.kind,
            "pipeline": f.pipeline,
        }

    recovered = sorted(set(state) - {f.key for f in findings})
    return to_send, new_state, recovered


# ── Output ───────────────────────────────────────────────────────────────────

NOT_CHECKED = (
    "NOT CHECKED by this monitor:\n"
    "  - breadth / plausibility. details is null for 32 of 37 pipelines, so there\n"
    "    is nothing to assert per shop, per date or per step.\n"
    "  - rows_affected. Zero is the NORMAL state for 4 pipelines, so a flat\n"
    "    zero-rows rule would be wrong. Counts are shown, never alerted on.\n"
    "  - a job that runs on time, exits 0 and writes nothing WILL NOT be caught.\n"
    "    finance_anomalies (94d) and sync_received (43d) are in that state now and\n"
    "    write no run rows at all, so they are invisible here."
)


def format_message(to_send: list[Finding], all_findings: list[Finding], recovered: list[str]) -> str:
    lines = [f"[PIPELINE-WATCH] {len(to_send)} finding(s) to report, {len(all_findings)} open"]
    by_kind: dict[str, list[Finding]] = {}
    for f in to_send:
        by_kind.setdefault(f.kind, []).append(f)
    for kind in ("STUCK", "FAILED", "DEGRADED", "STALE", "SILENT"):
        for f in by_kind.get(kind, []):
            age = f.extra.get("age_h", 0.0)
            age_txt = "new" if age < 1 else f"unresolved {humanise(age * 60)}"
            lines.append(f"  [{kind}] {f.text}  ({age_txt})")
    if recovered:
        lines.append("recovered since last run:")
        lines.extend(f"  + {k}" for k in recovered)
    lines.append("")
    lines.append(NOT_CHECKED)
    return "\n".join(lines)


def send_telegram(env: dict, message: str) -> bool:
    token = env.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = env.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.error("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing — alert not sent")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": message[:4000]},
            timeout=15,
        )
        r.raise_for_status()
        return True
    except requests.RequestException as exc:
        # Never let the transport carry credentials into a log line.
        log.error("Telegram send failed: %s", type(exc).__name__)
        return False


def print_table(table: list[dict]) -> None:
    hdr = f"{'pipeline':<36} {'verdict':<18} {'age':>8} {'window':>8} {'rows':>8}  status"
    print(hdr)
    print("-" * len(hdr))
    for r in table:
        print(f"{r['pipeline']:<36} {r['verdict']:<18} {r['age']:>8} {r['window']:>8} "
              f"{r['rows']:>8}  {r['status']}")


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(description="Scheduled reader for pipeline_run_log")
    ap.add_argument("--send", action="store_true",
                    help="actually send to Telegram. Default is dry run.")
    ap.add_argument("--explain", action="store_true",
                    help="print the full per-pipeline table, including passing ones")
    args = ap.parse_args()

    env = dotenv_values(ENV_FILE)
    now = datetime.now(timezone.utc)

    conn = db_connect(env)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        findings, table = evaluate(cur, now)
    finally:
        conn.close()

    state = load_state()
    to_send, new_state, recovered = decide_sends(findings, state, now)

    alerting = [f for f in findings if f.alerting]
    drift = [f for f in findings if not f.alerting]

    if args.explain:
        print_table(table)
        print()
        print(f"deliberately NOT watched ({len(RETIRED)} names seen in history, "
              f"each a recorded decision rather than an oversight):")
        for name, why in sorted(RETIRED.items()):
            print(f"  - {name:<40} {why}")
        print()

    print(f"open findings: {len(alerting)} alerting, {len(drift)} drift-only")
    for f in sorted(alerting, key=lambda x: (x.kind, x.pipeline)):
        print(f"  [{f.kind}] {f.text}")
    for f in drift:
        print(f"  [DRIFT] {f.text}")
    if recovered:
        print("recovered since last run:")
        for k in recovered:
            print(f"  + {k}")
    print()
    print(f"would send: {len(to_send)} finding(s) this run "
          f"(others are open but inside their re-fire window)")
    print()
    print(NOT_CHECKED)

    if not args.send:
        print()
        print("DRY RUN — nothing sent, state file not written. Use --send to arm.")
        return 0

    if to_send or recovered:
        ok = send_telegram(env, format_message(to_send, alerting, recovered))
        log.info("alert sent=%s findings=%d sent=%d", ok, len(alerting), len(to_send))
    else:
        log.info("nothing to send; %d finding(s) open and inside re-fire window", len(alerting))

    save_state(new_state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
