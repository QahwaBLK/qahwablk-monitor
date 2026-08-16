#!/usr/bin/env python3
"""Tests for pipeline_watch.

Covers the parts that cannot be observed by running against the live table:
the stuck check (no run is currently stuck), the re-fire policy (needs time to
pass), and drift detection (needs an unregistered name).

Run:  python3 tests/test_pipeline_watch.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pipeline_watch as pw  # noqa: E402

NOW = datetime(2026, 8, 17, 3, 0, tzinfo=timezone.utc)
FAILS: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'OK  ' if ok else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}")
    if not ok:
        FAILS.append(label)


class FakeCursor:
    """Stands in for a DictCursor over pipeline_run_log."""

    def __init__(self, rows: dict[str, dict], active: set[str]):
        self._rows, self._active, self._result = rows, active, None

    def execute(self, sql: str, params=None):
        if "DISTINCT pipeline_name" in sql:
            self._result = [{"pipeline_name": n} for n in self._active]
        else:
            row = self._rows.get(params[0])
            self._result = [row] if row else []

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result


def row(started_min_ago: float, status: str, finished: bool = True, rows_affected=0):
    started = NOW - timedelta(minutes=started_min_ago)
    return {
        "pipeline_name": "x", "started_at": started,
        "finished_at": started + timedelta(seconds=5) if finished else None,
        "status": status, "rows_affected": rows_affected,
        "duration_seconds": 5, "err": "",
    }


def test_status_normalisation():
    print("status normalisation — the three vocabularies")
    check("success -> OK", pw.normalise_status("success") == pw.STATUS_OK)
    check("error -> FAILED (rhythmos + cashier scoring)", pw.normalise_status("error") == pw.STATUS_FAILED)
    check("failure -> FAILED (sync_gl, scorecard_compute)", pw.normalise_status("failure") == pw.STATUS_FAILED)
    check("partial_failure -> DEGRADED (sync_inventory)", pw.normalise_status("partial_failure") == pw.STATUS_DEGRADED)
    check("running -> RUNNING", pw.normalise_status("running") == pw.STATUS_RUNNING)
    check("unknown word -> FAILED, not silently passed", pw.normalise_status("weird") == pw.STATUS_FAILED)
    check("None -> FAILED", pw.normalise_status(None) == pw.STATUS_FAILED)


def test_namespace():
    print("name normalisation — 27 slash vs 10 bare")
    check("slash form", pw.namespace_of("odoo-pipeline/sync-pos") == "odoo-pipeline")
    check("bare form bucketed, not renamed", pw.namespace_of("sync_gl") == "(unnamespaced)")


def test_zero_rows_never_alerts():
    print("zero rows_affected must NOT alert (4 pipelines are normally zero)")
    name = "zenhr-pipeline/work-shifts"
    rows = {name: row(60, "success", rows_affected=0)}
    findings, _ = pw.evaluate(FakeCursor(rows, {name}), NOW)
    hits = [f for f in findings if f.pipeline == name]
    check("no finding for a fresh successful zero-row run", not hits,
          f"got {[f.kind for f in hits]}")


def test_stuck():
    print("stuck detection")
    name = "odoo-pipeline/sync-pos"
    exp = pw.PIPELINES[name]
    rows = {name: row(exp.stuck_min + 30, "running", finished=False, rows_affected=None)}
    findings, _ = pw.evaluate(FakeCursor(rows, {name}), NOW)
    kinds = {f.kind for f in findings if f.pipeline == name}
    check("run past stuck threshold with no finished_at -> STUCK", "STUCK" in kinds, str(kinds))

    rows = {name: row(10, "running", finished=False, rows_affected=None)}
    findings, _ = pw.evaluate(FakeCursor(rows, {name}), NOW)
    kinds = {f.kind for f in findings if f.pipeline == name}
    check("a young running job is not stuck", "STUCK" not in kinds, str(kinds))


def test_freshness_and_status():
    print("freshness and failure status")
    name = "sync_gl"
    exp = pw.PIPELINES[name]
    rows = {name: row(exp.window_min + 120, "success")}
    findings, _ = pw.evaluate(FakeCursor(rows, {name}), NOW)
    check("older than window -> STALE",
          "STALE" in {f.kind for f in findings if f.pipeline == name})

    rows = {name: row(10, "failure")}
    findings, _ = pw.evaluate(FakeCursor(rows, {name}), NOW)
    check("status 'failure' -> FAILED", "FAILED" in {f.kind for f in findings if f.pipeline == name})

    rows = {"odoo-pipeline/sync-inventory": row(10, "partial_failure")}
    findings, _ = pw.evaluate(FakeCursor(rows, {"odoo-pipeline/sync-inventory"}), NOW)
    check("status 'partial_failure' -> DEGRADED",
          "DEGRADED" in {f.kind for f in findings if f.pipeline == "odoo-pipeline/sync-inventory"})


def test_silent_and_drift():
    print("silent registered pipeline, and drift of unregistered names")
    findings, _ = pw.evaluate(FakeCursor({}, set()), NOW)
    silent = [f for f in findings if f.kind == "SILENT"]
    check("every registered pipeline with no row -> SILENT", len(silent) == len(pw.PIPELINES),
          f"{len(silent)} of {len(pw.PIPELINES)}")

    findings, _ = pw.evaluate(FakeCursor({}, {"brand-new/thing"}), NOW)
    drift = [f for f in findings if f.kind == "DRIFT"]
    check("unregistered writer -> DRIFT", len(drift) == 1 and drift[0].pipeline == "brand-new/thing")
    check("DRIFT does not page", drift and drift[0].alerting is False)

    findings, _ = pw.evaluate(FakeCursor({}, {"pulse/cups-served"}), NOW)
    check("a RETIRED name does not raise DRIFT",
          not [f for f in findings if f.kind == "DRIFT"])


def test_refire_never_suppresses_forever():
    """The failure mode this design exists to avoid.

    zenhr_monitor.py sends only when the problem SET changes, so a problem that
    persists goes quiet after night one. Here an unchanged problem must still
    re-fire, at least every MAX_QUIET_H.
    """
    print("re-fire policy — a persistent problem must never go permanently quiet")
    f = pw.Finding(key="STALE::x", kind="STALE", pipeline="x", text="x is stale")

    to_send, state, _ = pw.decide_sends([f], {}, NOW)
    check("brand new finding sends immediately", len(to_send) == 1)

    t = NOW + timedelta(minutes=10)
    f2 = pw.Finding(key="STALE::x", kind="STALE", pipeline="x", text="x is stale")
    to_send, state2, _ = pw.decide_sends([f2], state, t)
    check("10 minutes later, same problem, stays quiet (dedup works)", len(to_send) == 0)

    t = NOW + timedelta(hours=1, minutes=1)
    f3 = pw.Finding(key="STALE::x", kind="STALE", pipeline="x", text="x is stale")
    to_send, state3, _ = pw.decide_sends([f3], state2, t)
    check("crossing the 1h escalation boundary re-fires", len(to_send) == 1)

    # Walk forward a week, one run per hour, and assert it never goes quiet for
    # longer than MAX_QUIET_H.
    st, last_send_h, max_gap = state3, 1.0, 0.0
    for hour in range(2, 24 * 7):
        t = NOW + timedelta(hours=hour)
        fx = pw.Finding(key="STALE::x", kind="STALE", pipeline="x", text="x is stale")
        sends, st, _ = pw.decide_sends([fx], st, t)
        if sends:
            max_gap = max(max_gap, hour - last_send_h)
            last_send_h = hour
    max_gap = max(max_gap, (24 * 7 - 1) - last_send_h)
    check(f"over 7 days the longest silence is {max_gap:.0f}h, cap is {pw.MAX_QUIET_H}h",
          max_gap <= pw.MAX_QUIET_H, f"max_gap={max_gap}")

    to_send, st2, recovered = pw.decide_sends([], st, NOW + timedelta(days=8))
    check("clearing the problem reports a recovery", recovered == ["STALE::x"], str(recovered))
    check("state does not leak cleared findings", st2 == {})


def main() -> int:
    for t in (test_status_normalisation, test_namespace, test_zero_rows_never_alerts,
              test_stuck, test_freshness_and_status, test_silent_and_drift,
              test_refire_never_suppresses_forever):
        t()
        print()
    if FAILS:
        print(f"{len(FAILS)} check(s) failed: {FAILS}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
