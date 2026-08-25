#!/usr/bin/env python3
"""Odoo <-> internal-tables stock drift watchdog.

WHY THIS EXISTS. On 2026-08-25 four separate instances of the same failure
surfaced in one day, each one hidden until something removed the layer masking
it:

  1. product names       - product_metadata is SKU-prefixed, shop_stock_levels
                           is bare, so the join silently never matched (#357)
  2. archived SKU twins  - stock left on an archived product when a pack size
                           changed and a new product record was created (#4707)
  3. categories          - product_metadata said "Non-Perishable" where Odoo
                           said "Consumables (Qty Tracked)", so space caps were
                           ignored (#359)
  4. units of measure    - product_location_space.space_cap for "Wax paper for
                           grill (Carton of 500)" was entered as 500 SHEETS,
                           but the Odoo unit IS the carton. #359 would have
                           shown 22 shops a JOD 139,530 order

Every one was invisible until a code change exposed it. None would have been
caught by a test, because the code was correct and the DATA was wrong.

This watchdog looks for the two classes that carry money:

  A. stock sitting on ARCHIVED Odoo products at internal (real shelf)
     locations - invisible to the dashboard, which filters archived products
     deliberately and correctly
  B. space_caps that tower over the most any shop has ever held - almost
     always a unit-of-measure entry error

Alerts @Blk_Server_bot only when something is NEW or the picture has grown.
Silent when steady. State file makes an unresolved problem re-alert weekly
rather than daily, matching pass_cert_expiry_alert.sh.

READ-ONLY against Postgres. The connection is opened read-only so this can
never write, whatever anyone edits into a query later.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

# Cron scripts keep an explicit StreamHandler basicConfig: the json-logger
# standard silenced five pipelines for a week when it was assumed.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("monitor/odoo-stock-drift")

TG_ENV = "/srv/qahwablk/cashier-dashboard/.env"
STATE_FILE = os.environ.get(
    "STATE_FILE", "/srv/qahwablk/monitor/odoo-stock-drift.state"
)
REPEAT_SECS = 600_000  # ~6.9d: daily cron re-alerts weekly without drift

# A cap more than this many times the network-wide max on-hand for the product
# is treated as an entry error. Mirrors REORDER_V2_CAP_MAX_RATIO in the
# reorder-v2 engine; keep the two in step.
CAP_MAX_RATIO = float(os.environ.get("CAP_MAX_RATIO", "20"))

# Ignore archived-stock noise below this. Zero-cost products and single stray
# units are not worth a Telegram at 06:00.
MIN_JOD = float(os.environ.get("MIN_JOD", "50"))


def env_value(path: str, key: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip()
    except OSError as exc:
        log.error("cannot read %s: %s", path, exc)
    return ""


def telegram(msg: str) -> bool:
    token = env_value(TG_ENV, "TELEGRAM_BOT_TOKEN")
    chat = env_value(TG_ENV, "TELEGRAM_CHAT_ID")
    if not token or not chat:
        log.error("missing telegram creds in %s", TG_ENV)
        return False
    data = urllib.parse.urlencode({"chat_id": chat, "text": msg}).encode()
    try:
        with urllib.request.urlopen(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=30
        ) as resp:
            return resp.status == 200
    except Exception as exc:  # noqa: BLE001 - alerting must never raise into cron
        log.error("telegram send failed: %s", exc)
        return False


def db_connect():
    cfg = {"dbname": os.getenv("PG_DBNAME", "qahwablk")}
    # Default to peer auth as the invoking OS user (monitor cron runs as mego).
    if os.getenv("PG_USER"):
        cfg["user"] = os.getenv("PG_USER")
    if os.getenv("PG_HOST"):
        cfg["host"] = os.getenv("PG_HOST")
    conn = psycopg2.connect(**cfg)
    conn.set_session(readonly=True)  # belt and braces: this job never writes
    return conn


# ── A. stock stranded on archived Odoo products ──────────────────────────────

ARCHIVED_STOCK_SQL = """
SELECT p.product_id,
       p.name,
       p.uom_name,
       SUM(q.quantity)                              AS qty,
       ROUND(SUM(q.quantity * p.standard_price), 2) AS jod,
       COUNT(*)                                     AS locations
FROM odoo_stock_quants q
JOIN odoo_products p   ON p.product_id = q.product_id
JOIN odoo_locations l  ON l.location_id = q.location_id
WHERE p.active IS NOT TRUE
  -- internal = a real shelf. customer/inventory/transit/production are Odoo
  -- virtual locations; counting them overstates this ~10x (a delivered or
  -- written-off quant is not stranded stock).
  AND l.usage = 'internal'
GROUP BY p.product_id, p.name, p.uom_name
HAVING SUM(q.quantity * p.standard_price) >= %s
ORDER BY jod DESC
"""


# ── B. implausible space caps ────────────────────────────────────────────────

IMPLAUSIBLE_CAP_SQL = """
WITH netmax AS (
    SELECT product_name, MAX(stock_on_hand) AS max_soh
    FROM shop_stock_levels
    GROUP BY product_name
)
SELECT pls.product_name,
       pls.uom_name,
       MAX(pls.space_cap)                                    AS cap,
       MAX(nm.max_soh)                                       AS network_max_on_hand,
       ROUND(MAX(pls.space_cap) / NULLIF(MAX(nm.max_soh), 0), 1) AS ratio,
       COUNT(*)                                              AS locations
FROM product_location_space pls
JOIN netmax nm ON nm.product_name = pls.product_name
WHERE pls.space_cap IS NOT NULL
  AND nm.max_soh > 0
  AND pls.space_cap > %s * nm.max_soh
GROUP BY pls.product_name, pls.uom_name
ORDER BY ratio DESC
"""


def load_state() -> dict:
    try:
        with open(STATE_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump(state, fh)
    except OSError as exc:
        log.error("cannot write %s: %s", STATE_FILE, exc)


def main() -> int:
    try:
        conn = db_connect()
    except Exception as exc:  # noqa: BLE001
        log.error("db connect failed: %s", exc)
        return 1

    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(ARCHIVED_STOCK_SQL, (MIN_JOD,))
        archived = cur.fetchall()
        cur.execute(IMPLAUSIBLE_CAP_SQL, (CAP_MAX_RATIO,))
        caps = cur.fetchall()

    archived_jod = sum(float(r["jod"] or 0) for r in archived)
    arch_keys = {str(r["product_id"]) for r in archived}
    cap_keys = {r["product_name"] for r in caps}

    log.info(
        "archived-stock: %d products / JOD %.2f | implausible caps: %d",
        len(archived), archived_jod, len(caps),
    )

    if not archived and not caps:
        # Clear the state so the next occurrence alerts immediately rather than
        # waiting out a stale repeat window.
        if load_state():
            save_state({})
        log.info("clean - nothing to report")
        return 0

    state = load_state()
    prev_arch = set(state.get("archived", []))
    prev_caps = set(state.get("caps", []))
    last_sent = float(state.get("last_sent", 0))

    new_arch = arch_keys - prev_arch
    new_caps = cap_keys - prev_caps
    due_repeat = (time.time() - last_sent) > REPEAT_SECS

    if not (new_arch or new_caps or due_repeat):
        log.info("no change since last alert and repeat window not reached")
        return 0

    lines = ["⚠️ Odoo stock-drift watchdog"]

    if archived:
        lines.append(
            f"\nA. Stock on ARCHIVED products (real shelves): "
            f"{len(archived)} products, JOD {archived_jod:,.2f}"
        )
        for r in archived[:8]:
            mark = " *NEW*" if str(r["product_id"]) in new_arch else ""
            lines.append(
                f"  {r['name'][:44]} - {r['qty']:g} {r['uom_name'] or ''} "
                f"= JOD {float(r['jod']):,.2f} ({r['locations']} loc){mark}"
            )
        if len(archived) > 8:
            lines.append(f"  ... and {len(archived) - 8} more")
        lines.append("  Fix: move the stock to the active product (CONVERT UNITS)")
        lines.append("  or write it off. Do NOT un-archive.")

    if caps:
        lines.append(
            f"\nB. Implausible space_caps (> {CAP_MAX_RATIO:g}x network max "
            f"on-hand): {len(caps)}"
        )
        for r in caps[:8]:
            mark = " *NEW*" if r["product_name"] in new_caps else ""
            lines.append(
                f"  {r['product_name'][:44]} - cap {r['cap']:g} "
                f"{r['uom_name'] or ''} vs max held {r['network_max_on_hand']:g} "
                f"({r['ratio']:g}x, {r['locations']} loc){mark}"
            )
        if len(caps) > 8:
            lines.append(f"  ... and {len(caps) - 8} more")
        lines.append(
            "  Usually a unit mix-up (a per-pack count entered as the pack "
            "count). Reorder v2 withholds suggestions for these."
        )

    msg = "\n".join(lines)
    if telegram(msg):
        save_state({
            "archived": sorted(arch_keys),
            "caps": sorted(cap_keys),
            "last_sent": time.time(),
        })
        log.info("alert sent (%d new archived, %d new caps)", len(new_arch), len(new_caps))
    else:
        log.error("alert NOT sent; state left unchanged so the next run retries")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
