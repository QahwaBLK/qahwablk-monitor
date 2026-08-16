#!/usr/bin/env python3
"""Shareeb WhatsApp quality watchdog.

WHY THIS EXISTS. The celebration templates (sq_app_*) went live 2026-08-16 and are
MARKETING category — not by choice: all four were submitted as UTILITY and Meta's
classifier moved them to MARKETING, along with 13 others on this WABA (see
order-pickup#98). Mego's decision 2026-08-16 was to stay MARKETING and monitor.

Marketing templates are scored on user feedback. A drop to a low quality rating gets a
template PAUSED, and per order-pickup#83 shareeb WhatsApp send failures are swallowed —
so a pause would be completely invisible. This watchdog is the compensating control.

Checks daily and alerts @Blk_Server_bot ONLY on degradation:
  - phone number quality_rating leaves GREEN
  - messaging limit tier drops
  - any sq_app_* template leaves APPROVED (PAUSED / DISABLED / REJECTED)
  - any sq_app_* template quality_score drops to YELLOW / RED / LOW / MEDIUM

Silent when healthy. State file holds the last snapshot so a steady-state problem
re-alerts weekly rather than daily, matching pass_cert_expiry_alert.sh.

Read-only against the Graph API. Sends nothing to customers.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request

# Cron scripts keep an explicit StreamHandler basicConfig: the json-logger standard
# silenced five pipelines for a week when it was assumed.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("shareeb/wa-quality")

ORDER_ENV = "/srv/qahwablk/order/backend/.env"
TG_ENV = "/srv/qahwablk/cashier-dashboard/.env"
STATE_FILE = os.environ.get("STATE_FILE", "/srv/qahwablk/monitor/shareeb-wa-quality.state")
WABA_ID = os.environ.get("WABA_ID", "322747457597702")
GRAPH = "https://graph.facebook.com/v19.0"
REPEAT_SECS = 600_000  # ~6.9d: daily cron re-alerts weekly without drift
NAME_PREFIX = "sq_app_"

GOOD_PHONE_QUALITY = {"GREEN"}
GOOD_TEMPLATE_QUALITY = {"GREEN", "HIGH", "UNKNOWN"}  # UNKNOWN = not yet rated, not a fault
GOOD_STATUS = {"APPROVED"}
TIER_ORDER = ["TIER_50", "TIER_250", "TIER_1K", "TIER_2K", "TIER_10K", "TIER_100K", "TIER_UNLIMITED"]


def env_value(path: str, key: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip()
    except OSError as exc:
        log.error("cannot read %s: %s", path, exc)
    return ""


def graph_get(path: str, token: str, **params) -> dict:
    url = f"{GRAPH}/{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


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


def main() -> int:
    token = env_value(ORDER_ENV, "WHATSAPP_TOKEN")
    phone_id = env_value(ORDER_ENV, "WHATSAPP_PHONE_NUMBER_ID")
    if not token or not phone_id:
        log.error("WHATSAPP_TOKEN / WHATSAPP_PHONE_NUMBER_ID missing from %s", ORDER_ENV)
        return 1

    try:
        phone = graph_get(
            phone_id, token,
            fields="display_phone_number,quality_rating,whatsapp_business_manager_messaging_limit",
        )
        tpls = graph_get(
            f"{WABA_ID}/message_templates", token,
            fields="name,language,status,category,quality_score", limit=200,
        )
    except Exception as exc:  # noqa: BLE001
        log.error("graph api call failed: %s", exc)
        return 1

    quality = phone.get("quality_rating", "UNKNOWN")
    tier = phone.get("whatsapp_business_manager_messaging_limit", "UNKNOWN")

    watched = [t for t in tpls.get("data", []) if t.get("name", "").startswith(NAME_PREFIX)]
    problems: list[str] = []

    if quality not in GOOD_PHONE_QUALITY:
        problems.append(f"phone quality_rating = {quality} (was GREEN)")

    prior = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as fh:
                prior = json.load(fh)
        except (OSError, ValueError):
            prior = {}

    prior_tier = prior.get("tier")
    if prior_tier and tier in TIER_ORDER and prior_tier in TIER_ORDER:
        if TIER_ORDER.index(tier) < TIER_ORDER.index(prior_tier):
            problems.append(f"messaging tier DROPPED {prior_tier} -> {tier}")

    for t in watched:
        key = f"{t['name']}[{t['language']}]"
        status = t.get("status")
        score = (t.get("quality_score") or {}).get("score", "UNKNOWN")
        if status not in GOOD_STATUS:
            problems.append(f"{key} status = {status}")
        if score not in GOOD_TEMPLATE_QUALITY:
            problems.append(f"{key} quality = {score}")

    log.info(
        "checked phone_quality=%s tier=%s templates=%d problems=%d",
        quality, tier, len(watched), len(problems),
    )

    snapshot = {
        "tier": tier,
        "quality": quality,
        "checked_at": int(time.time()),
        "templates": {f"{t['name']}[{t['language']}]": {
            "status": t.get("status"),
            "category": t.get("category"),
            "score": (t.get("quality_score") or {}).get("score", "UNKNOWN"),
        } for t in watched},
    }

    if not problems:
        snapshot["last_alert"] = prior.get("last_alert", 0)
        _write_state(snapshot)
        return 0

    now = int(time.time())
    last_alert = int(prior.get("last_alert") or 0)
    if now - last_alert < REPEAT_SECS:
        log.info("problems present but within repeat window, not re-alerting")
        snapshot["last_alert"] = last_alert
        _write_state(snapshot)
        return 0

    body = "\n".join(f"- {p}" for p in problems)
    msg = (
        "[WA QUALITY] shareeb celebration templates degraded.\n\n"
        f"{body}\n\n"
        f"Phone {phone.get('display_phone_number','?')} | tier {tier} | quality {quality}\n"
        "These are MARKETING category by Meta's own re-categorisation (order-pickup#98). "
        "A PAUSED template stops celebrations silently — send failures are swallowed "
        "(order-pickup#83). Check the WhatsApp Manager template list and recent user "
        "feedback before sending more."
    )
    if telegram(msg):
        snapshot["last_alert"] = now
        log.info("alert sent: %d problem(s)", len(problems))
    else:
        snapshot["last_alert"] = last_alert
        log.error("alert NOT delivered")
    _write_state(snapshot)
    return 0


def _write_state(snapshot: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, indent=1)
    except OSError as exc:
        log.error("cannot write state %s: %s", STATE_FILE, exc)


if __name__ == "__main__":
    sys.exit(main())
