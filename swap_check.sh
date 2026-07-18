#!/usr/bin/env bash
# Daily swap pressure check — SILENT unless swap usage exceeds the threshold.
# Runs from mego's crontab at 05:00 UTC (08:00 Amman). Alert-only by design:
# no message means swap is healthy. Token from .env, never hardcoded.
set -euo pipefail

THRESHOLD_PCT=80
ENV_FILE="/srv/qahwablk/cashier-dashboard/.env"

read -r SWAP_TOTAL SWAP_USED <<< "$(free | awk '/Swap/ {print $2, $3}')"
if [ "$SWAP_TOTAL" -eq 0 ]; then
  echo "no swap configured — nothing to check"
  exit 0
fi
SWAP_PCT=$(( SWAP_USED * 100 / SWAP_TOTAL ))
if [ "$SWAP_PCT" -le "$THRESHOLD_PCT" ]; then
  echo "swap ${SWAP_PCT}% used (threshold ${THRESHOLD_PCT}%) — silent"
  exit 0
fi

AVAIL_RAM="$(free -h | awk '/Mem/ {print $7}')"

# Top 3 processes by VmSwap out of /proc — needs sudo to read every process.
TOP_SWAP="$(sudo -n bash -c '
  for f in /proc/[0-9]*/status; do
    awk -v pid="${f%/status}" "
      /^Name:/  {name=\$2}
      /^VmSwap:/ {if (\$2 > 0) printf \"%d %s %s\n\", \$2, substr(pid,7), name}
    " "$f" 2>/dev/null
  done' | sort -rn | head -3 | awk '{printf "%d MB  pid %s  %s\n", $1/1024, $2, $3}')"
[ -z "$TOP_SWAP" ] && TOP_SWAP="(no per-process swap found)"

MSG="<b>BLK SWAP ALERT</b> $(date -u '+%Y-%m-%d %H:%M UTC')
swap: ${SWAP_PCT}% used (threshold ${THRESHOLD_PCT}%)
available RAM: ${AVAIL_RAM}

<b>Top 3 by swap:</b>
$TOP_SWAP"

TOKEN="$(sudo -n grep -E '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | cut -d= -f2-)"
CHAT_ID="$(sudo -n grep -E '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d= -f2-)"
if [ -z "$TOKEN" ] || [ -z "$CHAT_ID" ]; then
  echo "ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing from $ENV_FILE" >&2
  exit 1
fi
HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys; print(json.dumps({"chat_id": sys.argv[1], "text": sys.argv[2], "parse_mode": "HTML"}))' "$CHAT_ID" "$MSG")")
if [ "$HTTP" != "200" ]; then
  echo "ERROR: Telegram send failed HTTP $HTTP" >&2
  exit 1
fi
echo "swap alert sent (${SWAP_PCT}%)"
