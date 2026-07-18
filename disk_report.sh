#!/usr/bin/env bash
# Weekly disk hygiene report — snapshot, growth diff, Telegram summary.
# Runs from mego's crontab (Sunday 05:00 UTC = 08:00 Amman). Needs
# passwordless sudo for du over root-owned dirs and for reading the
# Telegram token out of the qahwablk-owned .env (never hardcoded here).
set -euo pipefail

VOL="/mnt/HC_Volume_105265098"
SNAP_DIR="/mnt/data/monitor-snapshots"
ENV_FILE="/srv/qahwablk/cashier-dashboard/.env"
RETENTION_DAYS=84   # 12 weeks of weekly snapshots
TODAY="$(date +%Y%m%d)"
SNAP="$SNAP_DIR/disk-$TODAY.txt"

mkdir -p "$SNAP_DIR"

# ── 1. Snapshot: MB per top-level dir on the volume, plus /tmp and /var/log ──
{
  for d in "$VOL"/*/; do
    sudo -n du -sm "${d%/}" 2>/dev/null
  done
  sudo -n du -sm /tmp /var/log 2>/dev/null
} | sort -rn > "$SNAP"

# ── 2. Growth diff vs the previous snapshot (if one exists) ─────────────────
PREV="$(ls -1 "$SNAP_DIR"/disk-*.txt 2>/dev/null | grep -v "disk-$TODAY" | sort | tail -1 || true)"
GROWTH="(no previous snapshot — baseline run)"
if [ -n "$PREV" ]; then
  GROWTH="$(awk -F'\t' '
    NR==FNR { prev[$2]=$1; next }
    { delta=$1-prev[$2]; if (delta!=0) printf "%+d MB  %s\n", delta, $2 }
  ' "$PREV" "$SNAP" | sort -rn | head -5)"
  [ -z "$GROWTH" ] && GROWTH="(no per-dir changes vs $(basename "$PREV"))"
fi

# ── 3. Logs >100M not covered by any logrotate config ───────────────────────
# Expand every glob in logrotate configs into a covered-file list, then
# flag big *.log files that are not in it. Postgres manages its own logs.
COVERED="$(mktemp)"
awk '{for(i=1;i<=NF;i++) if ($i ~ /^\//) print $i}' /etc/logrotate.conf /etc/logrotate.d/* 2>/dev/null \
  | tr -d '{' | while read -r pat; do compgen -G "$pat" || true; done | sort -u > "$COVERED"
BIGLOGS="$(sudo -n find "$VOL" / -xdev -name "*.log" -size +100M -type f 2>/dev/null \
  | grep -v "^$VOL/postgres/" | sort -u | while read -r f; do
      grep -qxF "$f" "$COVERED" || { sz=$(sudo -n du -m "$f" | cut -f1); echo "${sz} MB  $f"; }
    done)"
rm -f "$COVERED"
[ -z "$BIGLOGS" ] && BIGLOGS="none"

# ── 4. df + swap summary ────────────────────────────────────────────────────
DF_ROOT="$(df -h / | awk 'NR==2 {printf "root: %s/%s (%s)", $3, $2, $5}')"
DF_VOL="$(df -h "$VOL" | awk 'NR==2 {printf "volume: %s/%s (%s)", $3, $2, $5}')"
SWAP_PCT="$(free | awk '/Swap/ {if ($2>0) printf "%.0f", $3/$2*100; else print 0}')"

# ── 5. Compose + send ───────────────────────────────────────────────────────
MSG="<b>BLK DISK REPORT</b> $(date -u '+%Y-%m-%d %H:%M UTC')
$DF_ROOT
$DF_VOL
swap: ${SWAP_PCT}% used

<b>Top growers since last snapshot:</b>
$GROWTH

<b>Logs &gt;100M without logrotate:</b>
$BIGLOGS"

TOKEN="$(sudo -n grep -E '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | cut -d= -f2-)"
CHAT_ID="$(sudo -n grep -E '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d= -f2-)"
if [ -z "$TOKEN" ] || [ -z "$CHAT_ID" ]; then
  echo "ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing from $ENV_FILE" >&2
  exit 1
fi
HTTP=$(curl -s -o /tmp/disk_report_tg_resp.json -w "%{http_code}" \
  -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys; print(json.dumps({"chat_id": sys.argv[1], "text": sys.argv[2], "parse_mode": "HTML"}))' "$CHAT_ID" "$MSG")")
if [ "$HTTP" != "200" ]; then
  echo "ERROR: Telegram send failed HTTP $HTTP: $(cat /tmp/disk_report_tg_resp.json)" >&2
  exit 1
fi
echo "report sent ($HTTP), snapshot: $SNAP"

# ── 6. Snapshot retention: keep 12 weeks ────────────────────────────────────
find "$SNAP_DIR" -maxdepth 1 -name "disk-*.txt" -mtime +$RETENTION_DAYS -delete
