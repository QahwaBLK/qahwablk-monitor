#!/bin/bash
# Apple Wallet pass-signing cert expiry watch. Companion to
# cashier-dashboard/scripts/cert-expiry-alert.sh (same /etc/cron.d file,
# root, 09:00 UTC daily) — that one only walks Let's Encrypt; this watches
# the .p12 Pass Type ID cert, which breaks pass signing silently on expiry.
#
# Alerts @Blk_Server_bot starting THRESHOLD_DAYS (60) before expiry, then
# repeats weekly (state file) until the cert is replaced. Silent otherwise.
# The .p12 password comes from the order backend .env and is never printed.

set -u
P12=/srv/qahwablk/order/backend/secrets/shareeb_pass_cert_der.p12
ORDER_ENV=/srv/qahwablk/order/backend/.env
TG_ENV=/srv/qahwablk/cashier-dashboard/.env
THRESHOLD_DAYS="${THRESHOLD_DAYS:-60}"
STATE_FILE="${STATE_FILE:-/srv/qahwablk/monitor/pass-cert-alert.state}"
REPEAT_SECS=600000   # ~6.9 days: daily cron re-alerts weekly without drift

if [ ! -r "$P12" ] || [ ! -r "$ORDER_ENV" ]; then
    echo "pass-cert-expiry: $P12 or $ORDER_ENV unreadable" >&2
    exit 1
fi

PASS_PW=$(grep -E '^PASS_CERT_PASSWORD=' "$ORDER_ENV" | head -1 | cut -d= -f2-)
if [ -z "$PASS_PW" ]; then
    echo "pass-cert-expiry: PASS_CERT_PASSWORD missing from $ORDER_ENV" >&2
    exit 1
fi
export PASS_PW

end=$(openssl pkcs12 -in "$P12" -nokeys -clcerts -passin env:PASS_PW 2>/dev/null \
      | openssl x509 -noout -enddate 2>/dev/null | cut -d= -f2)
unset PASS_PW
if [ -z "$end" ]; then
    echo "pass-cert-expiry: could not read leaf cert from $P12 (wrong password? corrupt file?)" >&2
    exit 1
fi

end_epoch=$(date -d "$end" +%s)
now_epoch=$(date +%s)
days=$(( (end_epoch - now_epoch) / 86400 ))
end_date=$(date -d "$end" +%Y-%m-%d)

[ "$days" -gt "$THRESHOLD_DAYS" ] && exit 0

# Weekly repeat gate
if [ -f "$STATE_FILE" ]; then
    last=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
    [ $(( now_epoch - last )) -lt "$REPEAT_SECS" ] && exit 0
fi

TOKEN=$(grep -E '^TELEGRAM_BOT_TOKEN=' "$TG_ENV" | head -1 | cut -d= -f2-)
CHAT_ID=$(grep -E '^TELEGRAM_CHAT_ID=' "$TG_ENV" | head -1 | cut -d= -f2-)
if [ -z "$TOKEN" ] || [ -z "$CHAT_ID" ]; then
    echo "pass-cert-expiry: missing telegram creds in $TG_ENV" >&2
    exit 1
fi

msg="[CERT] Apple Wallet pass cert expires ${end_date} (${days} days).
Renew the Pass Type ID cert (pass.com.qahwablk.shareeb) in the Apple Developer account, download the new .p12, replace it in /srv/qahwablk/order/backend/secrets/, restart the order backend service (qahwablk-order-api). Existing passes keep working; new pass signing breaks on expiry. Coordinate with Anan."

http=$(curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
    -d "chat_id=${CHAT_ID}" \
    --data-urlencode "text=${msg}")
if [ "$http" != "200" ]; then
    echo "pass-cert-expiry: telegram send failed HTTP $http" >&2
    exit 1
fi
echo "$now_epoch" > "$STATE_FILE"
echo "pass-cert-expiry: alert sent (${days} days left)"
