#!/bin/bash
# repo_drift_alert.sh — daily check that every git checkout on this server
# matches its origin. Companion to cert-expiry-alert / cron-staleness-alert
# (same /etc/cron.d convention, root, daily).
#
# WHY THIS EXISTS (2026-08-04):
# Only three repos auto-deploy on a cron (cashier-dashboard, order-lb,
# krom-plus-app) and eight auto-pull on merge via the cto-agent webhook.
# Everything else is deploy-by-hand, and nothing told anyone when a
# hand-deployed repo fell behind. On 2026-08-04 four repos were drifted at
# once: cto-agent 1 behind (a merged fix sitting undeployed for 10 hours),
# order 2 behind, order-v3 23 behind, blkprojects-recon 28 behind since
# 2026-07-04. order-v3 and blkprojects-recon were not in cto-agent's
# REPO_WATCH_CONFIG at all, so no existing check could ever have seen them.
#
# This scans the FILESYSTEM, not a config list, precisely so a repo nobody
# registered still gets caught.
#
# Silent when everything is in sync. Alerts @Blk_Server_bot otherwise, and
# re-alerts only when the drift picture CHANGES or once a week, so a repo
# that is deliberately parked does not nag daily.

set -u

ROOT="${REPO_ROOT:-/mnt/HC_Volume_105265098/qahwablk}"
TG_ENV="${TG_ENV:-/srv/qahwablk/cashier-dashboard/.env}"
STATE_FILE="${STATE_FILE:-/srv/qahwablk/monitor/repo-drift-alert.state}"
REPEAT_SECS=600000   # ~6.9 days: re-alert weekly even if nothing changed

# Repos that are intentionally not kept in sync. Add with a dated reason.
# Format: one repo basename per line.
IGNORE_REPOS="${IGNORE_REPOS:-}"

now_epoch=$(date +%s)
drift_lines=""
detail_lines=""
n_drift=0
n_checked=0

for d in "$ROOT"/*/; do
    repo=$(basename "$d")
    [ -d "$d/.git" ] || continue
    case " $IGNORE_REPOS " in *" $repo "*) continue ;; esac
    n_checked=$((n_checked + 1))

    owner=$(stat -c %U "$d/.git" 2>/dev/null) || continue
    g() { sudo -n -u "$owner" git -C "$d" "$@" 2>/dev/null; }

    # Fetch as the OWNER. Running git as the wrong user on these repos
    # half-applies and pollutes .git (2026-07-05).
    g fetch origin --quiet

    branch=$(g rev-parse --abbrev-ref HEAD)
    if [ "$branch" = "HEAD" ]; then
        drift_lines="${drift_lines}- ${repo}: DETACHED HEAD at $(g rev-parse --short HEAD)"$'\n'
        n_drift=$((n_drift + 1))
        continue
    fi

    upstream="origin/${branch}"
    if ! g rev-parse --verify --quiet "$upstream" >/dev/null; then
        drift_lines="${drift_lines}- ${repo}: no ${upstream} (never fetched, or renamed upstream)"$'\n'
        n_drift=$((n_drift + 1))
        continue
    fi

    behind=$(g rev-list --count "HEAD..$upstream")
    ahead=$(g rev-list --count "$upstream..HEAD")
    behind=${behind:-0}
    ahead=${ahead:-0}

    [ "$behind" = "0" ] && [ "$ahead" = "0" ] && continue

    n_drift=$((n_drift + 1))
    if [ "$ahead" != "0" ] && [ "$behind" != "0" ]; then
        state="DIVERGED (${behind} behind, ${ahead} ahead)"
    elif [ "$ahead" != "0" ]; then
        state="${ahead} AHEAD (unpushed local commits)"
    else
        state="${behind} behind"
    fi

    last_pull=$(g reflog --date=short | grep -m1 'pull' | sed -E 's/.*\{([0-9-]+)\}.*/\1/')
    drift_lines="${drift_lines}- ${repo} [${branch}]: ${state}, last pull ${last_pull:-never}"$'\n'

    if [ "$behind" != "0" ]; then
        top=$(g log --oneline "HEAD..$upstream" | head -2 | sed 's/^/    /')
        detail_lines="${detail_lines}${repo}:"$'\n'"${top}"$'\n'
    fi
done

if [ "$n_drift" -eq 0 ]; then
    echo "repo-drift: all ${n_checked} checkouts in sync"
    rm -f "$STATE_FILE"
    exit 0
fi

# Re-alert only when the picture changes, or weekly. The signature is the
# drift list itself, so "cto-agent went from 1 to 2 behind" re-alerts but a
# parked repo sitting at the same offset stays quiet.
sig=$(printf '%s' "$drift_lines" | md5sum | cut -d' ' -f1)
if [ -f "$STATE_FILE" ]; then
    last_sig=$(head -1 "$STATE_FILE" 2>/dev/null)
    last_ts=$(sed -n 2p "$STATE_FILE" 2>/dev/null || echo 0)
    if [ "$sig" = "$last_sig" ] && [ $(( now_epoch - ${last_ts:-0} )) -lt "$REPEAT_SECS" ]; then
        echo "repo-drift: ${n_drift} drifted, unchanged since last alert — staying quiet"
        exit 0
    fi
fi

TOKEN=$(grep -E '^TELEGRAM_BOT_TOKEN=' "$TG_ENV" | head -1 | cut -d= -f2-)
CHAT_ID=$(grep -E '^TELEGRAM_CHAT_ID=' "$TG_ENV" | head -1 | cut -d= -f2-)
if [ -z "$TOKEN" ] || [ -z "$CHAT_ID" ]; then
    echo "repo-drift: missing telegram creds in $TG_ENV" >&2
    exit 1
fi

msg="[DRIFT] ${n_drift} of ${n_checked} checkouts do not match origin.

${drift_lines}
Waiting commits:
${detail_lines}
Deploy: cashier-dashboard / order-lb / krom-plus-app auto-deploy on cron. The eight pull-mode repos auto-pull on merge. Everything else is by hand:
  order        sudo /srv/qahwablk/order-deploy.sh v1
  cto-agent    sudo -u mego git -C /srv/qahwablk/cto-agent pull --ff-only && sudo systemctl restart cto-agent-webhook cto-agent-watcher
  other        sudo -u <owner> git -C /srv/qahwablk/<repo> pull --ff-only, then restart its service"

http=$(curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
    -d "chat_id=${CHAT_ID}" \
    --data-urlencode "text=${msg}")
if [ "$http" != "200" ]; then
    echo "repo-drift: telegram send failed HTTP $http" >&2
    exit 1
fi

printf '%s\n%s\n' "$sig" "$now_epoch" > "$STATE_FILE"
echo "repo-drift: alert sent (${n_drift} of ${n_checked} drifted)"
