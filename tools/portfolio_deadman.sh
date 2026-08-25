#!/usr/bin/env bash
# Deadman for the hands-off portfolio loop: silence must mean "healthy", so
# something OUTSIDE Airflow has to notice when the daily portfolio_sync run
# never happened (dead scheduler, stuck chain, import error - all of which
# produce no failure callback). Alerts on three conditions:
#   1. no portfolio.sync_runs row for today (the run never completed)
#   2. undelivered pings older than today (Telegram kept failing across runs)
#   3. reconcile drift: an OPEN book position reads SELL on the board but was
#      not archived (the sync archives SELL-flips every run, so a survivor is a
#      stuck archive, not transient drift - low-noise by construction)
#
# Install on the droplet (root crontab; the chain finishes ~10:00 UTC, so
# 16:30 UTC leaves generous slack before alerting):
#   30 16 * * * /opt/elt-canonical-data/tools/portfolio_deadman.sh >> /var/log/portfolio_deadman.log 2>&1
#
# Credentials: everything (DATABASE_URL, TELEGRAM_*) is read from the
# airflow-scheduler container env - no secrets in this file or the crontab.
set -u

STAMP="$(date -u +'%F %T')"
# DEADMAN_TODAY_OVERRIDE: test hook - set to an impossible date to dry-fire the
# missing-run alert path end-to-end (the ping really sends). Unset in the crontab.
# DEADMAN_STALE_OVERRIDE: test hook for the OTHER branch - set to a positive
# number to dry-fire the stale-outbox alert without seeding unsent rows.
TODAY="${DEADMAN_TODAY_OVERRIDE:-$(date -u +%F)}"

ROW="$(docker exec airflow-scheduler bash -lc \
  "psql \"\$DATABASE_URL\" -tA -F'|' -c \"SELECT coalesce(max(as_of)::text,'never'), count(*) FILTER (WHERE NOT sent AND jsonb_array_length(msgs) > 0 AND as_of < CURRENT_DATE) FROM portfolio.sync_runs\"" \
  2>/dev/null)" || ROW=""
LAST="${ROW%%|*}"
STALE_UNSENT="${ROW##*|}"
[ -z "$ROW" ] && { LAST="query-failed"; STALE_UNSENT="0"; }
STALE_UNSENT="${DEADMAN_STALE_OVERRIDE:-$STALE_UNSENT}"

# Reconcile drift: open positions the board now reads SELL but the sync did not
# archive. DEADMAN_DRIFT_OVERRIDE dry-fires this branch without seeding rows.
DRIFT="$(docker exec airflow-scheduler bash -lc \
  "psql \"\$DATABASE_URL\" -tA -c \"SELECT count(*) FROM portfolio.positions p WHERE p.sold_date IS NULL AND EXISTS (SELECT 1 FROM serving.return_cluster_ticker_global_action_current g WHERE upper(g.ticker) = upper(p.ticker) AND g.global_action = 'SELL')\"" \
  2>/dev/null)" || DRIFT="0"
[ -z "$DRIFT" ] && DRIFT="0"
DRIFT="${DEADMAN_DRIFT_OVERRIDE:-$DRIFT}"

MSG=""
if [ "$LAST" != "$TODAY" ]; then
  MSG="🚨 portfolio_sync did NOT run today (last: $LAST). The daily chain or the scheduler is down - check Airflow on the droplet. Until it runs: no adopts, no sell/recoup pings."
elif [ "${STALE_UNSENT:-0}" -gt 0 ] 2>/dev/null; then
  MSG="🚨 portfolio_sync ran, but $STALE_UNSENT day(s) of Telegram pings are still undelivered (outbox). Telegram delivery is failing - check TELEGRAM_* env / network on the droplet."
elif [ "${DRIFT:-0}" -gt 0 ] 2>/dev/null; then
  MSG="🚨 $DRIFT open book position(s) read SELL on the board but were NOT archived by today's sync - the archive step may be stuck. Check portfolio_sync and the board."
fi

if [ -z "$MSG" ]; then
  echo "$STAMP ok - sync ran today, outbox clear"
  exit 0
fi

echo "$STAMP ALERT: $MSG"
# utils.alerting.notify resolves the Telegram credentials itself (dotenv) -
# a bare docker-exec env does NOT carry TELEGRAM_*, so never use raw urllib here.
docker exec airflow-scheduler python3 -c '
import sys
sys.path.insert(0, "/opt/airflow/dags")
from utils.alerting import notify
print("telegram:", "sent" if notify(sys.argv[1]) else "SEND FAILED")
' "$MSG"
