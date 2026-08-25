#!/bin/bash
# AlphaStream pipeline watch (versioned successor of /opt/alphastream/pipeline_watch.sh,
# brought into the repo 2026-08-24 after the lock-stall incident).
#
# Three checks, all DAGs:
#   1. STALE  - the daily chain (or the daily raw fetch) has no SUCCESS in >26h.
#   2. FAILED - any DAG's latest run FAILED (belt-and-braces with the per-task
#               on_failure_callback pings; this also catches callback-path loss).
#   3. STUCK  - any DagRun sitting in RUNNING beyond its per-DAG budget
#               (2026-08-24 incident class: a lock queue held dbt_run_combined
#               "running" for 6h+ and the old watch only fired at 26h stale).
#               Budgets: walk_forward 15h, manual bulk exempt, everything else 4h.
#
# Telegram creds come from Airflow Variables (survive container recreation).
# Max one delivered alert per 6h (state file). Runs from root crontab hourly:
#   17 * * * * /opt/elt-canonical-data/tools/pipeline_watch.sh >> /var/log/alphastream_pipeline_watch.log 2>&1
STATE=/var/tmp/alphastream_pipeline_watch.last
if [ -f "$STATE" ] && [ $(( $(date +%s) - $(cat "$STATE") )) -lt 21600 ]; then
  echo "$(date -u +%FT%TZ) SKIP (alerted <6h ago)"; exit 0
fi
# Host disk fills silently and takes EVERY DAG down at once - the daily-chain
# stale alert would only notice hours later. Check the host filesystem (this
# script runs on the droplet, not in a container) and pass it into the alert.
HOST_DISK_PCT=$(df -P / | awk 'NR==2{gsub("%","",$5); print $5}')
OUT=$(docker exec -i -e HOST_DISK_PCT="$HOST_DISK_PCT" airflow-scheduler python3 - <<PY
import os
from datetime import datetime, timedelta, timezone
from airflow.models import DagRun, Variable
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

DISK_ALERT_PCT = 85

STALE_DAGS = {"cdm_ingest_massive": 26, "raw_ingest_massive_daily": 26}
STUCK_BUDGET_H = {"walk_forward_dbt_models": 15, "raw_ingest_massive_bulk": None}  # None = exempt
STUCK_DEFAULT_H = 4

now = datetime.now(timezone.utc)
msgs = []
try:
    disk = int(os.environ.get("HOST_DISK_PCT", "0"))
    if disk >= DISK_ALERT_PCT:
        msgs.append("host disk at %d%% (>=%d%%) - free space before it takes every DAG down" % (disk, DISK_ALERT_PCT))
except ValueError:
    pass
with create_session() as s:
    dag_ids = [r[0] for r in s.query(DagRun.dag_id).distinct().all()]
    for dag_id in sorted(dag_ids):
        runs = (s.query(DagRun)
                .filter(DagRun.dag_id == dag_id, DagRun.start_date.isnot(None))
                .order_by(DagRun.start_date.desc()).limit(20).all())
        if not runs:
            continue
        latest = runs[0]
        # Recency bounds: a retired DAG whose last-ever run failed in January
        # must not nag forever (seen on first live run: 3 dead DAGs surfaced).
        if dag_id in STALE_DAGS:
            last_ok = next((r for r in runs if r.state == DagRunState.SUCCESS), None)
            lim = STALE_DAGS[dag_id]
            if not last_ok or (now - last_ok.start_date) > timedelta(hours=lim):
                age = "never" if not last_ok else "%.1fh" % ((now - last_ok.start_date).total_seconds()/3600)
                msgs.append("no successful %s run in %s" % (dag_id, age))
        if latest.state == DagRunState.FAILED and (now - latest.start_date) < timedelta(hours=48):
            msgs.append("%s latest run FAILED (%s)" % (dag_id, latest.run_id))
        budget = STUCK_BUDGET_H.get(dag_id, STUCK_DEFAULT_H)
        if budget is not None:
            for r in runs:
                if (r.state == DagRunState.RUNNING
                        and timedelta(hours=budget) < (now - r.start_date) < timedelta(days=14)):
                    h = (now - r.start_date).total_seconds()/3600
                    msgs.append("%s STUCK in running for %.1fh (budget %dh) - check locks/queues" % (dag_id, h, budget))
if msgs:
    tok = Variable.get("TELEGRAM_BOT_TOKEN", default_var="")
    cid = Variable.get("TELEGRAM_CHAT_ID", default_var="")
    text = "\U0001F6A8 [prod] pipeline watch: " + "; ".join(msgs[:8])
    if tok and cid:
        import json, urllib.request
        req = urllib.request.Request("https://api.telegram.org/bot%s/sendMessage" % tok,
            data=json.dumps({"chat_id": cid, "text": text}).encode(),
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=20); print("ALERT_SENT: " + text)
        except Exception as e:
            print("ALERT_FAILED: %s" % e)
    else:
        print("ALERT_UNCONFIGURED: " + text)
else:
    print("OK")
PY
)
echo "$(date -u +%FT%TZ) $OUT"
case "$OUT" in *ALERT_SENT*) date +%s > "$STATE";; esac
