#!/usr/bin/env bash
# Sunday run: the old system's whole chain once a week, then everything old but the database stops.
#
# launchd starts it at 01:00 each Sunday (tools/launchd/com.alphastream.sunday-run.plist). The Mac wakes itself at
# 00:55 for it: sudo pmset repeat wakeorpoweron U 00:55:00. Decided with Kevin on 2026-11-25, in place of the daily
# run, which has been off since 2026-11-11 to save the Mac's memory.
#
# In order:
#   1. Keeps the Mac awake until it ends, and makes sure the database (staging-postgres) runs. The database stays on
#      all week, because the dashboard's old pages read it.
#   2. Starts Airflow's page, pauses every job that has a timer, then starts the scheduler. Between Sundays every
#      timed job stays paused, so nothing runs on its own. Jobs that only another job starts stay unpaused.
#   3. Tells Telegram what is about to run.
#   4. Runs each job that is due, in the daily order. It unpauses the job, the scheduler starts the one run that is
#      due, the script waits until that run and every run it set off have ended, then pauses the job again.
#      A monthly job is due on the first Sunday after its day.
#   5. Tells Telegram how each job ended and how long the run took. At 12:00 it stops waiting and says what did not
#      finish, so nothing old runs when the new system's 13:00 job starts.
#   6. Stops every old container but the database, and writes a summary that the allure check
#      backend.jobs.sunday_run reads.
#
#   tools/sunday_run.sh             the run
#   tools/sunday_run.sh --dry-run   prints the plan and the first message, and touches nothing
set -uo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"

DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

DB=${DB:-staging-postgres}
SCHEDULER=${SCHEDULER:-airflow-staging-scheduler}
WEBSERVER=${WEBSERVER:-airflow-staging-webserver}
OTHERS=(airflow-shiny airflow-postgres dbt-docs-canonical allure)   # started by hand; stopped at the end if running
STOP_AT=${STOP_AT:-12}            # hour the run stops waiting
LATEST_START=${LATEST_START:-8}   # a run that could only start at this hour or later is skipped
POLL=${POLL:-60}                  # seconds between looks while a job runs
SUMMARY=${SUMMARY:-$HOME/Library/Logs/alphastream-sunday-run.last.json}
LOCK=/tmp/alphastream-sunday-run.lock

# The jobs with a timer, in the daily order: "dag|what Telegram calls it".
JOBS=(
  "raw_ingest_massive_daily|Prices, the last 60 days"
  "raw_gap_fill_weekly|Missing price days"
  "raw_ingest_delisted_monthly|Delisted stocks (monthly)"
  "walk_forward_dbt_models|Walk-forward models (monthly)"
  "qual_scorecards_4monthly|Scorecards (every four months)"
  "cdm_ingest_massive|Rebuild, then models, scorecards and portfolio sync"
  "board_transition_notifier|Board changes"
  "gate_snapshot_archive|Snapshot"
  "qual_category_tags|Category tags"
)
# Timed jobs that never run here: SEC filings and statements come from alphastream-next since 2026-11-26 (Kevin:
# "alphastream should not ingest edgar"). They stay paused, so neither this run nor Airflow's own timer starts them.
OFF=(raw_ingest_edgar_daily raw_ingest_edgar_statements)
# The jobs that only another job starts. cdm_ingest_massive sets them off in this order.
TRIGGERED=(
  "metrics_dbt_models|Metrics"
  "inference_dbt_models|Models"
  "inference_backtest_dbt_models|Backtest"
  "qual_scorecards_on_entry|Scorecards on entry"
  "portfolio_sync|Portfolio sync"
)

dag_of()   { printf '%s' "${1%%|*}"; }
label_of() { printf '%s' "${1#*|}"; }
CRON_DAGS=();      for j in "${JOBS[@]}"; do CRON_DAGS+=("$(dag_of "$j")"); done
TRIGGERED_DAGS=(); for j in "${TRIGGERED[@]}"; do TRIGGERED_DAGS+=("$(dag_of "$j")"); done

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# Reads and sets Airflow's own records through Airflow, inside the page's container.
read -r -d '' HELPER_PY <<'PY'
import json, sys
from datetime import datetime, timezone
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session

cmd, args = sys.argv[1], sys.argv[2:]
now = datetime.now(timezone.utc)

def at(epoch):
    return datetime.fromtimestamp(float(epoch), timezone.utc)

def mins(a, b):
    return round((b - a).total_seconds() / 60) if a and b else None

def took(m):
    if m is None:
        return ""
    return f"{m} min" if m < 60 else f"{m // 60} h {m % 60} min"

with create_session() as s:
    if cmd in ("pause", "unpause"):
        for d in args:
            dm = s.query(DagModel).filter(DagModel.dag_id == d).one_or_none()
            if dm is None:
                print(f"no job called {d}", file=sys.stderr)
            else:
                dm.is_paused = cmd == "pause"
        print("OUT:ok")
    elif cmd == "unparsed":      # how many of the jobs the scheduler has not read since then
        t = at(args[0])
        seen = dict(s.query(DagModel.dag_id, DagModel.last_parsed_time).filter(DagModel.dag_id.in_(args[1:])).all())
        print("OUT:" + str(sum(1 for d in args[1:] if not seen.get(d) or seen[d] < t)))
    elif cmd == "due":           # the jobs whose next run the scheduler would start now
        nxt = dict(s.query(DagModel.dag_id, DagModel.next_dagrun_create_after).filter(DagModel.dag_id.in_(args)).all())
        print("OUT:" + " ".join(d for d in args if nxt.get(d) and nxt[d] <= now))
    elif cmd == "started":       # runs of one job queued since then
        print("OUT:" + str(s.query(DagRun).filter(DagRun.dag_id == args[0], DagRun.queued_at >= at(args[1])).count()))
    elif cmd == "active":        # runs still queued or running, of jobs that are not paused
        print("OUT:" + str(s.query(DagRun).join(DagModel, DagModel.dag_id == DagRun.dag_id)
              .filter(DagRun.state.in_(("queued", "running")), DagModel.is_paused.is_(False)).count()))
    elif cmd == "leftover":      # runs left queued or running from before this run
        rows = s.query(DagRun.dag_id, DagRun.state).filter(DagRun.state.in_(("queued", "running"))).all()
        print("OUT:" + " ".join(f"{d}:{st}" for d, st in rows))
    elif cmd == "report":        # start epoch, end time, 1 if stopped at the deadline, then dag|label|status
        t0, end_hhmm, cut = at(args[0]), args[1], args[2] == "1"
        jobs, chain = [], []
        for a in args[3:]:
            d, label, status = a.split("|", 2)
            (chain if status == "triggered" else jobs).append((d, label, status))
        runs = {}
        for r in s.query(DagRun).filter(DagRun.queued_at >= t0).order_by(DagRun.queued_at):
            runs.setdefault(r.dag_id, []).append(r)

        def outcome(d):
            rs = runs.get(d) or []
            if not rs:
                return None, None
            return str(rs[-1].state), mins(rs[0].start_date or rs[0].queued_at, rs[-1].end_date or now)

        mark = {"success": "✅", "failed": "❌"}
        lines, notdue, out = [], [], []
        for d, label, status in jobs:
            if status == "notdue":
                notdue.append(label)
                out.append(dict(dag=d, label=label, result="not due"))
            elif status == "cut":
                lines.append(f"⏹ {label}: not reached before the stop")
                out.append(dict(dag=d, label=label, result="not reached"))
            else:
                state, m = outcome(d)
                if state is None:
                    lines.append(f"❌ {label}: did not start")
                    out.append(dict(dag=d, label=label, result="did not start"))
                    continue
                word = {"success": "done", "failed": "failed"}.get(state, f"still {state} when the run stopped")
                lines.append(f"{mark.get(state, '⏹')} {label}: {word}, {took(m)}")
                out.append(dict(dag=d, label=label, result=state, minutes=m))
                if d == "cdm_ingest_massive":
                    parts = []
                    for cd, clabel, _ in chain:
                        cs, cm = outcome(cd)
                        parts.append(f"{mark.get(cs, '⏹')} {clabel}")
                        out.append(dict(dag=cd, label=clabel, result=cs or "did not run", minutes=cm, part_of=d))
                    lines.append("      " + "  ".join(parts))
        total = took(mins(t0, now))
        head = (f"Sunday run stopped at {end_hhmm}, after {total}: some jobs were still going."
                if cut else f"Sunday run finished at {end_hhmm}, after {total}.")
        tail = ([f"Not due this week: {', '.join(notdue)}."] if notdue else [])
        text = "\n".join([head] + lines + tail + ["Everything old but the database switches off now."])
        print("JSON:" + json.dumps(out))
        print("---TEXT---")
        print(text)
PY

helper() { docker exec -i "$WEBSERVER" python3 - "$@" <<<"$HELPER_PY" | sed -n 's/^OUT://p' | tail -1; }

# Telegram goes through the old system's own notify, as tools/portfolio_deadman.sh does.
say() {
  if [ "$DRY" = 1 ]; then
    printf -- '--- Telegram would get:\n%s\n---\n' "$1"
    return 0
  fi
  local out
  out=$(docker exec -i "$WEBSERVER" python3 - "$1" 2>/dev/null <<'PY'
import sys
sys.path.insert(0, "/opt/airflow/dags")
from utils.alerting import notify
print("OUT:sent" if notify(sys.argv[1]) else "OUT:failed")
PY
)
  out=$(printf '%s\n' "$out" | sed -n 's/^OUT://p' | tail -1)
  log "telegram: ${out:-failed}"
  [ "$out" = "sent" ]
}

exists()  { docker inspect "$1" >/dev/null 2>&1; }
running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]; }
health()  { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1" 2>/dev/null; }

# Waits up to $2 seconds for a container to say it is healthy.
wait_healthy() {
  local waited=0
  while [ "$waited" -lt "$2" ]; do
    [ "$(health "$1")" = "healthy" ] && return 0
    sleep 10; waited=$((waited + 10))
  done
  return 1
}

start() {
  running "$1" && return 0
  log "starting $1"
  docker start "$1" >/dev/null
}

STARTED_AT=$(date '+%Y-%m-%dT%H:%M:%S%z')
START_EPOCH=$(date +%s)
START_SENT=no
END_SENT=no
ERROR=""
SKIPPED=false
CUT=0
JOBS_JSON="[]"
DONE=0

# Stops every old container but the database.
stop_old() {
  local c
  for c in "$SCHEDULER" "$WEBSERVER" "${OTHERS[@]}"; do
    if running "$c"; then
      log "stopping $c"
      docker stop -t 60 "$c" >/dev/null
    fi
  done
}

still_running_json() {
  local c list=()
  for c in "$SCHEDULER" "$WEBSERVER" "${OTHERS[@]}"; do running "$c" && list+=("\"$c\""); done
  local IFS=,
  printf '[%s]' "${list[*]:-}"
}

write_summary() {
  [ "$DRY" = 1 ] && return 0
  local err=${ERROR//\"/\'}
  printf '{"run_day": "%s", "started": "%s", "finished": "%s", "skipped": %s, "error": "%s", "stopped_at_deadline": %s, "messages": {"start": "%s", "end": "%s"}, "database_running": %s, "old_containers_running": %s, "jobs": %s}\n' \
    "$(date +%F)" "$STARTED_AT" "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$SKIPPED" "$err" \
    "$([ "$CUT" = 1 ] && echo true || echo false)" "$START_SENT" "$END_SENT" \
    "$(running "$DB" && echo true || echo false)" "$(still_running_json)" "$JOBS_JSON" > "$SUMMARY"
  log "summary written to $SUMMARY"
}

# Any stop before the end still tells Telegram and switches the old containers off.
on_exit() {
  local code=$?
  if [ "$DONE" != 1 ] && [ "$DRY" != 1 ]; then
    ERROR=${ERROR:-"it ended unexpectedly (exit $code)"}
    log "stopped early: $ERROR"
    running "$WEBSERVER" && say "Sunday run stopped early: $ERROR. Everything old but the database switches off now." && END_SENT=sent
    stop_old
    write_summary
  fi
  rmdir "$LOCK" 2>/dev/null
}

fail() { ERROR=$1; exit 1; }

if ! mkdir "$LOCK" 2>/dev/null; then
  log "another Sunday run holds $LOCK; nothing to do"
  exit 0
fi
trap on_exit EXIT

[ "$DRY" = 1 ] || { caffeinate -i -s -w $$ & }

log "Sunday run begins$([ "$DRY" = 1 ] && echo ', dry run')"
docker info >/dev/null 2>&1 || { [ "$DRY" = 1 ] && fail "Docker is not running"; log "starting OrbStack"; orb start; sleep 30; }
docker info >/dev/null 2>&1 || fail "Docker did not start"
for c in "$DB" "$SCHEDULER" "$WEBSERVER"; do exists "$c" || fail "the container $c is missing (docker/airflow-staging, docker/staging-postgres)"; done

hour=$((10#$(date +%H)))
stop_epoch=$(date -j -v"${STOP_AT}"H -v0M -v0S +%s)

if [ "$DRY" = 1 ]; then
  msg="Sunday run starting at $(date +%H:%M). In this order, each when it is due:"
  n=0
  for j in "${JOBS[@]}"; do n=$((n + 1)); msg+=$'\n'"$n. $(label_of "$j")"; done
  msg+=$'\n'"It stops by ${STOP_AT}:00 at the latest. Then everything old but the database switches off."
  say "$msg"
  log "database running: $(running "$DB" && echo yes || echo no). Would pause: ${CRON_DAGS[*]} ${OFF[*]}. Would unpause: ${TRIGGERED_DAGS[*]}"
  log "stops waiting at $(date -r "$stop_epoch" '+%H:%M'); skips a start at ${LATEST_START}:00 or later"
  DONE=1
  exit 0
fi

# 1. The database, which stays on.
start "$DB" || fail "the database did not start"
wait_healthy "$DB" 180 || fail "the database did not become healthy"

# 2. Airflow's page first, so every timed job is paused before the scheduler could start one.
start "$WEBSERVER" || fail "Airflow's page did not start"
wait_healthy "$WEBSERVER" 300 || fail "Airflow's page did not become healthy"

if [ "$hour" -ge "$LATEST_START" ]; then
  SKIPPED=true
  say "Sunday run skipped: the Mac could only start it at $(date +%H:%M), too late to finish by ${STOP_AT}:00. It tries again next Sunday at 01:00." && START_SENT=sent
  stop_old
  DONE=1
  write_summary
  exit 0
fi

[ "$(helper pause "${CRON_DAGS[@]}" "${OFF[@]}")" = "ok" ] || fail "the timed jobs could not be paused"
[ "$(helper unpause "${TRIGGERED_DAGS[@]}")" = "ok" ] || fail "the chained jobs could not be unpaused"
left=$(helper leftover)
[ -n "$left" ] && log "runs left from before: $left"

sched_epoch=$(date +%s)
start "$SCHEDULER" || fail "the scheduler did not start"
wait_healthy "$SCHEDULER" 300 || fail "the scheduler did not become healthy"

# The scheduler works out each job's next run when it reads the job's file.
waited=0
while [ "$(helper unparsed "$sched_epoch" "${CRON_DAGS[@]}")" != "0" ]; do
  [ "$waited" -ge 600 ] && fail "the scheduler did not read every job within 10 minutes"
  sleep 15; waited=$((waited + 15))
done
due=" $(helper due "${CRON_DAGS[@]}") "

# 3. What is about to run.
msg="Sunday run starting at $(date +%H:%M). In this order:"
n=0; notdue=()
for j in "${JOBS[@]}"; do
  if [[ "$due" == *" $(dag_of "$j") "* ]]; then n=$((n + 1)); msg+=$'\n'"$n. $(label_of "$j")"; else notdue+=("$(label_of "$j")"); fi
done
[ "$n" = 0 ] && msg+=$'\n'"Nothing is due."
if [ "${#notdue[@]}" -gt 0 ]; then
  notdue_list=$(printf '%s, ' "${notdue[@]}"); msg+=$'\n'"Not due this week: ${notdue_list%, }."
fi
msg+=$'\n'"It stops by ${STOP_AT}:00 at the latest. Then everything old but the database switches off."
say "$msg" && START_SENT=sent

# 4. Each due job in turn.
REPORT=()
for j in "${JOBS[@]}"; do
  d=$(dag_of "$j"); l=$(label_of "$j")
  if [[ "$due" != *" $d "* ]]; then REPORT+=("$d|$l|notdue"); continue; fi
  if [ "$CUT" = 1 ] || [ "$(date +%s)" -ge "$stop_epoch" ]; then CUT=1; REPORT+=("$d|$l|cut"); continue; fi
  t=$(date +%s)
  log "running $d"
  helper unpause "$d" >/dev/null
  waited=0
  while [ "$(helper started "$d" "$t")" = "0" ] && [ "$waited" -lt 300 ]; do sleep 15; waited=$((waited + 15)); done
  while [ "$(helper active)" != "0" ]; do
    if [ "$(date +%s)" -ge "$stop_epoch" ]; then CUT=1; log "${STOP_AT}:00 reached while $d runs"; break; fi
    sleep "$POLL"
  done
  helper pause "$d" >/dev/null
  REPORT+=("$d|$l|ran")
  log "done waiting for $d"
done
for j in "${TRIGGERED[@]}"; do REPORT+=("$(dag_of "$j")|$(label_of "$j")|triggered"); done

# 5. How it ended.
rep=$(docker exec -i "$WEBSERVER" python3 - report "$START_EPOCH" "$(date +%H:%M)" "$CUT" "${REPORT[@]}" <<<"$HELPER_PY")
JOBS_JSON=$(printf '%s\n' "$rep" | sed -n 's/^JSON://p' | tail -1)
[ -n "$JOBS_JSON" ] || JOBS_JSON="[]"
end_text=$(printf '%s\n' "$rep" | sed '1,/^---TEXT---$/d')
[ -n "$end_text" ] || end_text="Sunday run finished at $(date +%H:%M), but its report could not be read. Everything old but the database switches off now."
say "$end_text" && END_SENT=sent

# 6. Everything old but the database stops.
stop_old
DONE=1
write_summary
log "Sunday run ends"
