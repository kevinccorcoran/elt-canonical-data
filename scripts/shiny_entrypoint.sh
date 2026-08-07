#!/usr/bin/env bash
# Resilient launcher for the Shiny dashboard.
#   - app.R runs in a restart loop, so a crash or OOM-kill self-heals.
#   - a watchdog restarts app.R ONLY when it is genuinely hung, so it self-heals
#     without killing a healthy-but-busy process (see the watchdog note below).
# DB-level protection (connect_timeout + statement_timeout) lives in app.R's
# get_con() helper; this script is the process-level backstop.
# Port is env-overridable (SHINY_PORT), matching app.R's runApp default of 3838.
set -u
PORT="${SHINY_PORT:-3838}"

# Preflight: the r-cran-shiny apt bundle's event-loop stack (httpuv <= 1.6.9 /
# later 1.3.0) segfaults R in execCallbacks under reconnect churn. The
# Dockerfile installs a fixed stack from CRAN; if a stale image ever serves the
# old ABI again, say so LOUDLY on every start instead of crashing in silence.
preflight() {
  local ver
  ver=$(Rscript -e 'cat(as.character(packageVersion("httpuv")), as.character(packageVersion("later")))' 2>/dev/null)
  echo "[preflight] event-loop stack: httpuv/later = ${ver:-unknown}"
  case "${ver%% *}" in
    1.6.[0-9]|1.[0-5].*)
      echo "[preflight] ERROR: stale httpuv (${ver%% *}) - known segfault ABI (crashes under reconnect churn). Rebuild the image: cd docker/airflow && docker compose build && docker compose up -d --no-deps airflow-shiny" ;;
  esac
}

# Total CPU ticks (utime+stime) burned by the running app.R R process. Used by
# the watchdog to tell "busy computing" (ticks rising) from "hung" (ticks flat).
# Matches the R exec process by its --file= arg (NOT the bash entrypoint, whose
# cmdline also mentions app.R). Empty output => no such process (down/restarting).
app_cpu_ticks() {
  local total=0 p st rest
  for p in /proc/[0-9]*; do
    grep -qa "file=/opt/airflow/scripts/app.R" "$p/cmdline" 2>/dev/null || continue
    st=$(cat "$p/stat" 2>/dev/null) || continue
    rest=${st#*) }                         # drop 'pid (comm) '; comm may hold spaces
    set -- $rest                            # $1=state(f3) .. $12=utime(f14) $13=stime(f15)
    total=$(( total + ${12:-0} + ${13:-0} ))
  done
  [ "$total" -gt 0 ] && echo "$total"
}

run_app() {
  preflight
  while true; do
    echo "[supervisor] starting app.R"
    Rscript /opt/airflow/scripts/app.R > /opt/airflow/scripts/shiny.log 2>&1
    code=$?
    echo "[supervisor] app.R exited (code $code), restarting in 3s"
    # shiny.log is truncated on every start; keep the last crashed run's log
    if [ "$code" -ne 0 ]; then
      cp /opt/airflow/scripts/shiny.log /opt/airflow/scripts/shiny_crash_last.log 2>/dev/null
    fi
    # durable, classified restart history (best-effort; must never break the loop)
    python3 /opt/airflow/scripts/shiny_monitor.py record \
      --kind supervisor --exit-code "$code" \
      --logfile /opt/airflow/scripts/shiny.log \
      --events /opt/airflow/scripts/shiny_events.log 2>/dev/null || true
    sleep 3
  done
}

# The dashboard is single-threaded: while it runs the Lifecycle query bundle
# (~9s) and then synchronously renders the board + ~10 plotly charts + tables, it
# CANNOT answer this HTTP poll for tens of seconds, yet it is NOT hung. The old
# watchdog killed on 2 dead polls (~30s) and so killed a healthy Generate every
# time it ran long - the crash-loop Kevin kept hitting (ERR_EMPTY_RESPONSE).
#
# Two guards now distinguish busy from hung, and only a true hang is restarted:
#   1. CPU-progress: a dead poll whose R process burned CPU since the last poll
#      is WORKING (render/compute) -> defer, not a fail. Bounded by BUSY_LIMIT so
#      a pathological busy-loop still gets recycled.
#   2. Threshold: only FAIL_LIMIT consecutive dead polls with NO CPU progress
#      (i.e. truly stuck, e.g. an I/O deadlock) trigger a restart. FAIL_LIMIT x
#      ~25s/poll (~100s) exceeds app.R's 60s statement_timeout, so a stuck *query*
#      self-aborts and frees the thread BEFORE the process is ever killed.
watchdog() {
  local fails=0 busy=0 restart prev_cpu cpu
  local FAIL_LIMIT=4     # consecutive no-progress dead polls => treat as hung
  local BUSY_LIMIT=16    # hard cap (~7min) so even a busy-loop eventually recycles
  sleep 60               # grace period for the initial data load
  prev_cpu=$(app_cpu_ticks)
  while true; do
    sleep 15
    if curl -fsS --max-time 10 "http://localhost:${PORT}/" >/dev/null 2>&1; then
      fails=0; busy=0; prev_cpu=$(app_cpu_ticks)
      continue
    fi
    # Unresponsive. Busy (CPU rising) or hung (CPU flat / no process)?
    cpu=$(app_cpu_ticks)
    restart=0
    if [ -n "$cpu" ] && [ -n "$prev_cpu" ] && [ "$cpu" -gt "$prev_cpu" ]; then
      busy=$((busy + 1)); fails=0
      echo "[watchdog] ${PORT} busy - app working, deferring (${busy}/${BUSY_LIMIT})"
      [ "$busy" -ge "$BUSY_LIMIT" ] && restart=1
    else
      fails=$((fails + 1))
      echo "[watchdog] ${PORT} unresponsive, no CPU progress (${fails}/${FAIL_LIMIT})"
      [ "$fails" -ge "$FAIL_LIMIT" ] && restart=1
    fi
    prev_cpu=$cpu
    if [ "$restart" -eq 1 ]; then
      echo "[watchdog] restarting app.R (fails=${fails} busy=${busy})"
      python3 /opt/airflow/scripts/shiny_monitor.py record \
        --kind watchdog --note "${PORT} unresponsive fails=${fails} busy=${busy}" \
        --logfile /opt/airflow/scripts/shiny.log \
        --events /opt/airflow/scripts/shiny_events.log 2>/dev/null || true
      for p in /proc/[0-9]*; do
        grep -qa "file=/opt/airflow/scripts/app.R" "$p/cmdline" 2>/dev/null \
          && kill -TERM "${p##*/}" 2>/dev/null
      done
      fails=0; busy=0
      sleep 60             # let it come back before checking again
      prev_cpu=$(app_cpu_ticks)
    fi
  done
}

run_app &
watchdog &
wait
