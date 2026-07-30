#!/usr/bin/env bash
# Resilient launcher for the Shiny dashboard on port 3838.
#   - app.R runs in a restart loop, so a crash or OOM-kill self-heals.
#   - a watchdog restarts app.R if 3838 stops responding, so a hang self-heals.
# DB-level protection (connect_timeout + statement_timeout) lives in app.R's
# get_con() helper; this script is the process-level backstop.
set -u

run_app() {
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

watchdog() {
  local fails=0
  sleep 120          # grace period for the initial data load
  while true; do
    sleep 30
    if curl -fsS --max-time 10 http://localhost:3838/ >/dev/null 2>&1; then
      fails=0
    else
      fails=$((fails + 1))
      echo "[watchdog] 3838 unresponsive (${fails}/3)"
      if [ "$fails" -ge 3 ]; then
        echo "[watchdog] restarting app.R"
        python3 /opt/airflow/scripts/shiny_monitor.py record \
          --kind watchdog --note "3838 unresponsive x${fails}" \
          --logfile /opt/airflow/scripts/shiny.log \
          --events /opt/airflow/scripts/shiny_events.log 2>/dev/null || true
        for p in /proc/[0-9]*; do
          grep -qa "file=/opt/airflow/scripts/app.R" "$p/cmdline" 2>/dev/null \
            && kill -TERM "${p##*/}" 2>/dev/null
        done
        fails=0
        sleep 60       # let it come back before checking again
      fi
    fi
  done
}

run_app &
watchdog &
wait
