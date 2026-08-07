#!/usr/bin/env bash
# Resilient launcher for the Shiny dashboard.
#   - app.R runs in a restart loop, so a crash or OOM-kill self-heals.
#   - the watchdog restarts app.R ONLY when it is genuinely hung; it never kills
#     a process that is busy working or still booting (that was the crash loop:
#     a segfault -> supervisor restart -> the old watchdog SIGTERM'd the booting
#     process -> browser reconnect churn -> another segfault).
# DB-level protection (connect_timeout + statement_timeout) lives in app.R's
# get_con(); this script is the process-level backstop.
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

# Identify the running app.R R process: echoes "<pid> <cpu_ticks>" (utime+stime),
# or nothing if it is not running (down / mid-restart). The watchdog uses the PID
# to tell a restart from a hang, and the ticks to tell "busy working" from "stuck".
# Matches the R exec by its --file= arg, NOT the bash entrypoint (whose cmdline
# also mentions app.R), and NOT PID 1.
app_proc() {
  local p st rest
  for p in /proc/[0-9]*; do
    grep -qa "file=/opt/airflow/scripts/app.R" "$p/cmdline" 2>/dev/null || continue
    st=$(cat "$p/stat" 2>/dev/null) || continue
    rest=${st#*) }                      # drop 'pid (comm) '; comm may contain spaces
    set -- $rest                         # $1=state(f3) .. $12=utime(f14) $13=stime(f15)
    echo "${p##*/} $(( ${12:-0} + ${13:-0} ))"
    return
  done
}

run_app() {
  preflight
  local backoff=3 up_since ran
  while true; do
    echo "[supervisor] starting app.R"
    up_since=$(date +%s 2>/dev/null || echo 0)
    Rscript /opt/airflow/scripts/app.R > /opt/airflow/scripts/shiny.log 2>&1
    code=$?
    ran=$(( $(date +%s 2>/dev/null || echo 0) - up_since ))
    [ "$ran" -ge 120 ] && backoff=3   # stayed up a sustained window => healthy, fast restart
    echo "[supervisor] app.R exited (code $code) after ${ran}s, restarting in ${backoff}s"
    # shiny.log is truncated on every start; keep the last crashed run's log
    if [ "$code" -ne 0 ]; then
      cp /opt/airflow/scripts/shiny.log /opt/airflow/scripts/shiny_crash_last.log 2>/dev/null
    fi
    # durable, classified restart history (best-effort; must never break the loop)
    python3 /opt/airflow/scripts/shiny_monitor.py record \
      --kind supervisor --exit-code "$code" \
      --logfile /opt/airflow/scripts/shiny.log \
      --events /opt/airflow/scripts/shiny_events.log 2>/dev/null || true
    sleep "$backoff"
    # exponential backoff so a boot-crash loop can't hammer prod (3->6->12->..->60)
    backoff=$(( backoff * 2 )); [ "$backoff" -gt 60 ] && backoff=60
  done
}

# The app is single-threaded: while it runs the Lifecycle query bundle + renders
# the board/plots/tables (and the portfolio DB queries), it cannot answer this
# HTTP poll for tens of seconds, yet it is NOT hung. And right after a
# segfault-restart it is busy booting. Killing it in either state is what caused
# the crash loop. So we distinguish, and only restart a process that is the SAME
# one as last poll, unresponsive, AND making no CPU progress, for FAIL_LIMIT polls.
# FAIL_LIMIT * ~25s (~100s) exceeds app.R's 60s statement_timeout, so a stuck
# *query* self-aborts and frees the thread before we would ever kill.
watchdog() {
  local fails=0 busy=0 restart line pid cpu prev_pid prev_cpu w
  local FAIL_LIMIT=4     # consecutive same-pid no-progress dead polls => hung
  local BUSY_LIMIT=20    # ~9min hard cap so even a CPU busy-loop eventually recycles
  sleep 60               # grace for the initial data load
  line=$(app_proc); prev_pid=${line%% *}; prev_cpu=${line##* }
  while true; do
    sleep 15
    if curl -fsS --max-time 10 "http://localhost:${PORT}/" >/dev/null 2>&1; then
      fails=0; busy=0
      line=$(app_proc); prev_pid=${line%% *}; prev_cpu=${line##* }
      continue
    fi
    line=$(app_proc); pid=${line%% *}; cpu=${line##* }
    restart=0
    if [ -z "$pid" ]; then
      # no app process: the supervisor is between restarts. Nothing to kill; wait.
      fails=$((fails + 1))
      echo "[watchdog] ${PORT} down, no app process (supervisor restarting) (${fails}/${FAIL_LIMIT})"
      [ "$fails" -ge "$FAIL_LIMIT" ] && restart=1
    elif [ "$pid" != "$prev_pid" ]; then
      # process restarted since last poll: it is booting, not hung. Reset + grace.
      echo "[watchdog] ${PORT} new app process ${pid} booting - grace"
      fails=0; busy=0
    elif [ -n "$cpu" ] && [ -n "$prev_cpu" ] && [ "$cpu" -gt "$prev_cpu" ]; then
      # actively burning CPU: a long render/query/boot, NOT hung. Defer.
      busy=$((busy + 1)); fails=0
      echo "[watchdog] ${PORT} busy - app working, deferring (${busy}/${BUSY_LIMIT})"
      [ "$busy" -ge "$BUSY_LIMIT" ] && restart=1
    else
      # same pid, unresponsive, no CPU progress: genuinely stuck.
      fails=$((fails + 1))
      echo "[watchdog] ${PORT} unresponsive, no CPU progress (${fails}/${FAIL_LIMIT})"
      [ "$fails" -ge "$FAIL_LIMIT" ] && restart=1
    fi
    prev_pid=$pid; prev_cpu=$cpu
    if [ "$restart" -eq 1 ]; then
      echo "[watchdog] restarting app.R (fails=${fails} busy=${busy} pid=${pid:-none})"
      python3 /opt/airflow/scripts/shiny_monitor.py record \
        --kind watchdog --note "${PORT} unresponsive fails=${fails} busy=${busy}" \
        --logfile /opt/airflow/scripts/shiny.log \
        --events /opt/airflow/scripts/shiny_events.log 2>/dev/null || true
      # Escalate against THIS captured pid only (never a fresh /proc glob, which
      # could hit a healthy replacement mid-restart): SIGTERM, wait up to 10s, then
      # SIGKILL, wait up to 5s. A process wedged in httpuv/libpq ignores a deferred
      # SIGTERM, so TERM alone can leave 3838 down forever.
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null
        w=0; while [ "$w" -lt 10 ] && kill -0 "$pid" 2>/dev/null; do sleep 1; w=$((w + 1)); done
        if kill -0 "$pid" 2>/dev/null; then
          echo "[watchdog] pid ${pid} ignored SIGTERM -> SIGKILL"
          kill -KILL "$pid" 2>/dev/null
          w=0; while [ "$w" -lt 5 ] && kill -0 "$pid" 2>/dev/null; do sleep 1; w=$((w + 1)); done
        fi
      fi
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        # survived SIGKILL (uninterruptible D-state / unreaped): do NOT pretend we
        # recovered - keep fails high, short-sleep, re-escalate next cycle.
        echo "[watchdog] pid ${pid} survived SIGKILL (stuck in kernel) - will re-escalate"
        fails=$FAIL_LIMIT; busy=0; sleep 10
      else
        # confirmed gone (or nothing to kill): the supervisor restarts it; grace.
        fails=0; busy=0; sleep 60
      fi
      line=$(app_proc); prev_pid=${line%% *}; prev_cpu=${line##* }
    fi
  done
}

run_app &
watchdog &
wait
