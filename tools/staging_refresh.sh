#!/usr/bin/env bash
# Staging starts as a copy of prod. The staging database on the staging-postgres container (Postgres 18.6, the same
# as prod; docker/staging-postgres) is loaded from a full copy of prod, then kept current by its own scheduler.
# Once a week a sync compares the price tables with prod day by day, by row count and a hash of the rows. Days
# that match are left alone. Days that differ or are missing are copied from prod, and nothing else.
#   tools/staging_refresh.sh on       a timer on Saturdays at 13:00 (DAY=1..7 with Monday 1, and AT=HH:MM, move it)
#   tools/staging_refresh.sh off      remove the timer
#   tools/staging_refresh.sh now      start the timed job now, as the timer would
#   tools/staging_refresh.sh run      what the timer runs: the sync; FORCE=1 makes it a full copy instead
#   tools/staging_refresh.sh sync     compare the price tables with prod day by day and copy the days that differ; DRY_RUN=1 only reports
#   tools/staging_refresh.sh copy     replace staging with a fresh full copy of prod now, by hand (an hour of downtime)
#   tools/staging_refresh.sh status   the copy's age, the newest dump and the last sync
#   tools/staging_refresh.sh switch   one time: move the Homebrew server's other databases here, retire it, take its port
#   tools/staging_refresh.sh airflow  set up and start the staging scheduler (docker/airflow-staging); safe to rerun
#   tools/staging_refresh.sh load     load the newest dump already in STAGING_DUMP_DIR, without reading prod (a new Mac)
#
# Between syncs staging runs nothing on its own: its scheduler has schedules switched off, so only a DAG started
# by hand runs there. The sync keeps the price tables equal to prod; the rest stays as of the last full copy.
# Why Saturday 13:00: prod's chain has landed Friday's session by noon and prod stays still all weekend, so the
# copy is complete before Sunday's 14:00 conformance run checks it.
# Prod is only read. pg_dump runs inside the container as Postgres 18 (the Mac's pg_dump is 14 and cannot dump an 18
# server), with --lock-wait-timeout so it gives up rather than queue behind a job's DDL. It never starts while prod's
# daily chain runs (03:00 to 12:00 here) or on the 1st and 2nd, when the monthly replay runs; a copy asked for then
# stops and says so. Passwords reach the container by name (docker exec -e NAME), never on a command line.
# Not copied: the schemas left over from past migrations (*_deprecated_*, *_remint_*).
# Only the newest dump is kept. It is a copy of prod outside DigitalOcean, as old as the last full copy.
# While a copy loads, staging is down (about an hour); the Mac has no room for two copies.
# The body sits in one block, so bash reads all of it before running: an edit during a run cannot change it.
{
set -euo pipefail
cd "$(dirname "$0")/.."
here="$(pwd -P)"
compose=(docker compose -f docker/staging-postgres/docker-compose.yml)
c=staging-postgres
db=staging
week=$((6 * 86400 + 20 * 3600))     # copy again once the copy is this old: weekly, near the same hour
label=com.alphastream.staging-refresh
plist="$HOME/Library/LaunchAgents/$label.plist"
domain="gui/$(id -u)"

set -a; . docker/staging-postgres/.env; set +a        # POSTGRES_PASSWORD, STAGING_DUMP_DIR, STAGING_PORT
: "${STAGING_DUMP_DIR:?set STAGING_DUMP_DIR in docker/staging-postgres/.env}"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*"; }
sql() { docker exec "$c" psql -U postgres -d "${2:-postgres}" -v ON_ERROR_STOP=1 -Atqc "$1"; }

up() {
  "${compose[@]}" up -d >/dev/null 2>&1
  for _ in $(seq 1 60); do
    docker exec "$c" pg_isready -U postgres -d postgres -q 2>/dev/null && return 0
    sleep 2
  done
  log "staging-postgres did not become ready"
  return 1
}

copied_at() {   # epoch seconds of the last copy, 0 when there is none
  local note
  note=$(sql "select shobj_description(oid, 'pg_database') from pg_database where datname = '$db'" 2>/dev/null || true)
  python3 -c 'import json, sys
try: print(int(json.loads(sys.argv[1])["copied_at_epoch"]))
except Exception: print(0)' "$note"
}

prod_busy() {   # the daily chain runs 04:00 to about 11:00 here; the monthly replay starts on the 1st
  local h d
  h=$((10#$(date +%H))); d=$((10#$(date +%d)))
  { [ "$h" -ge 3 ] && [ "$h" -lt 12 ]; } || [ "$d" -le 2 ]
}

prod() {        # run a command in the container with prod's login, handed over by name
  local env_file="${PROD_ENV_FILE:-docker/airflow/.env}"
  (
    set -a; . <(grep -E '^PROD_DB_(HOST|PORT|USER|PASSWORD|NAME)=' "$env_file"); set +a
    : "${PROD_DB_HOST:?}" "${PROD_DB_PORT:?}" "${PROD_DB_USER:?}" "${PROD_DB_PASSWORD:?}"
    PGHOST=$PROD_DB_HOST PGPORT=$PROD_DB_PORT PGUSER=$PROD_DB_USER PGPASSWORD=$PROD_DB_PASSWORD \
    PGDATABASE=${PROD_DB_NAME:-prod} PGSSLMODE=require \
      docker exec -e PGHOST -e PGPORT -e PGUSER -e PGPASSWORD -e PGDATABASE -e PGSSLMODE "$c" "$@"
  )
}

prod_chain_busy() {   # prod's daily chain writes prices from 03:00 to about 12:00 here
  local h
  h=$((10#$(date +%H)))
  [ "$h" -ge 3 ] && [ "$h" -lt 12 ]
}

copy_running() {      # a full copy is dumping or loading now (or a failed dump left a .partial behind)
  docker exec "$c" sh -c 'ls -d /dumps/*.partial >/dev/null 2>&1' && return 0
  [ "$(sql "select count(*) from pg_stat_activity where application_name in ('pg_restore', 'vacuumdb')")" != 0 ]
}

# The price tables the weekly sync keeps equal to prod, day by day.
sync_tables=(raw.api_data_ingestion_massive raw.api_data_ingestion_massive_delisted cdm.ingest_combined)

sync() {        # copy from prod the days staging lacks or holds differently, and nothing else
  up
  local sdb="${SYNC_DB:-$db}" tmp t cols hcols q n first last rc=0
  if [ "$(sql "select count(*) from pg_database where datname = '$sdb'")" != 1 ]; then
    log "sync: there is no $sdb database yet; load a copy first: tools/staging_refresh.sh copy"; return 1
  fi
  if [ "$sdb" = "$db" ] && copy_running; then
    log "sync: skipped, a full copy is dumping or loading (or a failed dump left a .partial in STAGING_DUMP_DIR)"
    return 0
  fi
  while prod_chain_busy; do       # a run that starts inside prod's chain (the Mac woke late) waits for it to end
    log "sync: prod's daily chain may be writing prices now; waiting until 12:00"; sleep 1800
  done
  tmp=$(mktemp -d)
  for t in "${sync_tables[@]}"; do
    cols=$(sql "select string_agg(quote_ident(column_name), ',' order by ordinal_position)
                from information_schema.columns
                where table_schema = split_part('$t', '.', 1) and table_name = split_part('$t', '.', 2)" "$sdb")
    if [ -z "$cols" ]; then log "sync: $t is not on $sdb"; rc=1; continue; fi
    # One line per day: its rows and a hash of their content. processed_at is left out: it is load time.
    hcols=$(tr ',' '\n' <<< "$cols" | grep -vx processed_at | paste -sd, -)
    q="select date, count(*), sum(hashtext(row($hcols)::text)) from $t group by date"
    if ! prod psql -v ON_ERROR_STOP=1 -Atqc "$q" > "$tmp/prod"; then log "sync: could not read $t on prod"; rc=1; continue; fi
    if ! sql "$q" "$sdb" > "$tmp/staging"; then log "sync: could not read $t on $sdb"; rc=1; continue; fi
    # The days prod has that staging lacks or holds differently, with prod's row count for each.
    awk -F'|' 'FILENAME == ARGV[1] {s[$1] = $2 "|" $3; next} s[$1] != $2 "|" $3 {print $1 "|" $2}' \
      "$tmp/staging" "$tmp/prod" > "$tmp/days"
    n=$(wc -l < "$tmp/days" | tr -d ' ')
    if [ "$n" = 0 ]; then log "sync: $t matches prod"; continue; fi
    first=$(cut -d'|' -f1 "$tmp/days" | sort | head -1); last=$(cut -d'|' -f1 "$tmp/days" | sort | tail -1)
    if [ "${DRY_RUN:-0}" = 1 ]; then
      log "sync: $t differs from prod on $n days, $first to $last; DRY_RUN, nothing copied"; continue
    fi
    if copy_days "$t" "$sdb" "$cols" "$tmp"; then
      log "sync: $t: copied $(awk -F'|' '{r += $2} END {print r + 0}' "$tmp/days") rows for $n days from prod, $first to $last"
    else
      log "sync: copying $t failed; staging keeps its rows"; rc=1
    fi
  done
  rm -rf "$tmp"
  return "$rc"
}

copy_days() {   # replace the days listed in $4/days in table $1 of database $2 with prod's rows for them
  local t=$1 sdb=$2 cols=$3 dir=$4 list want got
  list="'{$(cut -d'|' -f1 "$dir/days" | paste -sd, -)}'::date[]"
  want=$(awk -F'|' '{r += $2} END {print r + 0}' "$dir/days")
  prod psql -v ON_ERROR_STOP=1 -Atqc "copy (select $cols from $t where date = any($list)) to stdout" > "$dir/rows" || return 1
  got=$(wc -l < "$dir/rows" | tr -d ' ')
  # Prod changed between the count and the copy, or the copy broke off: change nothing.
  [ "$got" = "$want" ] || { log "sync: prod sent $got rows of $t, expected $want"; return 1; }
  # One transaction: staging's rows for those days go and prod's arrive, or nothing changes.
  docker exec -i "$c" psql -U postgres -d "$sdb" -v ON_ERROR_STOP=1 -q \
    -c "begin" \
    -c "create temp table incoming (like $t) on commit drop" \
    -c "copy incoming ($cols) from stdin" \
    -c "delete from $t where date = any($list)" \
    -c "insert into $t ($cols) select $cols from incoming" \
    -c "commit" < "$dir/rows"
}

refresh() {
  if prod_busy; then log "skipped: prod's jobs may be running now (03:00 to 12:00, and the 1st and 2nd); copy later"; return 0; fi
  up
  local age=$(( $(date +%s) - $(copied_at) ))
  if [ "${FORCE:-0}" != 1 ] && [ "$age" -lt "$week" ]; then
    log "skipped: the copy is $((age / 86400)) days old"; return 0
  fi
  local free_gb
  free_gb=$(df -g "$STAGING_DUMP_DIR" | awk 'NR == 2 {print $4}')
  if [ "$free_gb" -lt 25 ]; then log "stopped: $free_gb GB free on the Mac; a copy needs 25"; return 1; fi

  local stamp out version t0=$SECONDS
  stamp=$(date -u +%Y%m%dT%H%MZ); out="/dumps/prod-$stamp"
  version=$(prod psql -v ON_ERROR_STOP=1 -Atqc "show server_version")
  log "dumping prod ($version) to $STAGING_DUMP_DIR/prod-$stamp"
  prod pg_dump --format=directory --jobs=4 --compress=zstd:3 --lock-wait-timeout=60s \
               --exclude-schema='*_deprecated_*' --exclude-schema='*_remint_*' --file="$out.partial"
  docker exec "$c" mv "$out.partial" "$out"
  log "dumped in $(( (SECONDS - t0) / 60 )) min, $(du -sh "$STAGING_DUMP_DIR/prod-$stamp" | cut -f1)"
  load_dump "prod-$stamp" "$version"
}

load_dump() {   # replace staging with the dump /dumps/$1 (taken from prod $2), then keep only that dump
  local name=$1 version=$2 out="/dumps/$1" t0=$SECONDS
  log "loading $name into $db"
  # The staging scheduler pauses while its database is replaced, and comes back after, even on failure.
  local sched=""
  if [ "$(docker inspect -f '{{.State.Running}}' airflow-staging-scheduler 2>/dev/null)" = true ]; then
    sched=airflow-staging-scheduler; docker stop "$sched" >/dev/null
    trap 'docker start airflow-staging-scheduler >/dev/null 2>&1 || true' EXIT
  fi
  sql "drop database if exists $db with (force)"
  sql "create database $db"
  docker exec "$c" pg_restore -U postgres -d "$db" --jobs=4 --no-owner --no-acl --exit-on-error "$out"
  docker exec "$c" vacuumdb -U postgres -d "$db" --analyze-only --jobs=4 --quiet
  [ -z "$sched" ] || docker start "$sched" >/dev/null
  # The copy's time is the dump's, which its name carries (prod-YYYYMMDDTHHMMZ, UTC).
  local note
  note=$(python3 -c 'import calendar, json, sys, time
t = time.strptime(sys.argv[1], "prod-%Y%m%dT%H%MZ")
print(json.dumps({"copied_from": "prod", "copied_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", t),
                  "copied_at_epoch": calendar.timegm(t), "dump": sys.argv[1], "prod_version": sys.argv[2],
                  "not_copied": ["*_deprecated_*", "*_remint_*"]}))' "$name" "$version")
  sql "comment on database $db is '$note'"
  # Keep only this dump, and clear any dump a failed run left behind.
  docker exec "$c" sh -c 'cd /dumps && for d in prod-*; do [ "$d" = "$1" ] || rm -rf "$d"; done' sh "$name"
  log "done: $db is prod as of $name, loaded in $(( (SECONDS - t0) / 60 )) min"
}

load_newest() { # a new Mac whose STAGING_DUMP_DIR (an external drive) already holds a dump: no read of prod
  up
  local name version
  name=$(docker exec "$c" sh -c 'cd /dumps && ls -1d prod-* 2>/dev/null | grep -v partial | sort | tail -1')
  [ -n "$name" ] || { log "no dump in $STAGING_DUMP_DIR"; return 1; }
  version=$(docker exec "$c" pg_restore -l "/dumps/$name" | sed -n 's/^; *Dumped from database version: *//p' | head -1)
  load_dump "$name" "${version:-unknown}"
}

switch() {
  # The Homebrew postgresql@14 server on 5432 holds the old staging (stale since July) and dev, airflow_db and
  # board_test. Those three move here with their owners; the old staging is dropped; Homebrew stops; this server
  # takes port 5432; then the first copy of prod loads. Homebrew's files stay on disk until deleted by hand.
  [ "${STAGING_PORT:-5432}" != 5432 ] || { echo "set STAGING_PORT=5433 in docker/staging-postgres/.env for the switch" >&2; exit 2; }
  pg_isready -h 127.0.0.1 -p 5432 -q || { echo "no Homebrew server on 5432; nothing to switch" >&2; exit 2; }
  up
  export PGPASSWORD="$POSTGRES_PASSWORD"
  old() { docker exec -e PGPASSWORD "$c" psql -h host.docker.internal -p 5432 -U postgres -d "${2:-postgres}" -v ON_ERROR_STOP=1 -Atqc "$1"; }
  local tables="select count(*) from information_schema.tables where table_schema not in ('pg_catalog', 'information_schema')"
  sql "do \$\$ begin if not exists (select from pg_roles where rolname = 'kevin') then create role kevin login superuser; end if; end \$\$"
  for d in dev airflow_db board_test; do
    if [ -n "$(sql "select 1 from pg_database where datname = '$d'")" ]; then log "$d is already here"; continue; fi
    log "moving $d"
    docker exec -e PGPASSWORD "$c" sh -c \
      'set -e; pg_dump -h host.docker.internal -p 5432 -U postgres -Fc "$1" > /tmp/move.dump; pg_restore -U postgres --create --exit-on-error -d postgres /tmp/move.dump; rm /tmp/move.dump' sh "$d"
    [ "$(old "$tables" "$d")" = "$(sql "$tables" "$d")" ] || { log "$d: table counts differ after the move; stopping"; exit 1; }
  done
  log "dropping the old staging on Homebrew"
  old "drop database if exists staging with (force)"
  log "stopping Homebrew's postgresql@14 (its files stay on disk)"
  brew services stop postgresql@14 >/dev/null
  sed -i '' 's/^STAGING_PORT=.*/STAGING_PORT=5432/' docker/staging-postgres/.env
  export STAGING_PORT=5432
  "${compose[@]}" up -d >/dev/null 2>&1
  up
  log "staging-postgres now answers on 127.0.0.1:5432"
  FORCE=1 refresh
}

staging_airflow() {
  # The staging scheduler: its own metadata database, settings holding staging's values only, no schedules, and
  # only the price and model chain switched on so they can be started by hand. Safe to rerun; it keeps an existing .env.
  local dir=docker/airflow-staging ac=(docker compose -f docker/airflow-staging/docker-compose.yml)
  up
  [ -n "$(sql "select 1 from pg_database where datname = 'airflow_staging'")" ] || sql "create database airflow_staging"
  if [ ! -f "$dir/.env" ]; then
    ( umask 077
      massive=$(grep -E '^MASSIVE_API_KEY=' docker/airflow/.env | head -1 | cut -d= -f2-)
      python3 - "$dir/.env" "$POSTGRES_PASSWORD" "$massive" <<'PY'
import secrets, sys
path, pw, massive = sys.argv[1:4]
open(path, "w").write(f"STAGING_DB_PASSWORD={pw}\nMASSIVE_API_KEY={massive}\n"
                      f"AIRFLOW_SECRET_KEY={secrets.token_hex(32)}\nAIRFLOW_UI_PASSWORD={secrets.token_urlsafe(12)}\n")
PY
    )
    log "wrote $dir/.env (staging's values only)"
  fi
  if grep -q -E '^(PROD_DB_|DATABASE_URL_PROD)' "$dir/.env"; then echo "$dir/.env names prod; refusing" >&2; exit 1; fi
  "${ac[@]}" up -d
  for _ in $(seq 1 90); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' airflow-staging-scheduler 2>/dev/null)" = healthy ] && break
    sleep 4
  done
  ( set -a; . "$dir/.env"; set +a
    docker exec -e AIRFLOW_UI_PASSWORD airflow-staging-webserver sh -c \
      'airflow users list -o plain 2>/dev/null | grep -qw kevin || airflow users create -r Admin -u kevin -f Kevin -l Staging -e kevin@localhost -p "$AIRFLOW_UI_PASSWORD"' >/dev/null )
  # The scheduler parses the DAG files a little after it starts; wait for each before switching it on.
  for d in raw_ingest_massive_daily cdm_ingest_massive metrics_dbt_models inference_dbt_models inference_backtest_dbt_models; do
    for _ in $(seq 1 45); do
      docker exec airflow-staging-scheduler airflow dags unpause "$d" >/dev/null 2>&1 && { log "on: $d"; continue 2; }
      sleep 4
    done
    log "could not switch on $d; see the staging Airflow UI"
  done
  log "staging Airflow at http://localhost:8082 (user kevin, password in $dir/.env)"
}

case "${1:-status}" in
  airflow)
    staging_airflow ;;
  load)
    load_newest ;;
  run)
    mkdir -p "$STAGING_DUMP_DIR"
    if [ "${FORCE:-0}" = 1 ]; then refresh; else sync; fi ;;
  sync)
    sync ;;
  copy)
    mkdir -p "$STAGING_DUMP_DIR"
    FORCE=1 refresh ;;
  switch)
    switch ;;
  status)
    a=$(copied_at)
    if [ "$a" = 0 ]; then echo "no copy yet"; else echo "copied $(( ($(date +%s) - a) / 3600 )) hours ago"; fi
    ls -d "$STAGING_DUMP_DIR"/prod-* 2>/dev/null | tail -1 | xargs -I{} du -sh {} || echo "no dump"
    grep 'sync: ' "$STAGING_DUMP_DIR/refresh.log" 2>/dev/null | tail -1 || echo "no sync yet" ;;
  on)
    at="${AT:-13:00}"; day="${DAY:-6}"
    case "$at" in [0-9][0-9]:[0-5][0-9]) ;; *) echo "AT must be HH:MM" >&2; exit 2 ;; esac
    case "$day" in [1-7]) ;; *) echo "DAY must be 1 (Monday) to 7 (Sunday)" >&2; exit 2 ;; esac
    days=(- Mondays Tuesdays Wednesdays Thursdays Fridays Saturdays Sundays)
    hour=$((10#${at%:*})); minute=$((10#${at#*:}))
    mkdir -p "$(dirname "$plist")" "$STAGING_DUMP_DIR"
    cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array><string>$here/tools/staging_refresh.sh</string><string>run</string></array>
  <key>WorkingDirectory</key><string>$here</string>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
  <key>StartCalendarInterval</key>
  <dict><key>Weekday</key><integer>$day</integer><key>Hour</key><integer>$hour</integer><key>Minute</key><integer>$minute</integer></dict>
  <key>StandardOutPath</key><string>$STAGING_DUMP_DIR/refresh.log</string>
  <key>StandardErrorPath</key><string>$STAGING_DUMP_DIR/refresh.log</string>
</dict>
</plist>
EOF
    launchctl bootout "$domain/$label" 2>/dev/null || true
    launchctl bootstrap "$domain" "$plist"
    printf 'timer on: %s at %02d:%02d, checking staging against prod and backfilling any missing day\n' \
      "${days[$day]}" "$hour" "$minute" ;;
  off)
    launchctl bootout "$domain/$label" 2>/dev/null || true
    rm -f "$plist"
    echo "timer off" ;;
  now)
    launchctl kickstart "$domain/$label"
    echo "started. Follow it in $STAGING_DUMP_DIR/refresh.log" ;;
  *) echo "usage: tools/staging_refresh.sh [run|sync|copy|status|on|off|now|switch|airflow|load]" >&2; exit 2 ;;
esac
exit 0
}
