#!/bin/bash
# AlphaStream nightly database backup to iCloud Drive (added 2026-11-04).
# Since 2026-11-02 the Mac runs the live system, and nothing was backed up off it.
#
# Each run writes one dated folder:
#   staging.dump           the live database (pg_dump custom format, compressed)
#   airflow_staging.dump   Airflow's run history
#   globals.sql            roles and grants
#   SHA256SUMS             checksums of the files above
# The dumps are written inside the database container to /dumps, the bind mount of
# ~/alphastream-dumps, so nothing streams through docker exec. Each dump is read back
# with pg_restore --list before the folder moves into iCloud in one step.
# Keeps the newest KEEP runs (default 7). Older runs are evicted from the Mac once
# iCloud holds them, so the laptop stays lean while iCloud keeps every copy.
# Failures alert through Airflow's notify() (creds stay in Airflow Variables, no
# secrets on the host), the same path pipeline_watch.sh uses.
# Run by launchd (com.alphastream.db-backup, plist in tools/launchd/) daily at 12:30,
# after the nightly chain has finished (its last DAG ends about 11:00).
# Restore into an empty database:
#   pg_restore -U postgres -d <empty db> --jobs 4 staging.dump
set -uo pipefail

CONTAINER=${CONTAINER:-staging-postgres}
ALERT_VIA=${ALERT_VIA:-airflow-staging-scheduler}
HOST_DIR=${HOST_DIR:-$HOME/alphastream-dumps}
CONT_DIR=${CONT_DIR:-/dumps}
DEST=${DEST:-$HOME/Library/Mobile Documents/com~apple~CloudDocs/Backups/alphastream-db}
KEEP=${KEEP:-7}
MIN_FREE_GB=${MIN_FREE_GB:-10}
DBS=${DBS:-"staging airflow_staging"}
DISK=${DISK:-/System/Volumes/Data}
LAST_OK=${LAST_OK:-$HOME/Library/Logs/alphastream-db-backup.last_ok}

stamp=$(date +%Y-%m-%d_%H%M)
work=".inprogress-$stamp"
start=$(date +%s)
log() { echo "$(date '+%F %T') $*"; }

alert() {
  log "ALERT: $1"
  printf '%s' "$1" | docker exec -i "$ALERT_VIA" python3 -c '
import sys; sys.path.insert(0, "/opt/airflow/dags")
from utils.alerting import notify
sys.exit(0 if notify(sys.stdin.read()) else 1)' >/dev/null 2>&1 || log "ALERT NOT DELIVERED"
}
cleanup() { docker exec "$CONTAINER" rm -rf "$CONT_DIR/$work" >/dev/null 2>&1; }
fail() { alert "Check the database backup on the Mac: $1"; cleanup; exit 1; }

# One run at a time. A lock older than 6 hours is left over from a killed run.
LOCK=/tmp/alphastream-db-backup.lock
if [ -d "$LOCK" ] && [ $(( start - $(stat -f %m "$LOCK") )) -gt 21600 ]; then rmdir "$LOCK"; fi
if ! mkdir "$LOCK" 2>/dev/null; then log "SKIP another backup is running"; exit 0; fi
trap 'rmdir "$LOCK" 2>/dev/null' EXIT

docker exec "$CONTAINER" true >/dev/null 2>&1 || fail "the database container is not running"
free_gb=$(df -k "$DISK" | awk 'NR==2 {printf "%d", $4/1048576}')
[ "$free_gb" -ge "$MIN_FREE_GB" ] || fail "only ${free_gb} GB free on the disk, so no backup ran"

# 1. Dump each database inside the container and read each dump back
docker exec "$CONTAINER" mkdir -p "$CONT_DIR/$work" || fail "the work folder could not be created"
for db in $DBS; do
  docker exec "$CONTAINER" pg_dump -U postgres -d "$db" -Fc -f "$CONT_DIR/$work/$db.dump" \
    || fail "the dump of $db failed"
  toc=$(docker exec "$CONTAINER" pg_restore -l "$CONT_DIR/$work/$db.dump" 2>/dev/null) \
    || fail "the $db dump cannot be read back"
  data=$(printf '%s\n' "$toc" | grep -c ' TABLE DATA ')
  tables=$(docker exec "$CONTAINER" psql -U postgres -d "$db" -Atc "select count(*) from pg_class c
    join pg_namespace n on n.oid = c.relnamespace where c.relkind = 'r'
    and n.nspname not in ('pg_catalog', 'information_schema') and n.nspname not like 'pg_toast%'")
  [ "$data" -ge $(( tables * 9 / 10 )) ] || fail "the $db dump is missing tables"
  log "dumped $db: $data tables of data"
done
docker exec "$CONTAINER" pg_dumpall -U postgres --globals-only -f "$CONT_DIR/$work/globals.sql" \
  || fail "the dump of roles failed"

# 2. On the Mac: size check against the last backup, then checksums
src="$HOST_DIR/$work"
[ -s "$src/staging.dump" ] || fail "the dump folder is not visible on the Mac"
new_sz=$(stat -f %z "$src/staging.dump")
prev=$(ls -1d "$DEST"/20* 2>/dev/null | sort | tail -1)
if [ -n "$prev" ] && [ -f "$prev/staging.dump" ]; then
  old_sz=$(stat -f %z "$prev/staging.dump")
  [ "$new_sz" -ge $(( old_sz / 2 )) ] || fail "the live database dump shrank by more than half since the last backup"
fi
(cd "$src" && shasum -a 256 ./*.dump globals.sql > SHA256SUMS) || fail "the checksums could not be written"

# 3. Move the finished folder into iCloud in one step (same disk, so a rename)
if mkdir -p "$DEST" 2>/dev/null && mv "$src" "$DEST/$stamp" 2>/dev/null; then
  final="$DEST/$stamp"
else
  mv "$src" "$HOST_DIR/$stamp" || fail "the finished backup could not be moved out of the work folder"
  final="$HOST_DIR/$stamp"
  alert "Check the database backup on the Mac: it is saved on the Mac only because iCloud refused it. Give bash Full Disk Access in Privacy settings."
  ls -1d "$HOST_DIR"/20* 2>/dev/null | sort -r | tail -n +3 | while IFS= read -r d; do rm -rf "$d"; done
fi

# 4. Keep the newest KEEP runs. Evict older ones from the Mac (iCloud keeps them;
#    brctl refuses a file that has not finished uploading, so it is tried again tomorrow).
ls -1d "$DEST"/20* 2>/dev/null | sort -r | tail -n +$(( KEEP + 1 )) | while IFS= read -r d; do rm -rf "$d"; done
ls -1d "$DEST"/20* 2>/dev/null | sort -r | tail -n +2 | while IFS= read -r d; do
  for f in "$d"/*.dump; do brctl evict "$f" >/dev/null 2>&1; done
done

date +%s > "$LAST_OK"
log "OK $(du -sh "$final" | cut -f1) in $(( ($(date +%s) - start) / 60 )) min: $final"
