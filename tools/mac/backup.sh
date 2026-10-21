#!/usr/bin/env bash
# An encrypted backup of what a new Mac needs and git does not hold: every file in tools/mac/settings_files.txt and
# the small local databases (dev, airflow_db, board_test on staging-postgres). Staging itself is not in it: a new
# Mac rebuilds staging from prod (tools/staging_refresh.sh).
#   tools/mac/backup.sh init     store the backup passphrase in this Mac's Keychain; keep it in your password manager too
#   tools/mac/backup.sh run      write a backup now
#   tools/mac/backup.sh on       a weekly timer, Sundays at 20:00 (AT=HH:MM moves it); off removes it
#   tools/mac/backup.sh check    open the newest backup and confirm every listed file is inside
#   tools/mac/backup.sh restore  put the settings files back and load the databases (tools/mac/new_mac.sh calls it)
# Each backup is an AES-256 disk image in BACKUP_DIR (default: iCloud Drive/AlphaStream backups), so it lives off this
# Mac. On any Mac, double-click it and type the passphrase. A list of the file names inside (no values) sits next to
# each image for alphastream-conformance's weekly check. The newest three are kept.
# The body sits in one block, so bash reads all of it before running.
{
set -euo pipefail
umask 077
cd "$(dirname "$0")/../.."
canonical="$(pwd -P)"
repos="$(dirname "$canonical")"
backup_dir="${BACKUP_DIR:-$HOME/Library/Mobile Documents/com~apple~CloudDocs/AlphaStream backups}"
list=tools/mac/settings_files.txt
service="${BACKUP_SERVICE:-alphastream-backup}"          # the Keychain item holding the passphrase
read -r -a databases <<< "${BACKUP_DATABASES:-dev airflow_db board_test}"
label=com.alphastream.backup
plist="$HOME/Library/LaunchAgents/$label.plist"
domain="gui/$(id -u)"

passphrase() {
  security find-generic-password -s "$service" -a "$USER" -w 2>/dev/null \
    || { echo "no backup passphrase in the Keychain. Run: tools/mac/backup.sh init" >&2; exit 2; }
}
path_of() { case "$1" in "~/"*) echo "$HOME/${1#\~/}" ;; *) echo "$repos/$1" ;; esac; }
name_in_backup() { case "$1" in "~/"*) echo "settings/home/${1#\~/}" ;; *) echo "settings/$1" ;; esac; }
entries() { grep -v -E '^\s*(#|$)' "$list"; }
newest() { ls -1t "$backup_dir"/alphastream-*.dmg 2>/dev/null | head -1; }

mount_newest() {   # mount the newest image read-only at $1 and name it in $img; stop if it will not open
  local pass
  img=$(newest); [ -n "$img" ] || { echo "no backup in $backup_dir" >&2; exit 2; }
  pass=$(passphrase)    # $( ) drops the newline the Keychain adds; the image was made without one
  printf '%s' "$pass" | hdiutil attach -quiet -stdinpass -readonly -nobrowse -mountpoint "$1" "$img" \
    || { echo "could not open $img: wrong passphrase?" >&2; exit 1; }
}

stage="" mnt="" img=""   # global, so the exit traps still see them after the function returns

backup() {
  local pass stamp out missing=()
  pass=$(passphrase)
  stage=$(mktemp -d); trap 'rm -rf "$stage"' EXIT
  stamp=$(date +%Y%m%d-%H%M)
  while IFS= read -r f; do
    if [ -f "$(path_of "$f")" ]; then
      mkdir -p "$stage/$(dirname "$(name_in_backup "$f")")"
      cp -p "$(path_of "$f")" "$stage/$(name_in_backup "$f")"
    else
      missing+=("$f")
    fi
  done < <(entries)
  mkdir -p "$stage/databases"
  for d in "${databases[@]}"; do
    docker exec staging-postgres pg_dump -U postgres -Fc "$d" > "$stage/databases/$d.dump"
  done
  (cd "$stage" && find settings databases -type f | sort) > "$stage/MANIFEST.txt"
  mkdir -p "$backup_dir"
  out="$backup_dir/alphastream-$stamp.dmg"
  printf '%s' "$pass" | hdiutil create -quiet -encryption AES-256 -stdinpass -srcfolder "$stage" \
    -volname "AlphaStream backup $stamp" -format UDZO "$out"
  cp "$stage/MANIFEST.txt" "$backup_dir/alphastream-$stamp.manifest.txt"
  ls -1t "$backup_dir"/alphastream-*.dmg | tail -n +4 | while IFS= read -r old; do rm -f "$old" "${old%.dmg}.manifest.txt"; done
  echo "$(date '+%Y-%m-%d %H:%M') backup: $out ($(du -h "$out" | cut -f1))"
  [ ${#missing[@]} -eq 0 ] || echo "not on this Mac, so not in it: ${missing[*]}"
}

check() {
  local missing=()
  mnt=$(mktemp -d); trap 'hdiutil detach -quiet "$mnt" 2>/dev/null || true; rmdir "$mnt" 2>/dev/null || true' EXIT
  mount_newest "$mnt"
  while IFS= read -r f; do [ -f "$mnt/$(name_in_backup "$f")" ] || missing+=("$f"); done < <(entries)
  for d in "${databases[@]}"; do [ -s "$mnt/databases/$d.dump" ] || missing+=("database $d"); done
  echo "opened $img"
  if [ ${#missing[@]} -eq 0 ]; then echo "every listed file is inside"; else echo "missing: ${missing[*]}"; exit 1; fi
}

restore() {   # settings files back where they go (an existing file is kept), then the databases
  mnt=$(mktemp -d); trap 'hdiutil detach -quiet "$mnt" 2>/dev/null || true; rmdir "$mnt" 2>/dev/null || true' EXIT
  mount_newest "$mnt"
  echo "opened $img"
  while IFS= read -r f; do
    local src="$mnt/$(name_in_backup "$f")" dst; dst=$(path_of "$f")
    if [ -f "$dst" ]; then echo "kept $f (already here)"; continue; fi
    if [ -f "$src" ]; then mkdir -p "$(dirname "$dst")"; cp -p "$src" "$dst"; echo "restored $f"; fi
  done < <(entries)
  for d in "${databases[@]}"; do
    if [ -n "$(docker exec staging-postgres psql -U postgres -Atqc "select 1 from pg_database where datname = '$d'")" ]; then
      echo "kept database $d (already here)"; continue
    fi
    docker exec staging-postgres psql -U postgres -qc "do \$\$ begin if not exists (select from pg_roles where rolname = 'kevin') then create role kevin login superuser; end if; end \$\$"
    docker exec -i staging-postgres pg_restore -U postgres --create -d postgres < "$mnt/databases/$d.dump"
    echo "loaded database $d"
  done
}

case "${1:-}" in
  init)
    echo "Choose a passphrase for the backups. Keep it in your password manager: a new Mac needs it."
    security add-generic-password -U -s "$service" -a "$USER" -l "AlphaStream backup" -w
    echo "stored in the Keychain as '$service'" ;;
  run) backup ;;
  check) check ;;
  restore) restore ;;
  on)
    at="${AT:-20:00}"
    case "$at" in [0-9][0-9]:[0-5][0-9]) ;; *) echo "AT must be HH:MM" >&2; exit 2 ;; esac
    hour=$((10#${at%:*})); minute=$((10#${at#*:}))
    mkdir -p "$(dirname "$plist")" "$backup_dir"
    cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array><string>$canonical/tools/mac/backup.sh</string><string>run</string></array>
  <key>WorkingDirectory</key><string>$canonical</string>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
  <key>StartCalendarInterval</key>
  <dict><key>Weekday</key><integer>0</integer><key>Hour</key><integer>$hour</integer><key>Minute</key><integer>$minute</integer></dict>
  <key>StandardOutPath</key><string>$backup_dir/backup.log</string>
  <key>StandardErrorPath</key><string>$backup_dir/backup.log</string>
</dict>
</plist>
EOF
    launchctl bootout "$domain/$label" 2>/dev/null || true
    launchctl bootstrap "$domain" "$plist"
    printf 'timer on: a backup each Sunday at %02d:%02d into %s\n' "$hour" "$minute" "$backup_dir" ;;
  off)
    launchctl bootout "$domain/$label" 2>/dev/null || true
    rm -f "$plist"
    echo "timer off" ;;
  *) echo "usage: tools/mac/backup.sh [init|run|check|restore|on|off]" >&2; exit 2 ;;
esac
exit 0
}
