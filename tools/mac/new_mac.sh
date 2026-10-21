#!/usr/bin/env bash
# Set up AlphaStream on a new Mac: the steps are in docs/new_mac.md. In short, after Homebrew, iCloud and a GitHub key:
#   git clone git@github.com:kevinccorcoran/elt-canonical-data.git ~/repos/elt-canonical-data
#   ~/repos/elt-canonical-data/tools/mac/new_mac.sh
# It installs the tools (tools/mac/Brewfile), clones the other repos, puts the settings files and the local databases
# back from the newest encrypted backup (tools/mac/backup.sh, in iCloud Drive), rebuilds staging (from a dump on the
# drive if there is one, else from prod), starts staging's Airflow, the test report and the timers, and sets up the
# app's and the checks' Python. Safe to rerun: each step skips what is done.
#   tools/mac/new_mac.sh --check    on the old Mac: is everything a new Mac needs pushed and backed up?
# The body sits in one block, so bash reads all of it before running.
{
set -euo pipefail
cd "$(dirname "$0")/../.."
canonical="$(pwd -P)"
repos="$(dirname "$canonical")"
GITHUB="${GITHUB:-git@github.com:kevinccorcoran}"
REPOS=(elt-canonical-data elt-inference-models qualstream alphastream-conformance dash)

step() { printf '\n== %s\n' "$*"; }

check() {
  local bad=0
  for r in "${REPOS[@]}"; do
    local p="$repos/$r"
    if [ ! -d "$p/.git" ]; then echo "$r: not here"; bad=1; continue; fi
    if ! git -C "$p" remote get-url origin >/dev/null 2>&1; then echo "$r: no GitHub copy at all"; bad=1; continue; fi
    git -C "$p" fetch -q origin 2>/dev/null || true
    for b in $(git -C "$p" for-each-ref --format='%(refname:short)' refs/heads); do
      if ! git -C "$p" rev-parse -q --verify "origin/$b" >/dev/null; then
        [ "$b" = "$(git -C "$p" branch --show-current)" ] && { echo "$r: branch $b is only on this Mac"; bad=1; }
        continue
      fi
      local ahead; ahead=$(git -C "$p" rev-list --count "origin/$b..$b")
      [ "$ahead" = 0 ] || { echo "$r: $b has $ahead commits only on this Mac"; bad=1; }
    done
    local edits; edits=$(git -C "$p" status --porcelain --untracked-files=no | wc -l | tr -d ' ')
    [ "$edits" = 0 ] || echo "$r: $edits uncommitted edits (commit them before the switch)"
  done
  tools/mac/backup.sh check || bad=1
  [ "$bad" = 0 ] && echo "ready: a new Mac can be set up from GitHub and the backup" || { echo "not ready yet"; exit 1; }
}

setup() {
  step "tools"
  command -v brew >/dev/null || { echo "install Homebrew first: https://brew.sh" >&2; exit 2; }
  brew bundle --file tools/mac/Brewfile
  pyenv versions --bare 2>/dev/null | grep -qx 3.11.6 || pyenv install 3.11.6
  open -a OrbStack
  for _ in $(seq 1 60); do docker info >/dev/null 2>&1 && break; sleep 2; done
  docker info >/dev/null 2>&1 || { echo "Docker (OrbStack) is not up; open OrbStack and rerun" >&2; exit 2; }
  orb config set app.start_at_login true >/dev/null 2>&1 || true

  step "repos"
  for r in "${REPOS[@]}"; do
    [ -d "$repos/$r/.git" ] && { echo "$r is here"; continue; }
    git clone "$GITHUB/$r.git" "$repos/$r"
  done

  step "settings and local databases, from the newest backup"
  security find-generic-password -s alphastream-backup -a "$USER" -w >/dev/null 2>&1 || tools/mac/backup.sh init
  docker compose -f docker/staging-postgres/docker-compose.yml up -d 2>/dev/null || true   # needs its .env: restored below
  tools/mac/backup.sh restore || true          # settings first; the databases load once staging-postgres is up
  docker compose -f docker/staging-postgres/docker-compose.yml up -d
  for _ in $(seq 1 60); do docker exec staging-postgres pg_isready -U postgres -q 2>/dev/null && break; sleep 2; done
  tools/mac/backup.sh restore

  step "staging"
  tools/staging_refresh.sh load || FORCE=1 tools/staging_refresh.sh run
  tools/staging_refresh.sh airflow
  tools/staging_refresh.sh on
  tools/mac/backup.sh on

  step "the app"
  ( cd "$repos/dash"
    [ -d .venv ] || python3.11 -m venv .venv
    .venv/bin/pip install -q -e ".[dev]"
    docker compose up -d --build )

  step "the checks and their report"
  ( cd "$repos/alphastream-conformance" && ./setup.sh && docker compose -f report/docker-compose.yml up -d && ./schedule.sh on )

  step "done"
  echo "staging Airflow  http://localhost:8082   (user kevin; password in docker/airflow-staging/.env)"
  echo "test report      http://localhost:49548"
  echo "the app          http://127.0.0.1:8050   (container dash, up whenever the container is)"
  echo "A new ssh key for the droplet and GitHub: docs/new_mac.md."
}

case "${1:-}" in
  --check) check ;;
  "") setup ;;
  *) echo "usage: tools/mac/new_mac.sh [--check]" >&2; exit 2 ;;
esac
exit 0
}
