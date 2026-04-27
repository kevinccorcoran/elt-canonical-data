#!/usr/bin/env bash
set -euo pipefail

pip install --quiet dbt-postgres
dbt deps
dbt docs generate --target staging
python /prune_manifest.py
exec dbt docs serve --port 8080 --host 0.0.0.0
