#!/usr/bin/env bash
set -euo pipefail

# dbt bounded < 2.0: dbt-core 2.0.0-alpha removed `dbt docs generate/serve`.
pip install --quiet 'dbt-core<2' 'dbt-postgres<2'
dbt deps
dbt docs generate --target staging
python /prune_manifest.py
python /app/docker/dbt-docs-canonical/inject_schema_colors.py \
  target/index.html /app/docker/dbt-docs-canonical/dbt_docs_schema_colors.js
# Serve target/ statically (not `dbt docs serve`, which overwrites index.html
# with its packaged copy at startup and would wipe the schema-color inject).
exec python -m http.server 8080 --directory target --bind 0.0.0.0
