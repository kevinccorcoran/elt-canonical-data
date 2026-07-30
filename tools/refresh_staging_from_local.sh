#!/usr/bin/env bash
# Refresh only raw.api_data_ingestion_massive on managed DB from local.

set -e

echo "Refreshing raw.api_data_ingestion_massive..."

DUMP_FILE=api_data_ingestion_massive.dump
# Prod/staging connection details — sourced from the environment, never hardcoded.
# Export PROD_DB_HOST / PROD_DB_PORT / PROD_DB_USER first (see docs/security.md).
REMOTE_HOST="${PROD_DB_HOST:?set PROD_DB_HOST}"
REMOTE_PORT="${PROD_DB_PORT:?set PROD_DB_PORT}"
REMOTE_USER="${PROD_DB_USER:?set PROD_DB_USER}"
REMOTE_CONN="host=$REMOTE_HOST port=$REMOTE_PORT user=$REMOTE_USER dbname=staging sslmode=require"

# 1. Dump only the table from local
pg_dump -h localhost -U postgres -d staging \
  --format=custom \
  --table=raw.api_data_ingestion_massive \
  > "$DUMP_FILE"

echo "Local table dump created."

# 2. Ensure schema exists remotely
psql "$REMOTE_CONN" -c "CREATE SCHEMA IF NOT EXISTS raw;"

# 3. Drop only the target table remotely
psql "$REMOTE_CONN" -c "DROP TABLE IF EXISTS raw.api_data_ingestion_massive CASCADE;"

# 4. Restore only that table
PGSSLMODE=require pg_restore \
  --no-owner \
  --role=$REMOTE_USER \
  --host=$REMOTE_HOST \
  --port=$REMOTE_PORT \
  --username=$REMOTE_USER \
  --dbname=staging \
  "$DUMP_FILE"

echo "Table refresh complete."

