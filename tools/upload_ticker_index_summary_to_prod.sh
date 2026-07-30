#!/usr/bin/env bash
# Upload raw.ticker_index_summary from local staging to prod.

set -e

echo "Uploading raw.ticker_index_summary to PROD..."

DUMP_FILE=ticker_index_summary.dump
# Prod connection details — sourced from the environment, never hardcoded.
# Export PROD_DB_HOST / PROD_DB_PORT / PROD_DB_USER first (see docs/security.md).
REMOTE_HOST="${PROD_DB_HOST:?set PROD_DB_HOST}"
REMOTE_PORT="${PROD_DB_PORT:?set PROD_DB_PORT}"
REMOTE_USER="${PROD_DB_USER:?set PROD_DB_USER}"
REMOTE_DB=prod
REMOTE_CONN="host=$REMOTE_HOST port=$REMOTE_PORT user=$REMOTE_USER dbname=$REMOTE_DB sslmode=require"

# 1. Dump only the table from local (using staging database as source)
echo "Dumping local table..."
pg_dump -h localhost -U postgres -d staging \
  --format=custom \
  --table=raw.ticker_index_summary \
  > "$DUMP_FILE"

echo "Local table dump created."

# 2. Ensure schema exists remotely
# Only run this if you have the password or .pgpass set up
echo "Connecting to Remote Prod DB (You may be prompted for a password)..."
psql "$REMOTE_CONN" -c "CREATE SCHEMA IF NOT EXISTS raw;"

# 3. Drop only the target table remotely
echo "Dropping existing table on remote..."
psql "$REMOTE_CONN" -c "DROP TABLE IF EXISTS raw.ticker_index_summary CASCADE;"

# 4. Restore only that table
echo "Restoring table to remote..."
PGSSLMODE=require pg_restore \
  --no-owner \
  --role=$REMOTE_USER \
  --host=$REMOTE_HOST \
  --port=$REMOTE_PORT \
  --username=$REMOTE_USER \
  --dbname=$REMOTE_DB \
  "$DUMP_FILE"

echo "Table upload complete."
rm "$DUMP_FILE"
