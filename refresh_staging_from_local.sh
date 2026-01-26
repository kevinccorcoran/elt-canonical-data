#!/usr/bin/env bash
# Refresh only raw.api_data_ingestion_massive on managed DB from local.

set -e

echo "Refreshing raw.api_data_ingestion_massive..."

DUMP_FILE=api_data_ingestion_massive.dump
REMOTE_CONN="host=dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com port=25060 user=doadmin dbname=staging sslmode=require"

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
  --role=doadmin \
  --host=dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com \
  --port=25060 \
  --username=doadmin \
  --dbname=staging \
  "$DUMP_FILE"

echo "Table refresh complete."

