#!/usr/bin/env bash
# Upload the delisted tables from local staging to prod:
#   raw.ticker_metadata
#   raw.api_data_ingestion_massive_delisted
# Mirrors upload_api_data_ingestion_massive_to_prod.sh.
#
# After this runs, a prod `dbt run --select ingest_massive_delisted_inc
# ingest_massive_staging ingest_combined` will build the delisted layer
# on prod (the raw tables now exist + are populated).

set -e

echo "Uploading delisted tables (raw.ticker_metadata, raw.api_data_ingestion_massive_delisted) to PROD..."

DUMP_FILE=delisted_tables.dump
# Prod connection details
REMOTE_HOST=dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com
REMOTE_PORT=25060
REMOTE_USER=doadmin
REMOTE_DB=prod
REMOTE_CONN="host=$REMOTE_HOST port=$REMOTE_PORT user=$REMOTE_USER dbname=$REMOTE_DB sslmode=require"

# 1. Dump both delisted tables from local (staging as source).
#    Custom format carries DDL + indexes + the ticker_date_id unique
#    constraint, so the remote tables are recreated exactly.
echo "Dumping local tables..."
pg_dump -h localhost -U postgres -d staging \
  --format=custom \
  --table=raw.ticker_metadata \
  --table=raw.api_data_ingestion_massive_delisted \
  > "$DUMP_FILE"

echo "Local dump created."

# 2. Ensure schema exists remotely
echo "Connecting to Remote Prod DB (You may be prompted for a password)..."
psql "$REMOTE_CONN" -c "CREATE SCHEMA IF NOT EXISTS raw;"

# 3. Drop the target tables remotely
echo "Dropping existing tables on remote..."
psql "$REMOTE_CONN" -c "DROP TABLE IF EXISTS raw.api_data_ingestion_massive_delisted CASCADE;"
psql "$REMOTE_CONN" -c "DROP TABLE IF EXISTS raw.ticker_metadata CASCADE;"

# 4. Restore both tables
echo "Restoring tables to remote..."
PGSSLMODE=require pg_restore \
  --no-owner \
  --role=doadmin \
  --host=$REMOTE_HOST \
  --port=$REMOTE_PORT \
  --username=$REMOTE_USER \
  --dbname=$REMOTE_DB \
  "$DUMP_FILE"

echo "Delisted tables upload complete."
rm "$DUMP_FILE"
