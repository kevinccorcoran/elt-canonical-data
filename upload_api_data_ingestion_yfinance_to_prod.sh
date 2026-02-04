#!/usr/bin/env bash
# Upload cdm.api_data_ingestion_yfinance from local staging to prod.

set -e

echo "Uploading cdm.api_data_ingestion_yfinance to PROD..."

DUMP_FILE=api_data_ingestion_yfinance.dump
# Prod connection details
REMOTE_HOST=dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com
REMOTE_PORT=25060
REMOTE_USER=doadmin
REMOTE_DB=prod
REMOTE_CONN="host=$REMOTE_HOST port=$REMOTE_PORT user=$REMOTE_USER dbname=$REMOTE_DB sslmode=require"

# 1. Dump only the table from local (using staging database as source)
echo "Dumping local table..."
PGPASSWORD=9356 pg_dump -h localhost -U postgres -d staging \
  --format=custom \
  --table=cdm.api_data_ingestion_yfinance \
  > "$DUMP_FILE"

echo "Local table dump created."

# 2. Ensure schema exists remotely
# Only run this if you have the password or .pgpass set up
echo "Connecting to Remote Prod DB (You may be prompted for a password)..."
psql "$REMOTE_CONN" -c "CREATE SCHEMA IF NOT EXISTS cdm;"

# 3. Drop only the target table remotely
echo "Dropping existing table on remote..."
psql "$REMOTE_CONN" -c "DROP TABLE IF EXISTS cdm.api_data_ingestion_yfinance CASCADE;"

# 4. Restore only that table
echo "Restoring table to remote..."
PGSSLMODE=require pg_restore \
  --no-owner \
  --role=doadmin \
  --host=$REMOTE_HOST \
  --port=$REMOTE_PORT \
  --username=$REMOTE_USER \
  --dbname=$REMOTE_DB \
  "$DUMP_FILE"

echo "Table upload complete."
rm "$DUMP_FILE"
