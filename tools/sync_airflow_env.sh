#!/usr/bin/env bash
set -e
cd docker/airflow

cat > .env <<EOF
# Environment
ENV=$ENV

# Airflow metadata DB (ALWAYS the docker postgres)
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=airflow
AIRFLOW_DATABASE_URL=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow

# Trading DB (switches dev/staging)
DB_HOST=$DB_HOST
DB_PORT=$DB_PORT
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
DB_DATABASE=$DB_DATABASE
DATABASE_URL=$DATABASE_URL

# Secrets
MASSIVE_API_KEY=$MASSIVE_API_KEY
AIRFLOW__WEBSERVER__SECRET_KEY=$AIRFLOW__WEBSERVER__SECRET_KEY
EOF
