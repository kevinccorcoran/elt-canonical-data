from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator

# ───────────────────── Logging ─────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ───────────────────── Helpers ─────────────────────
def select_cmd(local_cmd: str, heroku_cmd: str) -> str:
    """
    Chooses the right bash command based on DB_DATABASE Airflow Variable.
    """
    return (
        "{% set env = (var.value.DB_DATABASE or 'dev') %}"
        "{% if env in ['dev','staging'] %}" + local_cmd +
        "{% else %}" + heroku_cmd + "{% endif %}"
    )

# ───────────────────── Bash command templates ─────────────────────
BASH_CMD_LOCAL = r'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill' EXIT

# Ensure Python can import datapipeline/
export PYTHONPATH=/Users/kevin/repos/ELT_private/src:$PYTHONPATH

/Users/kevin/repos/ELT_private/airflow_venv/bin/python -c "import numpy, pyarrow, polars, psycopg2; print('native-imports-ok')"

exec /Users/kevin/repos/ELT_private/airflow_venv/bin/python /Users/kevin/repos/ELT_private/src/datapipeline/ingestion/polygon_to_raw_etl.py
'''

BASH_CMD_HEROKU = r'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill' EXIT
export PYTHONPATH=$PYTHONPATH:/app/src
exec /app/.heroku/python/bin/python3 /app/src/datapipeline/ingestion/polygon_to_raw_etl.py
'''

DBT_CMD_LOCAL = r'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill' EXIT
cd /Users/kevin/repos/ELT_private/dbt/src/app
exec dbt run --select api_data_ingestion_polygon_inc
'''

DBT_CMD_HEROKU = r'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill' EXIT
cd /app
exec /app/.heroku/python/bin/dbt run --models api_data_ingestion_polygon_inc
'''

# ───────────────────── DAG Defaults ─────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ───────────────────── DAG ─────────────────────
with DAG(
    dag_id="raw_api_data_ingestion_bulk_polygon",
    default_args=default_args,
    description="Fetch Polygon API data and update incremental backup model",
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["polygon", "raw", "dbt"],
    max_active_runs=1,
) as dag:

    # 1. Fetch raw data from Polygon API
    fetch_polygon_data = BashOperator(
        task_id="fetch_polygon_data",
        bash_command=select_cmd(BASH_CMD_LOCAL, BASH_CMD_HEROKU),
        env={
            "DATABASE_URL": (
                "{% set env = (var.value.DB_DATABASE or 'dev') %}"
                "{% if env == 'dev' %}"
                "{{ var.value.DATABASE_URL_DEV }}"
                "{% elif env == 'staging' %}"
                "{{ var.value.DATABASE_URL_STAGING }}"
                "{% else %}"
                "{{ var.value.DATABASE_URL }}"
                "{% endif %}"
            ),
            "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
            "POLYGON_API_KEY": "{{ var.value.POLYGON_API_KEY }}",
        },
        append_env=True,
        do_xcom_push=False,
    )

    # 2. Run dbt incremental backup model
    run_dbt_model = BashOperator(
        task_id="run_dbt_api_data_ingestion_polygon_inc",
        bash_command=select_cmd(DBT_CMD_LOCAL, DBT_CMD_HEROKU),
        env={
            "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
            "DATABASE_URL": (
                "{% set env = (var.value.DB_DATABASE or 'dev') %}"
                "{% if env == 'dev' %}"
                "{{ var.value.DATABASE_URL_DEV }}"
                "{% elif env == 'staging' %}"
                "{{ var.value.DATABASE_URL_STAGING }}"
                "{% else %}"
                "{{ var.value.DATABASE_URL }}"
                "{% endif %}"
            ),
        },
        append_env=True,
        do_xcom_push=False,
    )

    fetch_polygon_data >> run_dbt_model
