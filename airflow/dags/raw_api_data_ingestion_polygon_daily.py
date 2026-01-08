from __future__ import annotations
import logging
from datetime import timedelta
import pendulum
from typing import Tuple, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ─────────────── connection string helper ───────────────
def get_db_connection_string(env: str) -> str:
    env_to_var_map = {
        "dev": "DATABASE_URL_DEV",
        "staging": "DATABASE_URL_STAGING",
        "heroku_postgres": "DATABASE_URL",
    }
    if env not in env_to_var_map:
        raise ValueError(f"Invalid environment specified: {env}. Please set a valid DB_DATABASE variable.")
    conn = Variable.get(env_to_var_map[env], default_var=None)
    if not conn:
        raise ValueError(f"Airflow Variable {env_to_var_map[env]} is not set.")
    return conn

def _is_trading_day(**context) -> bool:
    ny_dt = context["data_interval_end"].in_timezone("America/New_York")
    d = (ny_dt - timedelta(days=1)).date()
    while d.isoweekday() > 5:
        d -= timedelta(days=1)
    return True  # TODO: add holiday check if needed

def _compute_prev_trading_day(**context):
    ny_dt = context["data_interval_end"].in_timezone("America/New_York")
    d = (ny_dt - timedelta(days=1)).date()
    while d.isoweekday() > 5:
        d -= timedelta(days=1)
    return {"date": d.isoformat()}

# ─────────────── bash command + env builder ───────────────
def get_bash_command(env: str, db_connection_string: str) -> Tuple[str, Dict[str, str]]:
    one_day_args = (
    "--start_date \"{{ ti.xcom_pull(task_ids='compute_prev_trading_day')['date'] }}\" "
    "--end_date   \"{{ ti.xcom_pull(task_ids='compute_prev_trading_day')['date'] }}\""
    )


    if env == "heroku_postgres":
        bash_command = rf'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill || true' EXIT
export PYTHONPATH=$PYTHONPATH:/app/src
/app/.heroku/python/bin/python3 -c "import numpy, pyarrow, polars, psycopg2; print('native-imports-ok')"
exec /app/.heroku/python/bin/python3 /app/src/datapipeline/ingestion/polygon_to_raw_etl.py {one_day_args}
'''
    else:
        bash_command = rf'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill || true' EXIT
/Users/kevin/repos/ELT_private/airflow_venv/bin/python -c "import numpy, pyarrow, polars, psycopg2; print('native-imports-ok')"
exec /Users/kevin/repos/ELT_private/airflow_venv/bin/python /Users/kevin/repos/ELT_private/src/datapipeline/ingestion/polygon_to_raw_etl.py {one_day_args}
'''

    env_vars = {
        "DATABASE_URL": db_connection_string,
        "DB_DATABASE": env,
        "POLYGON_API_KEY": Variable.get("POLYGON_API_KEY"),
    }
    return bash_command, env_vars

# ─────────────── dbt commands ───────────────
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

def select_dbt_cmd(env: str) -> str:
    if env == "heroku_postgres":
        return DBT_CMD_HEROKU
    return DBT_CMD_LOCAL

# ─────────────── DAG config ───────────────
local_tz = pendulum.timezone("Europe/Amsterdam")

env = Variable.get("DB_DATABASE", default_var="dev")
db_connection_string = get_db_connection_string(env)
bash_command, env_vars = get_bash_command(env, db_connection_string)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "start_date": pendulum.datetime(2025, 8, 1, 0, 0, tz=local_tz),
}

with DAG(
    dag_id="raw_api_data_ingestion_polygon_daily",
    description="Daily Polygon load → trigger CDM ingestion (previous trading day)",
    schedule_interval="30 0 * * 1-5",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["polygon", "raw", env],
) as dag:

    check_trading_day = ShortCircuitOperator(
        task_id="check_trading_day",
        python_callable=_is_trading_day,
    )

    compute_prev_trading_day = PythonOperator(
        task_id="compute_prev_trading_day",
        python_callable=_compute_prev_trading_day,
    )

    fetch_polygon_data = BashOperator(
        task_id="fetch_polygon_data",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    run_dbt_model = BashOperator(
        task_id="run_dbt_api_data_ingestion_polygon_inc",
        bash_command=select_dbt_cmd(env),
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    check_trading_day >> compute_prev_trading_day >> fetch_polygon_data >> run_dbt_model
