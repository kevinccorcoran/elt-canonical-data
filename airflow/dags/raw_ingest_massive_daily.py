from datetime import timedelta
import pendulum
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.dbt_helpers import get_dbt_bash_command, get_dbt_deps_command

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────

# Never hard-fail at DAG import time
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")

LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")
NY_TZ = pendulum.timezone("America/New_York")

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def compute_prev_day(**context):
    dag_run = context.get("dag_run")

    if dag_run and dag_run.run_type == "manual":
        ny_now = pendulum.now(NY_TZ)
    else:
        ny_now = context["data_interval_end"].in_timezone(NY_TZ)

    d = (ny_now - timedelta(days=1)).date()

    return {"date": d.isoformat()}

# ──────────────────────────────────────────────
# DAG defaults
# ──────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2025, 8, 1, tz=LOCAL_TZ),
}

# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────

with DAG(
    dag_id="raw_ingest_massive_daily",
    description="Daily Massive ingestion (previous day)",
    schedule="0 4 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["raw", "ingest", "massive", "daily"],
) as dag:

    # ──────────────────────────────────────────
    # Compute previous day
    # ──────────────────────────────────────────

    compute_day = PythonOperator(
        task_id="calc_target_date",
        python_callable=compute_prev_day,
    )

    # ──────────────────────────────────────────
    # Fetch Massive API data
    # ──────────────────────────────────────────

    fetch_massive_data = BashOperator(
        task_id="ingest_daily_api",
        bash_command=f"""
        set -euxo pipefail

        export PYTHONPATH="{PROJECT_ROOT}/src"

        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.massive_to_raw_etl \
          --start_date "{{{{ ti.xcom_pull(task_ids='calc_target_date')['date'] }}}}" \
          --end_date   "{{{{ ti.xcom_pull(task_ids='calc_target_date')['date'] }}}}"
        """,
        env={
            "DB_HOST": os.environ.get("DB_HOST", ""),
            "DB_PORT": os.environ.get("DB_PORT", ""),
            "DB_USER": os.environ.get("DB_USER", ""),
            "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
            "DB_DATABASE": os.environ.get("DB_DATABASE") or "prod",
            "DATABASE_URL": f"postgresql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{os.environ.get('DB_DATABASE') or 'prod'}",
            "MASSIVE_API_KEY": os.environ.get("MASSIVE_API_KEY", ""),
            "MASSIVE_MAX_WORKERS": os.environ.get("MASSIVE_MAX_WORKERS", "32"),
            "PATH": os.environ.get("PATH", ""),
        },
        do_xcom_push=False,
    )

    # Use dbt helpers for consistency and to ensure deps are installed
    runtime_env = os.environ.get("DB_DATABASE") or "prod"
    
    bash_command, env_vars = get_dbt_deps_command(runtime_env)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "ingest_massive_inc")
    run_dbt = BashOperator(
        task_id="dbt_run_massive_inc",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ──────────────────────────────────────────
    # Task dependencies
    # ──────────────────────────────────────────

    compute_day >> fetch_massive_data >> dbt_deps >> run_dbt
