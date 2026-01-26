from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────

PROJECT_ROOT = "/Users/kevin/repos/elt-canonical-data"

LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")
NY_TZ = pendulum.timezone("America/New_York")

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def compute_prev_trading_day(**context):
    dag_run = context.get("dag_run")

    if dag_run and dag_run.run_type == "manual":
        ny_now = pendulum.now(NY_TZ)
    else:
        ny_now = context["data_interval_end"].in_timezone(NY_TZ)

    d = (ny_now - timedelta(days=1)).date()

    while d.weekday() >= 5:
        d -= timedelta(days=1)

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
    dag_id="raw_api_data_ingestion_massive_daily",
    description="Daily Massive ingestion (previous completed trading day)",
    schedule="30 0 * * 1-5",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["massive", "raw", "daily"],
) as dag:

    compute_day = PythonOperator(
        task_id="compute_prev_trading_day",
        python_callable=compute_prev_trading_day,
    )

    fetch_massive_data = BashOperator(
        task_id="fetch_massive_data",

        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.massive_to_raw_etl \
          --start_date "{{{{ ti.xcom_pull(task_ids='compute_prev_trading_day')['date'] }}}}" \
          --end_date   "{{{{ ti.xcom_pull(task_ids='compute_prev_trading_day')['date'] }}}}"
        """,

        env={
            "DB_DATABASE": "{{ var.value.get('DB_DATABASE', 'dev') }}",
            "DATABASE_URL": "{{ var.value.get('DATABASE_URL_' ~ (var.value.get('DB_DATABASE', 'dev') | upper)) }}",
            "MASSIVE_API_KEY": "{{ var.value.get('MASSIVE_API_KEY') }}",
        },

        append_env=True,
        do_xcom_push=False,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_api_data_ingestion_massive_inc",

        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}/dbt/src/app"
        dbt run --select api_data_ingestion_massive_inc
        """,

        append_env=True,
        do_xcom_push=False,
    )

    compute_day >> fetch_massive_data >> run_dbt
