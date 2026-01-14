from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ───────────────────── Constants ─────────────────────
PROJECT_ROOT = "/Users/kevin/repos/elt-canonical-data"
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")
NY_TZ = pendulum.timezone("America/New_York")

# ───────────────────── Helpers ─────────────────────
def compute_prev_trading_day(**context):
    """
    Determine the most recent *completed* US trading day.
    Works correctly for:
      - scheduled runs
      - manual runs
      - before / after NY market close
    """

    dag_run = context.get("dag_run")

    # Manual runs should use "now"
    if dag_run and dag_run.run_type == "manual":
        ny_now = pendulum.now(NY_TZ)
    else:
        ny_now = context["data_interval_end"].in_timezone(NY_TZ)

    # Go back one calendar day
    d = (ny_now - timedelta(days=1)).date()

    # Skip weekends
    while d.weekday() >= 5:
        d -= timedelta(days=1)

    return {"date": d.isoformat()}

# ───────────────────── DAG Defaults ─────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2025, 8, 1, tz=LOCAL_TZ),
}

# ───────────────────── DAG Definition ─────────────────────
with DAG(
    dag_id="raw_api_data_ingestion_polygon_daily",
    description="Daily Polygon ingestion (previous completed trading day)",
    schedule="30 0 * * 1-5",  # 00:30 local time, Mon–Fri
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["polygon", "raw", "daily"],
) as dag:

    compute_day = PythonOperator(
        task_id="compute_prev_trading_day",
        python_callable=compute_prev_trading_day,
    )

    fetch_polygon_data = BashOperator(
        task_id="fetch_polygon_data",
        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.polygon_to_raw_etl \
          --start_date "{{{{ ti.xcom_pull(task_ids='compute_prev_trading_day')['date'] }}}}" \
          --end_date   "{{{{ ti.xcom_pull(task_ids='compute_prev_trading_day')['date'] }}}}"
        """,
        env={
            "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
            "DATABASE_URL": (
                "{{ var.value.DATABASE_URL_DEV }}"
            ),
            "MASSIVE_API_KEY": "{{ var.value.MASSIVE_API_KEY }}",
        },
        append_env=True,
        do_xcom_push=False,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_api_data_ingestion_polygon_inc",
        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}/dbt/src/app"
        dbt run --select api_data_ingestion_polygon_inc
        """,
        append_env=True,
        do_xcom_push=False,
    )

    compute_day >> fetch_polygon_data >> run_dbt
