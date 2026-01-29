from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# IMPORTANT:
# This path must match what exists *inside the Airflow container*
# NOT your Mac path.
PROJECT_ROOT = "/opt/elt-canonical-data"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_api_data_ingestion_bulk_massive",
    description="Bulk Massive load (full history for all tickers)",
    default_args=default_args,

    # No schedule — run manually only
    schedule=None,

    # Static start date (Airflow requirement only)
    start_date=datetime(2025, 8, 1),

    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["massive", "raw", "bulk"],
) as dag:

    fetch_massive_data = BashOperator(
        task_id="fetch_massive_data",
        bash_command=f"""
        set -euxo pipefail

        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}"

        python -m datapipeline.ingestion.massive_to_raw_etl
        """,
        append_env=True,
        do_xcom_push=False,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_api_data_ingestion_massive_inc",
        bash_command=f"""
        set -euxo pipefail

        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}/dbt/src/app"

        dbt run --select api_data_ingestion_massive_inc
        """,
        append_env=True,
        do_xcom_push=False,
    )

    fetch_massive_data >> run_dbt
