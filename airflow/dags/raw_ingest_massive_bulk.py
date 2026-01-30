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
    dag_id="raw_ingest_massive_bulk",
    description="Bulk Massive load (Initial History)",
    default_args=default_args,

    # No schedule — run manually only
    schedule=None,

    # Static start date (Airflow requirement only)
    start_date=datetime(2025, 8, 1),

    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["raw", "ingest", "massive", "bulk"],
) as dag:

    fetch_massive_data = BashOperator(
        task_id="ingest_massive_bulk",
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
        task_id="dbt_run_massive_inc",
        bash_command=f"""
        set -euxo pipefail

        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}/dbt/src/app"

        dbt run --select ingest_massive_inc
        """,
        append_env=True,
        do_xcom_push=False,
    )

    fetch_massive_data >> run_dbt
