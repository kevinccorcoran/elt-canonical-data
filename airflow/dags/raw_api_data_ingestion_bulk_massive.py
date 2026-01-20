from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Absolute project root to ensure consistent execution
# regardless of where the Airflow worker starts the process.
PROJECT_ROOT = "/Users/kevin/repos/elt-canonical-data"

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

    # No schedule: this DAG is intended to be run explicitly
    # due to its heavy, full-history nature.
    schedule=None,

    # Static start date required by Airflow, not used for scheduling
    start_date=datetime(2025, 8, 1),

    catchup=False,
    max_active_runs=1,  # Prevent overlapping bulk loads
    is_paused_upon_creation=False,
    tags=["massive", "raw", "bulk"],
) as dag:

    fetch_massive_data = BashOperator(
        task_id="fetch_massive_data",

        # Run the full-history Massive ingestion script.
        # `set -euxo pipefail` ensures fast failure and clear logs.
        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.massive_to_raw_etl
        """,

        # Inject required runtime configuration via Airflow Variables
        # to avoid hardcoding credentials or environment details.
        env={
            "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
            "DATABASE_URL": "{{ var.value.DATABASE_URL_DEV }}",
            "MASSIVE_API_KEY": "{{ var.value.MASSIVE_API_KEY }}",
        },
        append_env=True,
        do_xcom_push=False,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_api_data_ingestion_massive_inc",

        # Run the downstream dbt model that processes
        # the newly ingested Massive raw data.
        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}/dbt/src/app"
        dbt run --select api_data_ingestion_massive_inc
        """,

        append_env=True,
        do_xcom_push=False,
    )

    # Ensure dbt only runs after raw ingestion completes successfully
    fetch_massive_data >> run_dbt
