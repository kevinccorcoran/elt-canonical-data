from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.dbt_helpers import get_dbt_bash_command, get_dbt_deps_command

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

    import os
    runtime_env = os.getenv("ENV", os.getenv("DB_DATABASE", "dev"))

    # Use dbt helpers for consistency and to ensure deps are installed
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

    fetch_massive_data >> dbt_deps >> run_dbt
