from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from utils.dbt_helpers import get_dbt_bash_command

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

runtime_env = Variable.get("ENV", default_var="dev")

PYTHON_VENV = "/Users/kevin/repos/ELT_private/airflow_venv/bin/python"
INGESTION_SCRIPT = (
    "/Users/kevin/repos/ELT_private/src/datapipeline/transform/api_data_ingestion_massive.py"
)

NUM_BATCHES = 6

local_tz = pendulum.timezone("Europe/Amsterdam")

default_args = {
    "owner": "kevin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2023, 1, 1, tz=local_tz),
}


# Helper for ingestion env vars
def get_db_env_vars(env: str) -> dict:
    if env == "staging":
        db_url = Variable.get("DATABASE_URL_STAGING")
    elif env == "heroku_postgres":
        db_url = Variable.get("DATABASE_URL")
    else:
        db_url = Variable.get("DATABASE_URL_DEV")

    return {
        "DATABASE_URL": db_url,
        "DB_DATABASE": env,
    }


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="cdm_api_data_ingestion_massive_parallel",
    default_args=default_args,
    description="Parallel ingestion → DBT staging → metrics trigger",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["cdm", "parallel"],
) as dag:


    # ──────────────────────────────────────────
    # Parallel ingestion batches
    # ──────────────────────────────────────────

    with TaskGroup(group_id="parallel_ingestion") as ingestion_group:
        for batch in range(1, NUM_BATCHES + 1):

            bash_cmd = (
                f"set -euo pipefail && "
                f'echo "=== Ingestion batch {batch}/{NUM_BATCHES} ===" && '
                f"{PYTHON_VENV} {INGESTION_SCRIPT} "
                f'--start_date \"1950-01-01\" '
                f'--end_date \"{{{{ ds }}}}\" '
                f"--batch {batch} "
                f"--num_batches {NUM_BATCHES}"
            )

            BashOperator(
                task_id=f"ingest_batch_{batch}",
                bash_command=bash_cmd,
                env=get_db_env_vars(runtime_env),
                append_env=True,
                do_xcom_push=False,
            )


    # ──────────────────────────────────────────
    # DBT: ticker_index_summary (runs in parallel with ingestion)
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "ticker_index_summary"
    )
    dbt_run_ticker_index_summary = BashOperator(
        task_id="dbt_run_ticker_index_summary",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )


    # ──────────────────────────────────────────
    # DBT staging models
    # ──────────────────────────────────────────

    # 1. Massive staging
    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "api_data_ingestion_massive_staging"
    )
    dbt_run_massive_staging = BashOperator(
        task_id="dbt_run_api_data_ingestion_massive_staging",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # 2. Yfinance staging
    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "api_data_ingestion_yfinance_staging"
    )
    dbt_run_yfinance_staging = BashOperator(
        task_id="dbt_run_api_data_ingestion_yfinance_staging",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )


    # ──────────────────────────────────────────
    # DBT excluded tickers (depends only on massive staging)
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "excluded_tickers_massive"
    )
    dbt_run_excluded_massive = BashOperator(
        task_id="dbt_run_data_quality_excluded_tickers_massive",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

        # 2. Yfinance excluded tickers
    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "excluded_tickers_yfinance"
    )
    dbt_run_excluded_yfinance = BashOperator(
        task_id="dbt_run_data_quality_excluded_tickers_yfinance",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ──────────────────────────────────────────
    # DBT downstream: api_data_ingestion_y_p
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "api_data_ingestion_y_p"
    )
    dbt_run_api_data_ingestion_y_p = BashOperator(
        task_id="dbt_run_cdm_api_data_ingestion_y_p",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )


    # ──────────────────────────────────────────
    # Trigger metrics DAG
    # ──────────────────────────────────────────

    trigger_metrics = TriggerDagRunOperator(
        task_id="trigger_metrics_dbt_models",
        trigger_dag_id="metrics_dbt_models",
        wait_for_completion=False,
    )


    # ──────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────

    # parallel ingestion AND index summary must finish before staging starts
    [ingestion_group, dbt_run_ticker_index_summary] >> dbt_run_massive_staging
    [ingestion_group, dbt_run_ticker_index_summary] >> dbt_run_yfinance_staging

    # exclusions:
    dbt_run_massive_staging >> dbt_run_excluded_massive
    dbt_run_yfinance_staging >> dbt_run_excluded_yfinance

    # api_data_ingestion_y_p waits for ALL FOUR
    [
        dbt_run_massive_staging,
        dbt_run_yfinance_staging,
        dbt_run_excluded_massive,
        dbt_run_excluded_yfinance
    ] >> dbt_run_api_data_ingestion_y_p

    # final step
    dbt_run_api_data_ingestion_y_p >> trigger_metrics

