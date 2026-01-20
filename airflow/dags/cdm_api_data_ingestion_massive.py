from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from utils.dbt_helpers import get_dbt_bash_command


# ──────────────────────────────────────────────
# PATHS (repo-safe, independent of CWD)
# ──────────────────────────────────────────────

# Resolve paths dynamically so the DAG works regardless of
# where Airflow is launched from (local, CI, container, etc.)
DAG_DIR = Path(__file__).resolve().parent          # airflow/dags
REPO_ROOT = DAG_DIR.parents[1]                     # repo root

INGESTION_SCRIPT = (
    REPO_ROOT
    / "src"
    / "datapipeline"
    / "transform"
    / "api_data_ingestion_massive.py"
)


# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

# Runtime environment is driven by Airflow Variables
# (not hardcoded, and not inferred from the machine)
runtime_env = Variable.get("ENV", default_var="dev")

# Number of parallel ingestion slices
# Chosen to balance API load and DB pressure
NUM_BATCHES = 6

# Explicit timezone for scheduling and timestamps
local_tz = pendulum.timezone("Europe/Amsterdam")

default_args = {
    "owner": "kevin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Fixed historical start date; catchup is disabled below
    "start_date": pendulum.datetime(2023, 1, 1, tz=local_tz),
}


# ──────────────────────────────────────────────
# Helper: DB env vars for ingestion scripts
# ──────────────────────────────────────────────

def get_db_env_vars(env: str) -> dict:
    """
    Build the minimal set of environment variables required
    by ingestion scripts.

    - Ingestion scripts expect DATABASE_URL
    - dbt relies on DB_* variables from the surrounding environment
    """
    if env == "staging":
        db_url = Variable.get("DATABASE_URL_STAGING")
    elif env == "prod":
        db_url = Variable.get("DATABASE_URL_PROD")
    else:
        db_url = Variable.get("DATABASE_URL_DEV")

    return {
        "DATABASE_URL": db_url,
        "ENV": env,
        "DB_DATABASE": env,
    }


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="cdm_api_data_ingestion_massive_parallel",
    default_args=default_args,
    description="Parallel ingestion → DBT staging → metrics trigger",
    schedule_interval=None,     # Manually triggered / externally orchestrated
    catchup=False,
    max_active_runs=1,          # Prevent overlapping runs against the same data
    is_paused_upon_creation=False,
    tags=["cdm", "parallel"],
) as dag:

    # ──────────────────────────────────────────
    # Parallel ingestion
    # ──────────────────────────────────────────

    # Split ingestion into deterministic batches so work can be
    # parallelized without overlapping responsibility.
    with TaskGroup(group_id="parallel_ingestion") as ingestion_group:
        for batch in range(1, NUM_BATCHES + 1):

            bash_cmd = (
                "set -euo pipefail && "
                f'echo "=== Ingestion batch {batch}/{NUM_BATCHES} ===" && '
                f"python {INGESTION_SCRIPT} "
                f'--start_date "1950-01-01" '
                f'--end_date "{{{{ ds }}}}" '
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
    # DBT: reference / lookup models
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
    # DBT exclusion / filtering models
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "excluded_tickers_massive"
    )

    dbt_run_excluded_massive = BashOperator(
        task_id="dbt_run_excluded_tickers_massive",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "excluded_tickers_yfinance"
    )

    dbt_run_excluded_yfinance = BashOperator(
        task_id="dbt_run_excluded_tickers_yfinance",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ──────────────────────────────────────────
    # DBT downstream aggregation
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "api_data_ingestion_y_p"
    )

    dbt_run_api_data_ingestion_y_p = BashOperator(
        task_id="dbt_run_api_data_ingestion_y_p",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ──────────────────────────────────────────
    # Trigger metrics DAG
    # ──────────────────────────────────────────

    # Metrics are computed in a separate DAG to keep
    # ingestion and analytics lifecycles decoupled.
    trigger_metrics = TriggerDagRunOperator(
        task_id="trigger_metrics_dbt_models",
        trigger_dag_id="metrics_dbt_models",
        wait_for_completion=False,
    )

    # ──────────────────────────────────────────
    # Dependencies
    # ──────────────────────────────────────────

    # Staging depends on both raw ingestion and reference data
    [ingestion_group, dbt_run_ticker_index_summary] >> dbt_run_massive_staging
    [ingestion_group, dbt_run_ticker_index_summary] >> dbt_run_yfinance_staging

    # Exclusions are derived from staged data
    dbt_run_massive_staging >> dbt_run_excluded_massive
    dbt_run_yfinance_staging >> dbt_run_excluded_yfinance

    # Final aggregation requires all upstream preparation
    [
        dbt_run_massive_staging,
        dbt_run_yfinance_staging,
        dbt_run_excluded_massive,
        dbt_run_excluded_yfinance,
    ] >> dbt_run_api_data_ingestion_y_p

    # Trigger metrics computation once canonical data is ready
    dbt_run_api_data_ingestion_y_p >> trigger_metrics
