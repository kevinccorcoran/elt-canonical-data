from datetime import timedelta
import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from utils.dbt_helpers import get_dbt_bash_command, get_dbt_deps_command


# ──────────────────────────────────────────────
# PATHS (repo-safe, independent of CWD)
# ──────────────────────────────────────────────

# Resolve paths dynamically so the DAG works regardless of
# where Airflow is launched from (local, CI, container, etc.)
DAG_DIR = Path(__file__).resolve().parent          # airflow/dags

if "PROJECT_ROOT" in os.environ:
    REPO_ROOT = Path(os.environ["PROJECT_ROOT"])
else:
    REPO_ROOT = DAG_DIR.parents[1]                     # repo root

RAW_INGEST_SCRIPT = (
    REPO_ROOT
    / "src"
    / "datapipeline"
    / "ingestion"
    / "massive_to_raw_etl.py"
)

TRANSFORM_SCRIPT = (
    REPO_ROOT
    / "src"
    / "datapipeline"
    / "transform"
    / "api_data_ingestion_massive.py"
)


# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

# Runtime environment: prefer the shell ENV var (fast), fall back to the Airflow
# ENV Variable (set to "prod" on the prod scheduler, same as the metrics /
# inference / walk_forward DAGs). Reading os.environ alone silently resolved
# "dev" after the 2026-08-15 scheduler-container recreation dropped the shell
# ENV, so every dbt task targeted a nonexistent "dev" database and the whole
# chain died 5 min in. The Variable fallback makes the env independent of the
# container's shell.
runtime_env = os.environ.get("ENV") or Variable.get("ENV", default_var="dev")

# Number of parallel ingestion slices
# Chosen to balance API load and DB pressure
NUM_BATCHES = 4

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
        db_url = os.environ.get("DATABASE_URL_STAGING") or Variable.get("DATABASE_URL_STAGING", default_var="")
    elif env == "prod":
        db_url = os.environ.get("DATABASE_URL_PROD") or os.environ.get("DATABASE_URL") or Variable.get("DATABASE_URL_PROD", default_var="")
    else:
        db_url = os.environ.get("DATABASE_URL") or Variable.get("DATABASE_URL_DEV", default_var="")
    
    if not db_url:
         # Note: Avoid raising ValueError here during parsing if possible, 
         # but we need it for the tasks to function. 
         # Using a placeholder if missing to avoid breaking DAG import.
         db_url = "MISSING_DATABASE_URL"

    return {
        "DATABASE_URL": db_url,
        "ENV": env,
        "DB_DATABASE": env,
        "MASSIVE_API_KEY": os.environ.get("MASSIVE_API_KEY") or Variable.get("MASSIVE_API_KEY", default_var=""),
    }


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────

with DAG(
    dag_id="cdm_ingest_massive",
    default_args=default_args,
    description="Daily Massive ingestion → DBT Staging → Metrics",
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,          # Prevent overlapping runs against the same data
    is_paused_upon_creation=False,
    tags=["cdm", "ingest", "massive"],
) as dag:

    # ──────────────────────────────────────────
    # Parallel ingestion
    # ──────────────────────────────────────────

    # Split ingestion into deterministic batches so work can be
    # parallelized without overlapping responsibility.
    with TaskGroup(group_id="parallel_ingestion") as ingestion_group:
        previous_task = None
        for batch in range(1, NUM_BATCHES + 1):

            bash_cmd = (
                "set -euo pipefail && "
                f'export PYTHONPATH="{REPO_ROOT}/src" && '
                f'echo "=== Ingestion batch {batch}/{NUM_BATCHES} ===" && '
                f"python {RAW_INGEST_SCRIPT} "
                f'--start_date "1950-01-01" --end_date "{{{{ ds }}}}" '
                f"--batch {batch} --num_batches {NUM_BATCHES} && "
                f'echo "=== Transformation batch {batch}/{NUM_BATCHES} ===" && '
                f"python {TRANSFORM_SCRIPT} "
                f'--start_date "1950-01-01" --end_date "{{{{ ds }}}}" '
                f"--batch {batch} --num_batches {NUM_BATCHES}"
            )

            task = BashOperator(
                task_id=f"ingest_batch_{batch}",
                bash_command=bash_cmd,
                env=get_db_env_vars(runtime_env),
                append_env=True,
                do_xcom_push=False,
            )

            # Enforce sequential execution to strictly limit memory usage
            # (Running 4x parallel batches causes OOM on small instances)
            if previous_task:
                previous_task >> task
            previous_task = task

    # ──────────────────────────────────────────
    # DBT: reference / lookup models
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_deps_command(runtime_env)

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

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
        runtime_env, "ingest_massive_staging"
    )

    dbt_run_massive_staging = BashOperator(
        task_id="dbt_run_massive_staging",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "ingest_yfinance_staging"
    )

    dbt_run_yfinance_staging = BashOperator(
        task_id="dbt_run_yfinance_staging",
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
        task_id="dbt_run_excluded_massive",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "excluded_tickers_yfinance"
    )

    dbt_run_excluded_yfinance = BashOperator(
        task_id="dbt_run_excluded_yfinance",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ──────────────────────────────────────────
    # DBT downstream aggregation
    # ──────────────────────────────────────────

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env, "ingest_combined"
    )

    dbt_run_combined = BashOperator(
        task_id="dbt_run_combined",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ──────────────────────────────────────────
    # Data-quality gate: unadjusted split detection
    # ──────────────────────────────────────────

    # Alerting net behind the ingest-side sentinel auto-heal
    # (massive_to_raw_etl.heal_basis_breaks). Fails the task (visible in
    # Airflow) if a recent one-day price ratio looks like an unadjusted
    # split; does NOT block the metrics trigger.
    test_cmd, test_env = get_dbt_bash_command(
        runtime_env, "assert_no_basis_break_ingest_combined"
    )
    dbt_test_no_basis_break = BashOperator(
        task_id="dbt_test_no_basis_break",
        bash_command=test_cmd.replace("dbt run --select", "dbt test --select"),
        env=test_env,
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
    dbt_deps >> dbt_run_ticker_index_summary
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
    ] >> dbt_run_combined

    # Trigger metrics computation once canonical data is ready;
    # the basis-break test runs alongside, alerting without blocking
    dbt_run_combined >> trigger_metrics
    dbt_run_combined >> dbt_test_no_basis_break
