"""Weekly qualitative LLM ticker grader -> qual.ticker_grades (consensus input).

This file lives in the qualstream repo but is *symlinked* into the shared Airflow
dags folder so the scheduler can see it:

    ln -s /Users/kevin/repos/qualstream/airflow/dag.py \
          /Users/kevin/repos/elt-canonical-data/airflow/dags/qual_ticker_grades_weekly.py

Like the other DAGs, it shells out to the sibling-repo code (export PYTHONPATH +
run the module) rather than importing it, so Airflow's scheduler never needs
qualstream's deps. To make the task *run*, the worker's python needs the 3 runtime
deps (anthropic, psycopg, python-dotenv) -- e.g. `pip install -e <qualstream>` --
plus ANTHROPIC_API_KEY + DATABASE_URL in the env (or a .env in the repo root).

Pick clusters without editing code via an Airflow Variable (Admin -> Variables):
    QUAL_CLUSTER_IDS = "1,4,7"        # comma- or space-separated; defaults to 1,4,7
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Resolve the qualstream repo whether this file is opened directly or via the
# symlink in the dags folder -- .resolve() follows the symlink to the real path.
QUALSTREAM_ROOT = Path(
    os.environ.get("QUALSTREAM_ROOT", Path(__file__).resolve().parents[1])
)

# Clusters to grade weekly. Override in the Airflow UI (Admin -> Variables) without
# touching code; passed straight to the runner's --cluster-ids.
CLUSTER_IDS = Variable.get("QUAL_CLUSTER_IDS", default_var="1,4,7")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="qual_ticker_grades_weekly",
    description="Weekly Claude qualitative ticker grades -> qual.ticker_grades.",
    schedule="0 6 * * 1",  # Mondays 06:00 UTC
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["qual", "llm", "ticker-grades"],
) as dag:

    grade_tickers = BashOperator(
        task_id="grade_tickers",
        # run_date defaults to today inside the runner -> always a forward-only,
        # real-time run (past dates are look-ahead contaminated and refused).
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.runner --cluster-ids "{CLUSTER_IDS}"'
        ),
        append_env=True,
    )
