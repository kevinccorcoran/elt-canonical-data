from __future__ import annotations
import logging
from datetime import timedelta
from typing import Tuple, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.types import DagRunType
from airflow.utils.session import provide_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- helpers ---------------------------------------------------------------
def get_db_connection_string(env: str) -> str:
    env_to_var_map = {
        "dev": "DATABASE_URL_DEV",
        "staging": "DATABASE_URL_STAGING",
        "heroku_postgres": "DATABASE_URL",
    }
    if env not in env_to_var_map:
        raise ValueError(f"Invalid DB_DATABASE='{env}'. Use dev | staging | heroku_postgres.")
    conn = Variable.get(env_to_var_map[env], default_var=None)
    if not conn:
        raise ValueError(f"Airflow Variable {env_to_var_map[env]} is not set.")
    return conn

def get_bash_command(env: str, db_connection_string: str) -> Tuple[str, Dict[str, str]]:
    # Read start/end from dag_run.conf (with optional params fallback)
    # and enforce a strict window with --no-full-history
    base_args = r'''--no-full-history \
      --start_date "${START}" \
      --end_date   "${END}"'''

    if env == "heroku_postgres":
        bash = rf'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill || true' EXIT
export PYTHONPATH=$PYTHONPATH:/app/src
START="{{{{ dag_run.conf.get('start', params.start) }}}}"
END="{{{{ dag_run.conf.get('end', params.end) }}}}"
if [ -z "$START" ] || [ -z "$END" ]; then
  echo "Provide --conf '{{{{\"start\":\"YYYY-MM-DD\",\"end\":\"YYYY-MM-DD\"}}}}'"; exit 2
fi
/app/.heroku/python/bin/python3 -c "import numpy, pyarrow, polars, psycopg2; print('native-imports-ok')"
exec /app/.heroku/python/bin/python3 /app/src/datapipeline/ingestion/polygon_to_raw_etl.py {base_args}
'''
    else:
        bash = rf'''
set -euxo pipefail
trap 'jobs -p | xargs -r kill || true' EXIT

START="{{{{ dag_run.conf.get('start', params.start) }}}}"
END="{{{{ dag_run.conf.get('end', params.end) }}}}"
if [ -z "$START" ] || [ -z "$END" ]; then
  echo "Provide --conf '{{{{\"start\":\"YYYY-MM-DD\",\"end\":\"YYYY-MM-DD\"}}}}'"; exit 2
fi

/Users/kevin/repos/ELT_private/airflow_venv/bin/python -c "import numpy, pyarrow, polars, psycopg2; print('native-imports-ok')"

exec /Users/kevin/repos/ELT_private/airflow_venv/bin/python \
  /Users/kevin/repos/ELT_private/src/datapipeline/ingestion/polygon_to_raw_etl.py {base_args}
'''
    env_vars = {
        "DATABASE_URL": db_connection_string,
        "DB_DATABASE": env,
        "MASSIVE_API_KEY": Variable.get("MASSIVE_API_KEY"),
    }
    return bash, env_vars

# --- DAG -------------------------------------------------------------------
env = Variable.get("DB_DATABASE", default_var="dev")
db_conn = get_db_connection_string(env)
bash_cmd, env_vars = get_bash_command(env, db_conn)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # backfills are usually explicit; change if you want retries
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="raw_api_data_ingestion_backfill",
    description="Ad-hoc Polygon backfill for a date range (no full history)",
    schedule_interval=None,          # manual only
    start_date=None,                 # irrelevant for manual DAGs
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["polygon", "backfill", env],
    params={"start": None, "end": None, "trigger_cdm": False},
) as dag:

    fetch_range = BashOperator(
        task_id="fetch_polygon_range",
        bash_command=bash_cmd,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    def should_trigger_cdm(**context) -> bool:
        # Only trigger CDM if explicitly requested via conf
        conf = context["dag_run"].conf or {}
        return bool(conf.get("trigger_cdm", False))

    gate = ShortCircuitOperator(
        task_id="gate_trigger_cdm",
        python_callable=should_trigger_cdm,
    )

    trigger_cdm = TriggerDagRunOperator(
        task_id="trigger_cdm_api_data_ingestion",
        trigger_dag_id="cdm_api_data_ingestion",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    fetch_range >> gate >> trigger_cdm
