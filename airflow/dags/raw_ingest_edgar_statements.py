from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.alerting import on_failure_telegram


PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")

# Each data-set zip (up to ~850 MB) lands here and is deleted after loading.
WORK_DIR = os.environ.get("EDGAR_FSN_WORK_DIR", "/tmp/edgar_fsn")


default_args = {
    "owner": "airflow",
    "on_failure_callback": on_failure_telegram,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "start_date": pendulum.datetime(2026, 10, 1, tz=LOCAL_TZ),
}


def _env() -> dict:
    db_database = os.environ.get("DB_DATABASE") or "prod"
    return {
        "DB_HOST":     os.environ.get("DB_HOST", ""),
        "DB_PORT":     os.environ.get("DB_PORT", ""),
        "DB_USER":     os.environ.get("DB_USER", ""),
        "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
        "DB_DATABASE": db_database,
        "DATABASE_URL": (
            f"postgresql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}"
            f"@{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{db_database}"
        ),
        # SEC fair access: "Name email@host". Shell env first, else the
        # Airflow Variable (rendered at run time; env is a templated field).
        "SEC_USER_AGENT": os.environ.get("SEC_USER_AGENT")
                          or "{{ var.value.get('SEC_USER_AGENT', '') }}",
        "PATH": os.environ.get("PATH", ""),
    }


with DAG(
    dag_id="raw_ingest_edgar_statements",
    description="SEC Financial Statement and Notes data sets -> edgar.fsn_* (full statements)",
    # Weekly: SEC publishes one data set a month; the loader skips every zip
    # already loaded (same Last-Modified), so most runs only check the list.
    # The FIRST run is the backfill: 79 zips, several hours.
    schedule="45 6 * * 1",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    # Paused until tools/create_edgar_statements_schema.sql has run on the
    # database; trigger once by hand for the backfill, then unpause.
    is_paused_upon_creation=True,
    tags=["raw", "ingest", "edgar", "weekly"],
) as dag:

    ingest_edgar_statements = BashOperator(
        task_id="ingest_edgar_statements",
        bash_command=f"""
        set -euxo pipefail
        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.edgar_statements_to_raw_etl \
            --work-dir "{WORK_DIR}"
        """,
        env=_env(),
        append_env=True,
        do_xcom_push=False,
    )
