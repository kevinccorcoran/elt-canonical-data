from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.alerting import on_failure_telegram


PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")

# companyfacts.zip + submissions.zip (~3 GB) land here and are deleted after
# a successful load. Override with the EDGAR_WORK_DIR env var.
WORK_DIR = os.environ.get("EDGAR_WORK_DIR", "/tmp/edgar_bulk")


default_args = {
    "owner": "airflow",
    "on_failure_callback": on_failure_telegram,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
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
    dag_id="raw_ingest_edgar_bulk",
    description="One-time SEC EDGAR backfill from the nightly bulk files",
    default_args=default_args,

    # No schedule: run manually once (after tools/create_edgar_schema.sql),
    # then unpause raw_ingest_edgar_daily. Safe to rerun: insert-only.
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz=LOCAL_TZ),

    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["raw", "ingest", "edgar", "bulk"],
) as dag:

    ingest_edgar_bulk = BashOperator(
        task_id="ingest_edgar_bulk",
        bash_command=f"""
        set -euxo pipefail
        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.edgar_to_raw_etl \
            --mode bulk \
            --work-dir "{WORK_DIR}"
        """,
        env=_env(),
        append_env=True,
        do_xcom_push=False,
    )
