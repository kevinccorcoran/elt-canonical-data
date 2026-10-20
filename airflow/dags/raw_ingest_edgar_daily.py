from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.alerting import on_failure_telegram


PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")


default_args = {
    "owner": "airflow",
    "on_failure_callback": on_failure_telegram,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
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
    dag_id="raw_ingest_edgar_daily",
    description="Daily SEC EDGAR fundamentals: new XBRL filings -> edgar schema",
    # 05:30 Amsterdam: after SEC rebuilds the monthly XBRL RSS feed
    # (~22:15 New York, ~04:15 Amsterdam). Window logic in the loader makes
    # a missed or doubled run harmless.
    schedule="30 5 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    # Paused until the one-time raw_ingest_edgar_bulk run has succeeded;
    # the loader refuses to run without a completed bulk load.
    is_paused_upon_creation=True,
    tags=["raw", "ingest", "edgar", "daily"],
) as dag:

    # The window end is the New York date of the run: the calendar SEC's
    # filed dates use.
    ingest_edgar_daily = BashOperator(
        task_id="ingest_edgar_daily",
        bash_command=f"""
        set -euxo pipefail
        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.edgar_to_raw_etl \
            --mode daily \
            --run-date "{{{{ data_interval_end.in_timezone('America/New_York').strftime('%Y-%m-%d') }}}}"
        """,
        env=_env(),
        append_env=True,
        execution_timeout=timedelta(hours=2),
        do_xcom_push=False,
    )
