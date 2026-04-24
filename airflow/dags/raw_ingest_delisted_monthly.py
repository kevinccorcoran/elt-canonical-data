from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")
LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": pendulum.datetime(2026, 5, 1, tz=LOCAL_TZ),
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
        "MASSIVE_API_KEY": os.environ.get("MASSIVE_API_KEY", ""),
        "PATH": os.environ.get("PATH", ""),
    }


with DAG(
    dag_id="raw_ingest_delisted_monthly",
    description="Monthly refresh: newly-delisted US stocks + bars backfill",
    schedule="0 6 1 * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["raw", "ingest", "massive", "delisted", "monthly"],
) as dag:

    # data_interval_start is the first instant of the previous month; we
    # pull anything delisted on/after that so brand-new delistings surface.
    delisted_since = "{{ data_interval_start.strftime('%Y-%m-%d') }}"

    refresh_metadata = BashOperator(
        task_id="refresh_ticker_metadata",
        bash_command=f"""
        set -euxo pipefail
        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.massive_fetch_ticker_metadata \
            --status=delisted \
            --delisted-since="{delisted_since}"
        """,
        env=_env(),
        append_env=True,
        do_xcom_push=False,
    )

    # Only fetch tickers that match the active universe shape
    # (NYSE/NASDAQ, CS/ADRC) and fall within Polygon's historical
    # data window (5 years on the Stocks Starter plan — update if
    # the plan is upgraded). Expected monthly delta is 50–100 tickers;
    # --limit is a safety cap against runaway quota use.
    # 1826 days ~= 5 years, giving a small safety margin.
    min_delisted_since = "{{ (data_interval_end - macros.timedelta(days=1826)).strftime('%Y-%m-%d') }}"

    backfill_aggregates = BashOperator(
        task_id="backfill_new_delisted_bars",
        bash_command=f"""
        set -euxo pipefail
        export PYTHONPATH="{PROJECT_ROOT}/src"
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.massive_backfill_delisted_aggregates \
            --exchanges XNYS,XNAS \
            --min-delisted-since "{min_delisted_since}" \
            --limit 1000
        """,
        env=_env(),
        append_env=True,
        do_xcom_push=False,
    )

    refresh_metadata >> backfill_aggregates
