"""Weekly gap fill: fetch again every missing day of the raw price table, then check it filled.

The daily fetch reaches back 60 days, so a missed morning fills itself the next day. This job covers the rest:
any open market day in the whole history with no rows, or under half a normal day's rows
(datapipeline.quality.price_gaps says which days count). Each run of missing days is fetched again with the same
loader the daily fetch uses, newest first and at most MAX_DAYS days a run; the daily cdm rebuild picks the rows
up. Days still missing after the fetch fail the run, so Telegram says so. Saturday 07:00, after the daily fetch
and before the cdm rebuild at 08:00. It replaced the Saturday copy from prod when staging became the live system.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.alerting import notify, on_failure_telegram

log = logging.getLogger(__name__)

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")
SRC = os.path.join(PROJECT_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

LOCAL_TZ = pendulum.timezone("Europe/Amsterdam")
NY_TZ = pendulum.timezone("America/New_York")
MAX_DAYS = 60       # missing days fetched in one run; the rest wait a week, newest go first
SETTLE_DAYS = 2     # the newest days are the daily fetch's; this job looks at days older than this


def _missing(g, cur, end):
    start = g.first_day(cur)
    return g.missing_days(g.daily_counts(cur, start, end), start, end)


def fill_gaps(**_):
    import psycopg2

    from datapipeline.quality import price_gaps as g

    end = pendulum.now(NY_TZ).date() - timedelta(days=SETTLE_DAYS)
    con = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        missing = _missing(g, con.cursor(), end)
    finally:
        con.close()
    if not missing:
        log.info("no missing days up to %s", end)
        return

    todo = sorted(missing)[-MAX_DAYS:]
    for first, last in reversed(g.spans(todo)):
        log.info("fetching %s to %s again", first, last)
        subprocess.run(
            [sys.executable, "-m", "datapipeline.ingestion.massive_to_raw_etl",
             "--start_date", first.isoformat(), "--end_date", last.isoformat()],
            cwd=PROJECT_ROOT, env={**os.environ, "PYTHONPATH": SRC}, check=True)

    con = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        still = _missing(g, con.cursor(), end)
    finally:
        con.close()
    filled = [d for d in todo if d not in set(still)]
    days = lambda ds: ", ".join(str(d) for d in ds[:8]) + (" ..." if len(ds) > 8 else "")
    if filled:
        notify(f"Gap fill: fetched {len(filled)} missing price day(s) again: {days(filled)}.")
    if still:
        raise RuntimeError(f"{len(still)} price day(s) still missing after the fetch: {days(still)}")


with DAG(
    dag_id="raw_gap_fill_weekly",
    description="Weekly: fetch again every missing day of the raw price table",
    schedule="0 7 * * 6",
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "on_failure_callback": on_failure_telegram,
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["raw", "ingest", "massive", "weekly", "quality"],
) as dag:
    PythonOperator(task_id="fill_gaps", python_callable=fill_gaps, execution_timeout=timedelta(hours=4))
