"""Gate snapshot archive DAG: append a dated copy of the live gate action
snapshot so the point-in-time signal panel stops being lost.

Problem this fixes: serving.return_cluster_ticker_global_action_current is
OVERWRITTEN every inference run and has no history table. The gate signals it
carries (buy_votes, net_long_density, agg_durability_score, agg_directional_score,
median_horizon_weighted_rank, best_recommendation_rank, ...) therefore exist only
as a single as-of snapshot. Any future "which signal predicted returns, by cluster,
short vs long" study needs the historical panel, and every un-archived day is data
gone forever. This DAG captures one row-set per observation_date into
serving.return_cluster_ticker_global_action_history.

Freeze-safe: pure read -> append logging. It touches NO gate/qs rubric, bar,
veto, or threshold and feeds NO recommendation. Same discipline as
board_transition_notifier / the monitoring snapshot logs.

Schedule: daily 08:45 UTC - after the 06:00 UTC ingest chain rebuilds the gate
(~07:30) and after board_transition_notifier (08:30) - and INDEPENDENT of the
chain, so a chain failure never silently drops a day's archive. Idempotent per
observation_date, so re-runs and weekend/holiday repeats (observation_date only
advances on trading days) never duplicate.

Safety: paused on creation (unpause on prod only - dev/local has no prod gate to
archive). Self-bootstrapping: CREATE/ALTER/INDEX ... IF NOT EXISTS recreate the
archive if it is ever dropped.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.alerting import on_failure_telegram

runtime_env = Variable.get("ENV", default_var="dev")

default_args = {
    "owner": "airflow",
    "on_failure_callback": on_failure_telegram,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SRC = "serving.return_cluster_ticker_global_action_current"
DST = "serving.return_cluster_ticker_global_action_history"


def _get_db_conn():
    """psycopg2 connection to this environment's DB (mirrors inference_dbt_models)."""
    import os

    import psycopg2

    password = Variable.get("DB_PASSWORD_PROD", default_var=os.getenv("DB_PASSWORD"))
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 25060)),
        user=os.getenv("DB_USER"),
        password=password,
        dbname=os.getenv("DB_DATABASE", "prod"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = True
    return conn


def _archive_gate_snapshot(**_):
    """Append every observation_date in the live snapshot not already archived.

    Idempotent on observation_date. The INSERT ... SELECT c.*, now() relies on
    DST = (SRC columns) + archived_at in that order (how it is created below); if
    SRC ever gains a column the INSERT fails loudly (telegram) rather than
    silently misaligning - the correct behaviour, prompting a migration.
    """
    import logging

    conn = _get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {DST} "
            f"(LIKE {SRC} INCLUDING DEFAULTS INCLUDING STORAGE)"
        )
        cur.execute(
            f"ALTER TABLE {DST} "
            f"ADD COLUMN IF NOT EXISTS archived_at timestamptz NOT NULL DEFAULT now()"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_gah_obsdate ON {DST} (observation_date)"
        )
        cur.execute(
            f"INSERT INTO {DST} "
            f"SELECT c.*, now() AS archived_at FROM {SRC} c "
            f"WHERE c.observation_date NOT IN "
            f"(SELECT DISTINCT observation_date FROM {DST})"
        )
        inserted = cur.rowcount
        cur.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT observation_date), MAX(observation_date) FROM {DST}"
        )
        total, days, latest = cur.fetchone()
        logging.info(
            "gate snapshot archive: +%s rows this run; archive now holds %s rows "
            "across %s day(s), latest observation_date=%s",
            inserted, total, days, latest,
        )
        if inserted == 0:
            logging.info(
                "no new observation_date to archive (already captured, or chain "
                "has not produced a newer snapshot yet) - non-fatal."
            )
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="gate_snapshot_archive",
    default_args=default_args,
    description="Append a dated copy of the gate action snapshot (point-in-time signal panel)",
    schedule="45 8 * * *",  # daily, after the ingest chain (~07:30) and notifier (08:30)
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    is_paused_upon_creation=True,  # unpause on prod only; dev has no prod gate to archive
    tags=["serving", "archive", "observability"],
) as dag:

    archive = PythonOperator(
        task_id="archive_gate_snapshot",
        python_callable=_archive_gate_snapshot,
        execution_timeout=timedelta(minutes=10),
    )
