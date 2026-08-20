"""Board transition notifier DAG: Telegram-ping every ACTIONABLE board flip.

Wraps tests/monitoring/notifier.py (diff each qualstream-graded ticker's board
state against the last snapshot in monitoring.board_state_snapshot; the table is
self-created and seeded silently on first run).

Gating: buy<->hold flapping is a KNOWN benign oscillation (single-vote marginal
straddle around the monthly-majority line), so those transitions are recorded in
the snapshot but NOT messaged. Everything else pings: new names entering, any
transition into sell/closed, off-board exits, and sell->buy re-entries.

Schedule: daily 08:30 UTC - after the 06:00 UTC ingest chain has rebuilt the
ledger/gate (~07:30) - and independent of it, so maturity-driven flips (which
change the board with no DB write) are still caught on days the chain fails.

Safety: paused on creation (unpause on prod only - locally there is neither a
prod DB nor Telegram creds). Without TELEGRAM_* Variables the run still diffs
and persists the snapshot but delivers nothing (send_notification no-ops).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.alerting import cred, on_failure_telegram


def _run_notifier(**_):
    import os
    import sys

    project_root = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Creds from durable Airflow Variables into the env the sender reads
    # (container shells lost TELEGRAM_* in the 2026-08-15 recreation).
    for k in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        v = cred(k)
        if v and not os.environ.get(k):
            os.environ[k] = v

    from tests.monitoring.notifier import format_message, run_once
    from tests.utilities.db import get_conn
    from tests.utilities.notify import send_notification

    env_label = cred("ENV") or os.environ.get("DB_DATABASE") or "prod"
    # get_conn defaults to the LOCAL dev DB and reads DB_NAME (unset here);
    # point it at this environment's database explicitly.
    conn = get_conn(dbname=os.environ.get("DB_DATABASE") or "prod")
    try:
        # Collect transitions without messaging (notify_fn no-op), then send
        # only the actionable subset - the snapshot still records everything.
        transitions = run_once(conn, lambda _text: True)
    finally:
        conn.close()

    flap = {"buy", "hold"}
    sent = 0
    for ticker, old, new, grade in transitions:
        if old is not None and {old, new} <= flap:
            continue  # buy<->hold oscillation: known benign, suppressed
        if send_notification(f"[{env_label}] {format_message(ticker, old, new, grade)}"):
            sent += 1
    print(f"[{env_label}] {len(transitions)} transition(s), {sent} messaged "
          f"({len(transitions) - sent} suppressed/undelivered)")


default_args = {
    "owner": "kevin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": pendulum.datetime(2026, 8, 1, tz="UTC"),
    "on_failure_callback": on_failure_telegram,
}

with DAG(
    dag_id="board_transition_notifier",
    description="Telegram ping on actionable board flips (buy<->hold flap suppressed)",
    default_args=default_args,
    schedule="30 8 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,   # unpause on prod only
    tags=["monitoring", "telegram", "board"],
) as dag:
    PythonOperator(
        task_id="diff_and_notify",
        python_callable=_run_notifier,
    )
