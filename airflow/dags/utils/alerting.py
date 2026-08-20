"""Telegram alerting for Airflow DAGs, with creds sourced from Airflow Variables.

Why Variables and not the shell env: the 2026-08-15 scheduler-container
recreation dropped TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID (and ENV) from the
container shell, which silently killed the qual-grader failure ping. Airflow
Variables live in the metadata DB and survive container recreation, so sourcing
creds from there (env first for local/test parity, Variable fallback) makes the
alert path independent of the container shell.

Set once, durably:  Admin -> Variables -> TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID.
"""
from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger(__name__)

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/elt-canonical-data")


def cred(name: str) -> str:
    """Shell env first (fast, local/test parity), then the durable Airflow
    Variable. Empty string when neither is set (send_notification then no-ops)."""
    val = os.environ.get(name)
    if val:
        return val
    try:
        from airflow.models import Variable
        return Variable.get(name, default_var="")
    except Exception:  # outside an Airflow context (e.g. a bare cron import)
        return ""


def notify(text: str) -> bool:
    """Deliver one Telegram message, creds pulled from Variables. Self-contained
    (stdlib only) - the prod container image does NOT mount the repo tests/ dir,
    so importing tests.utilities.notify here fails in-container. Returns False
    (never raises) when unconfigured or rejected."""
    tok = cred("TELEGRAM_BOT_TOKEN")
    cid = cred("TELEGRAM_CHAT_ID")
    if not tok or not cid:
        log.warning("Telegram not configured (TELEGRAM_BOT_TOKEN/CHAT_ID "
                    "Variables); not sent: %s", text)
        return False
    import json
    import urllib.request
    req = urllib.request.Request(
        "https://api.telegram.org/bot%s/sendMessage" % tok,
        data=json.dumps({"chat_id": cid, "text": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=20)
        return getattr(resp, "status", resp.getcode()) == 200
    except Exception as e:  # network / API rejection - log, never raise
        log.warning("Telegram send failed: %s", e)
        return False


def on_failure_telegram(context) -> None:
    """Airflow on_failure_callback: ping Telegram with the failed task + log URL.
    Swallows every error so a broken alert can never fail the task further."""
    try:
        ti = context.get("task_instance")
        dag_id = getattr(ti, "dag_id", "?")
        task_id = getattr(ti, "task_id", "?")
        dag_run = context.get("dag_run")
        run_id = context.get("run_id") or getattr(dag_run, "run_id", "?")
        log_url = getattr(ti, "log_url", "") or ""
        env = cred("ENV") or "prod"
        msg = (
            f"\U0001F6A8 [{env}] Airflow task FAILED\n"
            f"{dag_id}.{task_id}\n"
            f"run={run_id}\n{log_url}"
        )
        if not notify(msg):
            log.warning("failure alert not delivered (Telegram creds unset?)")
    except Exception:
        log.exception("on_failure_telegram errored (swallowed)")
