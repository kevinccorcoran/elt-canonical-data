"""RETIRED 2026-08-13 (paused in prod; file kept for reversibility).

The daily entry-trigger DAG (qual_scorecards_on_entry.py) now covers everything
this cadence did and more: new buys are graded on entry, and standing buys AND
holds re-grade after 90 days (--include-holds, unconditional). This DAG's
QUAL_INCLUDE_HOLDS knob defaulted off and was never set, so its holds sweep
never actually ran -- the 2026-08-13 gap analysis closed that by folding holds
into the daily run. Unpausing this would only force-regrade names the daily DAG
already keeps fresh. To revive: `airflow dags unpause qual_scorecards_4monthly`.

Original docstring follows.

Every-4-months buy-decision grader -> qual.ticker_scorecards (the 3838
Lifecycle board reads the latest non-vetoed row per ticker).

Runs 3x/year (Jan/May/Sep 1), a few days ahead of each 4-monthly buy/prune
checkpoint so grades are fresh when cohorts are formed. With ~12-month holds this
gives reviews at entry, month 4, and month 8. Data-only cost is a few dollars a
year either way. The grader judges ONLY from a point-in-time DATA block, so
`--as-of {{ data_interval_end | ds }}` stamps the scorecard's point-in-time date
-> a clean every-4-months, backtestable series. (NOT `{{ ds }}`: that is the data
interval START -- one full cadence interval in the past -- which would stamp every
scorecard 4 months old and let the consensus freshness filter expire it early.)

This file lives in the qualstream repo; copy it into the elt-canonical-data
repo's HOST-side dags folder (bind-mounted into the containers as
/opt/airflow/dags; a symlink won't resolve through the mount -- real copy only):

    # on the droplet
    cp /opt/qualstream/airflow/qual_scorecards_4monthly.py \
       /opt/elt-canonical-data/airflow/dags/
    # on a laptop checkout
    cp airflow/qual_scorecards_4monthly.py \
       ~/repos/elt-canonical-data/airflow/dags/

The task needs QUALSTREAM_ROOT (compose sets /opt/qualstream and mounts the repo
there), DATABASE_URL and ANTHROPIC_API_KEY in the container env
(docker/airflow/.env), and the image's python must have anthropic + psycopg
(baked into requirements.txt).

Like the other DAGs, it shells out (export PYTHONPATH + run the module) rather than
importing qualstream, so Airflow's scheduler never needs qualstream's deps. To make
the task RUN, the worker's python needs anthropic + psycopg + python-dotenv, plus
ANTHROPIC_API_KEY + DATABASE_URL in the env (and optionally QG_READ_DATABASE_URL
if provider reads should hit a different DB than the one being written).

Runs in --mode buy-decision: warehouse-only judgment of the pipeline's standing
buys (current BUYs x proven fut_lag-12 walk-forward slots). Needs NO fundamentals.
Writes qual.ticker_scorecards rows under rubric_version=buy_decision_v1, which the
3838 Lifecycle board reads automatically (latest non-vetoed row per ticker).

Uses QG_BUY_MODEL (default claude-sonnet-4-6). Haiku was measured collapsing most
candidates onto one score, which makes the board's top-20 ranking meaningless --
this mode ranks names against each other, so it needs the stronger model.

Airflow VARIABLES (Admin -> Variables; these are NOT shell env vars):
    QUAL_MAX_COST      = "10"  per-run dollar ceiling (runner aborts above it)
    QUAL_INCLUDE_HOLDS = "1"   also grade open positions that are no longer BUYs,
                               turning a SKIP into a prune veto (default off)
"""
from __future__ import annotations

import os
import re
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from utils.alerting import on_failure_telegram

QUALSTREAM_ROOT = Path(
    os.environ.get("QUALSTREAM_ROOT", Path(__file__).resolve().parents[1])
)

# Per-run cost ceiling (dollars). The runner refuses to start if the estimate
# exceeds this; raise via the Airflow Variable for larger universes.
MAX_COST = Variable.get("QUAL_MAX_COST", default_var="10")

# Interpolated into the shell command below, so constrain to a safe character
# class (rejects any shell metacharacters an Airflow-UI editor might set).
if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", MAX_COST or ""):
    raise ValueError(f"QUAL_MAX_COST must be a number, got: {MAX_COST!r}")

# Also grade open positions that are no longer BUYs (SKIP -> prune veto).
INCLUDE_HOLDS = Variable.get("QUAL_INCLUDE_HOLDS", default_var="0").strip() in ("1", "true", "True")
HOLDS_FLAG = " --include-holds" if INCLUDE_HOLDS else ""

default_args = {
    "owner": "airflow",
    "on_failure_callback": on_failure_telegram,
    # Repo convention: explicit False (no SMTP configured anywhere). Failure
    # visibility is the Airflow UI + the board's checkpoint banner, which reads
    # MAX(as_of) from qual.ticker_scorecards rather than trusting the calendar.
    "email_on_failure": False,
    "email_on_retry": False,
    # NOTE: a retry re-grades the whole batch (rows are upsert-idempotent, but
    # the API spend doubles). Keep retries low.
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="qual_scorecards_4monthly",
    description="Every-4-months buy-decision scorecards -> qual.ticker_scorecards (warehouse-only, point-in-time).",
    schedule="0 6 1 1,5,9 *",  # 06:00 UTC on Jan 1, May 1, Sep 1 (every 4 months)
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["qual", "scorecards", "4monthly"],
) as dag:

    grade_scorecards = BashOperator(
        task_id="grade_scorecards",
        # data_interval_end = the date this run actually fires -> the point-in-time
        # as_of. ({{ ds }} would be the interval START, 4 months in the past.)
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.runner --mode buy-decision '
            f'--as-of {{{{ data_interval_end | ds }}}} --max-cost {MAX_COST}{HOLDS_FLAG}'
        ),
        append_env=True,
    )
