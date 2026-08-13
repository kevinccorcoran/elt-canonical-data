"""Daily entry-trigger buy-decision grader -> qual.ticker_scorecards.

Grades a ticker qualstream the moment it becomes a standing buy, instead of
waiting for the 4-monthly cadence (qual_scorecards_4monthly.py). It resolves the
board's buy universe and, via `--regrade-after N`, grades ONLY names that lack a
grade within the last N days -- i.e. names that just entered the buy list, plus
any whose grade has gone stale. On a typical day that is zero or a handful of
names, so cost is a few cents; the run exits green when there is nothing new.

Why daily/triggered rather than scheduled on a clock: buy-decision mode reads the
CURRENT-state warehouse tables (ledger, gate, walk-forward), so its grade is only
valid for ~today. This DAG is therefore triggered at the END of
`inference_dbt_models` (right after the ledger + gate are rebuilt), so every grade
is stamped point-in-time on fresh data -- never look-ahead-contaminated, never a
stale universe. A name graded this way gets its scorecard as_of = its entry date,
which is exactly the point-in-time series needed to later validate whether the
qualstream >= 68 buys actually outperform.

The 4-monthly DAG stays for the periodic `--include-holds` prune pass over open
positions; this DAG covers fresh-buy grading. Grades are upsert-idempotent per
(ticker, as_of, rubric_version), so the two never collide.

REGRADE_AFTER_DAYS default 90: inside a ~12-month hold a name is re-graded roughly
every 90 days (entry, ~month 3, ~month 6, ~month 9), keeping the board's mark
fresh well under the 150-day expiry the 3838 board applies.

Deploy: same as qual_scorecards_4monthly.py -- copy into the host-side dags folder
(bind-mounted to /opt/airflow/dags). Needs QUALSTREAM_ROOT, DATABASE_URL and
ANTHROPIC_API_KEY in the container env, and anthropic + psycopg + python-dotenv in
the worker's python.
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

QUALSTREAM_ROOT = Path(
    os.environ.get("QUALSTREAM_ROOT", Path(__file__).resolve().parents[1])
)

# Per-run dollar ceiling. Daily fresh-buy batches are tiny, so this is low; the
# runner refuses to start if the estimate exceeds it.
MAX_COST = Variable.get("QUAL_ENTRY_MAX_COST", default_var="3")
if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", MAX_COST or ""):
    raise ValueError(f"QUAL_ENTRY_MAX_COST must be a number, got: {MAX_COST!r}")

# A name graded within this many days is considered fresh and skipped. Governs
# both "grade new entrants" (never graded -> always graded) and the in-hold
# refresh cadence. Kept under the board's 150-day grade-expiry so a held name's
# mark never lapses between refreshes.
REGRADE_AFTER_DAYS = Variable.get("QUAL_REGRADE_AFTER_DAYS", default_var="90")
if not re.fullmatch(r"[0-9]+", REGRADE_AFTER_DAYS or ""):
    raise ValueError(f"QUAL_REGRADE_AFTER_DAYS must be an integer, got: {REGRADE_AFTER_DAYS!r}")

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    # A retry re-grades only the still-ungraded names (idempotent upsert), so the
    # spend does not balloon; still keep retries low.
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="qual_scorecards_on_entry",
    description="Daily entry-trigger buy-decision grader (grades names as they enter the buy list).",
    # schedule=None: triggered by inference_dbt_models after the ledger + gate are
    # rebuilt, so the current-state read is fresh and the as_of is genuinely today.
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["qual", "scorecards", "entry", "daily"],
) as dag:

    grade_new_buys = BashOperator(
        task_id="grade_new_buys",
        # data_interval_end | ds = the trigger date (today) for a triggered run ->
        # the point-in-time as_of. --regrade-after skips already-fresh names.
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.runner --mode buy-decision '
            f'--as-of {{{{ data_interval_end | ds }}}} '
            f'--regrade-after {REGRADE_AFTER_DAYS} --max-cost {MAX_COST}'
        ),
        append_env=True,
    )
