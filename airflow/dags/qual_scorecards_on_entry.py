"""Daily entry-trigger buy-decision grader -> qual.ticker_scorecards.

Grades a ticker qualstream the moment it becomes a standing buy, instead of
waiting for the retired 4-monthly cadence (qual_scorecards_4monthly.py). It
resolves the board's buy universe PLUS open positions that dropped off the buy
list (--include-holds: still held, still need a live mark) and, via
`--regrade-after N`, grades ONLY names that lack a grade within the last N days
-- i.e. names that just entered the buy list, plus any buy or hold whose grade
has gone stale. On a typical day that is zero or a handful of names, so cost is
a few cents; the run exits green when there is nothing new.

--include-holds is UNCONDITIONAL here, not behind a Variable: the 4-monthly
DAG's default-off QUAL_INCLUDE_HOLDS knob was never set in prod, so holds were
silently graded by nothing (found in the 2026-08-13 gap analysis). Coverage of
open positions must not be optional.

Why daily/triggered rather than scheduled on a clock: buy-decision mode reads the
CURRENT-state warehouse tables (ledger, gate, walk-forward), so its grade is only
valid for ~today. This DAG is therefore triggered at the END of
`inference_dbt_models` (right after the ledger + gate are rebuilt), so every grade
is stamped point-in-time on fresh data -- never look-ahead-contaminated, never a
stale universe. A name graded this way gets its scorecard as_of = its entry date,
which is exactly the point-in-time series needed to later validate whether the
qualstream >= 68 buys actually outperform.

The 4-monthly DAG is RETIRED (paused in prod; file kept for reversibility) --
this DAG now covers both fresh-buy grading and the holds refresh. Grades are
upsert-idempotent per (ticker, as_of, rubric_version).

Failures ping Kevin via Telegram through utils.alerting.on_failure_telegram
(TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID from env, falling back to Airflow
Variables so the ping survives a container recreation - the 2026-08-15
recreate silently killed the old raw-os.getenv callback). A missing cred or a
failed ping only logs -- it never re-fails the task. Rationale: the one prior
Airflow grading failure (2026-08-05) sat unnoticed for 8 days; a dead grader
means new buys silently enter ungraded and drop out of the board's orange-+
filter and portfolio seeding.

REGRADE_AFTER_DAYS default 90: inside a ~12-month hold a name is re-graded roughly
every 90 days (entry, ~month 3, ~month 6, ~month 9), keeping the board's mark
fresh well under the 150-day expiry the 3838 board applies.

QUAL_ENTRY_MAX_NAMES caps names per run. Typical days grade zero or a handful, so
the cap does nothing -- it exists for the REFRESH WAVE. Every grade in the book
currently shares one as_of (2026-08-05), so the entire book goes stale on the
SAME day (2026-11-04 at 90 days) and lands as one ~55-name run. Uncapped on Opus
5 x QG_SAMPLES=3 that is ~$13, which blows QUAL_ENTRY_MAX_COST and the task fails
having graded nothing. Capped, the wave drains over a few days: names graded
today become fresh and drop out, so the next run picks up the next batch. The
resolver orders never-graded names first, so a genuine new entrant is never stuck
behind a routine refresh.

Deploy: same as qual_scorecards_4monthly.py -- copy into the host-side dags folder
(bind-mounted to /opt/airflow/dags). Needs QUALSTREAM_ROOT, DATABASE_URL and
ANTHROPIC_API_KEY in the container env, and anthropic + psycopg + python-dotenv in
the worker's python.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from utils.alerting import on_failure_telegram

log = logging.getLogger(__name__)

QUALSTREAM_ROOT = Path(
    os.environ.get("QUALSTREAM_ROOT", Path(__file__).resolve().parents[1])
)

# Per-run dollar ceiling. Daily fresh-buy batches are tiny, so this is low; the
# runner refuses to start if the estimate exceeds it.
MAX_COST = Variable.get("QUAL_ENTRY_MAX_COST", default_var="3")
if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", MAX_COST or ""):
    raise ValueError(f"QUAL_ENTRY_MAX_COST must be a number, got: {MAX_COST!r}")

# Max names per run. See the module docstring: this is the refresh-wave brake,
# not a daily-cost knob. Keep MAX_NAMES x QG_SAMPLES x per-name cost under
# QUAL_ENTRY_MAX_COST or the guard aborts the run before it starts.
MAX_NAMES = Variable.get("QUAL_ENTRY_MAX_NAMES", default_var="8")
if not re.fullmatch(r"[0-9]+", MAX_NAMES or ""):
    raise ValueError(f"QUAL_ENTRY_MAX_NAMES must be an integer, got: {MAX_NAMES!r}")

# A name graded within this many days is considered fresh and skipped. Governs
# both "grade new entrants" (never graded -> always graded) and the in-hold
# refresh cadence. Kept under the board's 150-day grade-expiry so a held name's
# mark never lapses between refreshes.
REGRADE_AFTER_DAYS = Variable.get("QUAL_REGRADE_AFTER_DAYS", default_var="90")
if not re.fullmatch(r"[0-9]+", REGRADE_AFTER_DAYS or ""):
    raise ValueError(f"QUAL_REGRADE_AFTER_DAYS must be an integer, got: {REGRADE_AFTER_DAYS!r}")

# New-category tag knobs (qualstream category.py; display-only, see the task).
# Measured 2026-10-13 on claude-opus-5-5 at effort low: ~$0.01 per name, so the
# first run's backfill of the graded universe (~185 names) drains at
# CATEGORY_MAX_NAMES a day in ~5 days for well under $1 a day; after that a
# typical day tags zero to a handful of fresh entrants. MAX_NAMES x the
# runner's per-name estimate must stay under MAX_COST or the guard refuses.
CATEGORY_MAX_COST = Variable.get("QUAL_CATEGORY_MAX_COST", default_var="2")
if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", CATEGORY_MAX_COST or ""):
    raise ValueError(f"QUAL_CATEGORY_MAX_COST must be a number, got: {CATEGORY_MAX_COST!r}")
CATEGORY_MAX_NAMES = Variable.get("QUAL_CATEGORY_MAX_NAMES", default_var="40")
if not re.fullmatch(r"[0-9]+", CATEGORY_MAX_NAMES or ""):
    raise ValueError(f"QUAL_CATEGORY_MAX_NAMES must be an integer, got: {CATEGORY_MAX_NAMES!r}")
CATEGORY_RETAG_DAYS = Variable.get("QUAL_CATEGORY_RETAG_DAYS", default_var="180")
if not re.fullmatch(r"[0-9]+", CATEGORY_RETAG_DAYS or ""):
    raise ValueError(f"QUAL_CATEGORY_RETAG_DAYS must be an integer, got: {CATEGORY_RETAG_DAYS!r}")

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    # A retry re-grades only the still-ungraded names (idempotent upsert), so the
    # spend does not balloon; still keep retries low.
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_telegram,
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

    # Verify, don't assume. This DAG being green means "the grader ran", not
    # "every standing buy has a grade" -- --limit can defer names, a resolver
    # change can shrink the universe, and the 2026-08-05 failure sat unnoticed
    # for 8 days. The check re-resolves the universe through the SAME resolver
    # the grader uses and fails the task if a name has been an ungraded standing
    # buy for more than 2 days, which fires the WhatsApp callback above.
    # Read-only: no API calls, no writes, costs nothing.
    grade_new_buys = BashOperator(
        task_id="grade_new_buys",
        # data_interval_end | ds = the trigger date (today) for a triggered run ->
        # the point-in-time as_of. --regrade-after skips already-fresh names.
        # --include-holds keeps open positions that left the buy list on the
        # same 90d refresh (unconditional -- see module docstring).
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.runner --mode buy-decision '
            f'--as-of {{{{ data_interval_end | ds }}}} '
            f'--regrade-after {REGRADE_AFTER_DAYS} --max-cost {MAX_COST} '
            f'--limit {MAX_NAMES} --include-holds'
        ),
        append_env=True,
    )

    verify_coverage = BashOperator(
        task_id="verify_coverage",
        # all_done, not the default all_success: if grading fails, the coverage
        # gap it caused is exactly what you want reported. With all_success this
        # task would be SKIPPED on the one day it matters most.
        trigger_rule="all_done",
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.coverage '
            f'--as-of {{{{ data_interval_end | ds }}}} '
            f'--regrade-after {REGRADE_AFTER_DAYS} --fail-after-days 2'
        ),
        append_env=True,
    )

    # Runs even if grading failed: a red grade task plus a red coverage task
    # tells you the gap is real, and coverage alone tells you grading silently
    # under-covered while reporting success.
    grade_new_buys >> verify_coverage

    # Hands-off book maintenance runs after grading so today's grades rank the
    # adopt queue. all_done: a red grade day must still snapshot states and
    # archive sells (it just adopts on yesterday's grades).
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    trigger_portfolio_sync = TriggerDagRunOperator(
        task_id="trigger_portfolio_sync",
        trigger_dag_id="portfolio_sync",
        trigger_rule="all_done",
    )
    verify_coverage >> trigger_portfolio_sync

    # New-category tag (Kevin 2026-10-13): "when qualstream grades a name, also
    # check whether the company is building a new category". A SEPARATE model
    # call per name (qualstream category.py) writing qual.ticker_category_tags,
    # which the 3838 master list reads for its marker and its "New-category
    # names only" filter. Display-only: it never touches the buy_decision_v1
    # prompt, the grade, the veto, the 68 bar or the adopt queue (that rubric is
    # pre-registered and frozen until 2027-02-13).
    # After grading, so a name graded today is tagged today; all_done, so a red
    # grade day still tags. Deliberately OFF the verify -> portfolio_sync chain:
    # a tagging failure (red task, Telegram ping) can never delay or block the
    # book's maintenance. Needs sql/006_category_tags.sql applied on the DB.
    tag_new_category = BashOperator(
        task_id="tag_new_category",
        trigger_rule="all_done",
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.category '
            f'--as-of {{{{ data_interval_end | ds }}}} '
            f'--retag-after {CATEGORY_RETAG_DAYS} --limit {CATEGORY_MAX_NAMES} '
            f'--max-cost {CATEGORY_MAX_COST}'
        ),
        append_env=True,
    )
    grade_new_buys >> tag_new_category
