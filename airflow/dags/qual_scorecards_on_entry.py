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

Failures ping Kevin via Telegram (same env contract as tests/utilities/notify.py:
TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID). A missing env or a failed ping
only logs -- it never re-fails the task. Rationale: the one prior
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

import json
import logging
import os
import re
import urllib.request
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)


def _notify_failure(context) -> None:
    """Telegram-ping the grading failure; never raise.

    Env contract matches tests/utilities/notify.py (TELEGRAM_BOT_TOKEN +
    TELEGRAM_CHAT_ID -- the alert does not auto-discover the chat id, so pin it).
    Anything short of a sent message -- unset env, network error, non-200 -- is
    logged and swallowed so the callback can never turn a task failure into a
    second failure or mask the original log. Kept stdlib-only and self-contained
    (no cross-package import) so the DAG carries no extra worker dependency beyond
    what the grader already needs.
    """
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            log.warning("qual failure notify skipped: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set")
            return
        ti = context.get("task_instance")
        msg = "qual grading FAILED: %s.%s %s" % (
            getattr(ti, "dag_id", "?"), getattr(ti, "task_id", "?"),
            context.get("ds", "?"))
        req = urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % token,
            data=json.dumps({"chat_id": chat_id, "text": msg}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            log.info("qual failure notify sent (HTTP %s)", resp.status)
    except Exception:
        log.exception("qual failure notify errored (swallowed)")

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

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    # A retry re-grades only the still-ungraded names (idempotent upsert), so the
    # spend does not balloon; still keep retries low.
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    # fires after the FINAL failed attempt; see _notify_failure for the contract
    "on_failure_callback": _notify_failure,
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
