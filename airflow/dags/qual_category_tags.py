"""Daily new-category tagger -> qual.ticker_category_tags (DISPLAY-ONLY).

Kevin 2026-10-13: when qualstream grades a name, also check whether the company
is building a new category; show a marker next to the ticker on the 3838
master list and a "New-category names only" view filter.

Runs `python -m qualstream.category`: its own model call per name, its own
table. Nothing in the grade, the 68 bar, the veto, the adopt queue or any
BUY/HOLD rule reads that table. The buy_decision_v1 rubric is pre-registered and
frozen until 2027-02-13; this DAG does not touch it.

Why a separate DAG on its own clock instead of a task inside
qual_scorecards_on_entry (2026-10-13 review):
  * Airflow sets a run's state from its leaf tasks, so a tagging problem must
    never make the GRADING run read as failed. The split does not isolate
    alerts: tools/pipeline_watch.sh alerts on any DAG's failed latest run under
    one global 6h throttle, so a failed run here still pings and can hold back
    other alerts for up to 6h. That is why the task fails only on systemic
    problems (see the failure policy below);
  * a bad display-only Variable must not be able to break the grading DAG's
    import (that would stop grading and the book). Here a malformed knob falls
    back to its default instead of raising;
  * created PAUSED: nothing runs until it is unpaused after the qualstream code
    is deployed and sql/006_category_tags.sql is applied (deploy order).

Schedule 10:00 UTC, after the ~08:00 UTC chain (cdm -> metrics -> inference ->
qual_scorecards_on_entry) has graded the day's entrants, so a name graded today
is normally tagged today; on a late-chain day it is tagged the next morning (a
display marker, not a rule).

The failure policy lives in category.py: a per-name answer problem (refusal,
placeholder) is skipped and retried next run; the task fails only on an API or
DB error, or when most names fail. execution_timeout bounds a hung API.

Knobs (Airflow Variables, all optional):
  QUAL_CATEGORY_MAX_NAMES (40)  names per run; the first run backfills the graded
                                universe (~185) at this rate, master-list names first
  QUAL_CATEGORY_MAX_COST  (2)   $ ceiling per run (the runner's guard; measured
                                ~$0.012 per name on claude-opus-5-5, 2026-10-13)
  QUAL_CATEGORY_RETAG_DAYS (180) re-tag a name after this many days
Deploy: same as qual_scorecards_on_entry.py (needs QUALSTREAM_ROOT, DATABASE_URL
and ANTHROPIC_API_KEY in the container env).
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


def _knob(name: str, default: str, pattern: str) -> str:
    """A display-only knob: a malformed value ('1,5', '2 ', '') falls back to the
    default with a warning instead of raising at import."""
    raw = Variable.get(name, default_var=default)
    val = (raw or "").strip()
    if re.fullmatch(pattern, val):
        return val
    log.warning("%s=%r is malformed; using the default %s", name, raw, default)
    return default


MAX_NAMES = _knob("QUAL_CATEGORY_MAX_NAMES", "40", r"[0-9]+")
MAX_COST = _knob("QUAL_CATEGORY_MAX_COST", "2", r"[0-9]+(\.[0-9]+)?")
RETAG_DAYS = _knob("QUAL_CATEGORY_RETAG_DAYS", "180", r"[0-9]+")

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    # a retry re-tags only names still untagged (idempotent upsert, fresh-skip)
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_telegram,
}

with DAG(
    dag_id="qual_category_tags",
    description="Daily display-only new-category tagger (qual.ticker_category_tags); never feeds the grade.",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["qual", "display-only", "daily"],
) as dag:

    tag_new_category = BashOperator(
        task_id="tag_new_category",
        # data_interval_end | ds = the run day (a scheduled run fires at the end
        # of its interval), the point-in-time as_of of every tag it writes.
        bash_command=(
            f'cd "{QUALSTREAM_ROOT}" && '
            f'export PYTHONPATH="{QUALSTREAM_ROOT}/src" && '
            f'python -m qualstream.category '
            f'--as-of {{{{ data_interval_end | ds }}}} '
            f'--retag-after {RETAG_DAYS} --limit {MAX_NAMES} --max-cost {MAX_COST}'
        ),
        # ~40 names x ~6 s is ~4 min; a hung API (120 s timeout x SDK retries)
        # must not hold a worker slot for hours
        execution_timeout=timedelta(minutes=30),
        append_env=True,
    )
