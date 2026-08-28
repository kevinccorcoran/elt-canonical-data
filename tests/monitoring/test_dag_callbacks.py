"""Every DAG must wire a failure callback - the alerting hole closer (F8,
audits 2026-08-22 and 2026-08-28).

A NEW DAG added without `on_failure_callback` fails silently: no Telegram ping,
and the 2026-08-05 grading failure showed a silent DAG can sit red for 8 days.
There is no central default_args helper enforcing it, so this test is the
tripwire.

Static source check, not a DagBag import: these tests run on the host where
airflow is not installed (DagBag needs a live airflow env). Grep-level is
enough for the failure mode this guards - someone writing a new DAG file and
forgetting alerting entirely.
"""
from __future__ import annotations

import pathlib
import re

DAGS_DIR = pathlib.Path(__file__).resolve().parents[2] / "airflow" / "dags"

# a "DAG file" instantiates DAG(...) or uses the @dag decorator
_DAG_PAT = re.compile(r"(?:^|\W)DAG\s*\(|@dag\b")


def _dag_files():
    return sorted(
        f for f in DAGS_DIR.glob("*.py")
        if _DAG_PAT.search(f.read_text(encoding="utf-8"))
    )


def test_dags_dir_found():
    assert DAGS_DIR.is_dir(), f"dags dir missing: {DAGS_DIR}"
    assert _dag_files(), "no DAG files detected - the pattern or layout changed"


def test_every_dag_wires_failure_callback():
    missing = [
        f.name for f in _dag_files()
        if "on_failure_callback" not in f.read_text(encoding="utf-8")
    ]
    assert not missing, (
        f"DAG file(s) without on_failure_callback: {missing} - failures there "
        "would never reach Telegram. Wire utils.alerting.on_failure_telegram "
        "in default_args like the other DAGs."
    )


def test_callbacks_use_the_hardened_path():
    """The standard is utils.alerting.on_failure_telegram (env + Airflow-
    Variable fallback, survives container recreation). A raw os.getenv
    Telegram callback dies on recreate - that is the exact 2026-08-15 failure
    this repo already had once."""
    offenders = []
    for f in _dag_files():
        src = f.read_text(encoding="utf-8")
        if "on_failure_telegram" not in src:
            offenders.append(f.name)
    assert not offenders, (
        f"DAG file(s) not using utils.alerting.on_failure_telegram: {offenders}"
    )
