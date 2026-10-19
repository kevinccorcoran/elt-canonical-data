"""Every DAG that rebuilds a walk-forward input must carry the walk-forward lock.

The monthly walk (walk_forward_dbt_models) rebuilds the train chain per cutoff
and reads the feature grid, the train clustering and transition_scored_split.
A daily DAG rebuilding any of them mid-walk hands a cutoff the wrong input
(metrics and backtest had no lock until 2026-10-20). Static source check, like
test_dag_callbacks.py: airflow is not installed on the host.
"""
from __future__ import annotations

import pathlib

DAGS_DIR = pathlib.Path(__file__).resolve().parents[2] / "airflow" / "dags"

# dbt models the walk reads or rebuilds per cutoff
WALK_INPUTS = (
    "intermediate_fibonacci_base",
    "ticker_weighted_growth_ranking_train",
    "ticker_cluster_volatility_summary_train",
    "return_cluster_feature_set_train",
    "return_cluster_transition_scored_split",
    "excess_return_joined",
)
WALK_DAG = "walk_forward_dbt_models.py"


def _builders():
    return sorted(
        f for f in DAGS_DIR.glob("*.py")
        if f.name != WALK_DAG
        and any(m in f.read_text(encoding="utf-8") for m in WALK_INPUTS)
    )


def test_walk_input_builders_found():
    names = {f.name for f in _builders()}
    assert {"metrics_dbt_models.py", "inference_backtest_dbt_models.py"} <= names, names


def test_walk_input_builders_carry_the_lock():
    missing = []
    for f in _builders():
        src = f.read_text(encoding="utf-8")
        if "ShortCircuitOperator" not in src or "walk_forward_not_running" not in src:
            missing.append(f.name)
    assert not missing, (
        f"DAG file(s) rebuilding walk-forward inputs without the lock: {missing}. "
        "Add a ShortCircuitOperator on utils.dag_locks.walk_forward_not_running "
        "in front of every root task."
    )
