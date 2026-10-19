"""Cross-DAG locks.

The monthly walk-forward (walk_forward_dbt_models) rebuilds the train chain
once per cutoff and reads tables the daily DAGs also rebuild: the feature grid
and train-side clustering (metrics_dbt_models) and feature_set_train /
transition_scored_split, the table each cutoff's holdout is snapshotted from
(inference_backtest_dbt_models, at the default 2016 cutoff). A daily rebuild
landing mid-walk can hand a cutoff the wrong grid or the wrong split.
inference_dbt_models carries its own copy of this check (2026-08-14 incident).
"""


def walk_forward_not_running(chain: str) -> bool:
    """ShortCircuit callable: False (skip everything downstream) while a
    walk-forward run is active or queued. The skip is a SUCCESS state, so no
    failure callback fires; ping it explicitly. A failed send never blocks."""
    from airflow.models import DagRun
    from airflow.utils.state import State

    active = DagRun.find(dag_id="walk_forward_dbt_models", state=State.RUNNING)
    active += DagRun.find(dag_id="walk_forward_dbt_models", state=State.QUEUED)
    if not active:
        return True
    print(
        f"walk_forward_dbt_models has {len(active)} active/queued run(s); "
        f"skipping the {chain} rebuild so the walk keeps one consistent input."
    )
    try:
        from utils.alerting import notify
        notify(
            f"⏸️ [prod] {chain} rebuild SKIPPED - walk_forward is running/queued, "
            "so today's daily chain waits. If WF ends early, re-trigger "
            "metrics_dbt_models; else tomorrow's run catches up."
        )
    except Exception:
        print("skip-notify failed (non-fatal)")
    return False
