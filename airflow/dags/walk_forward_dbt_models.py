from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from utils.dbt_helpers import get_inference_dbt_bash_command

runtime_env = Variable.get("ENV", default_var="dev")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    dag_id="walk_forward_dbt_models",
    default_args=default_args,
    description=(
        "Monthly walk-forward validation. Loops the train chain across "
        "84 quarterly cutoffs (2004-Q1 through 2024-Q4) and snapshots "
        "return_cluster_validation_agreement into "
        "inference.walk_forward_results per cutoff, then rebuilds the "
        "cluster_id->id map, ticker-IC ranks, and id_ic / pctile BUY-gate "
        "evidence from the fresh results so the gate never reads stale or "
        "misattributed IC. Long-running "
        "(~9-12h on prod). Decoupled from the daily inference_backtest "
        "cascade so the daily pipeline stays fast."
    ),
    schedule_interval="0 6 1 * *",
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
) as dag:

    walk_forward = BashOperator(
        task_id="run_walk_forward_orchestrator",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            'if [ "${ENV:-}" = "prod" ] && [ -d .git ]; then '
            "  git fetch --quiet origin main && "
            "  git reset --quiet --hard origin/main; "
            "fi && "
            "python scripts/walk_forward.py"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        # 84 quarterly cutoffs; historical prod runs fit inside 14h but with
        # little margin. A timeout kill mid-walk leaves walk_forward_results
        # mixed-generation, so give headroom instead of risking a partial walk.
        execution_timeout=timedelta(hours=18),
    )

    # ── Gate-evidence rebuild ──────────────────────────────────────────────
    # Fix: walk_forward.py rewrites walk_forward_results with fresh per-build
    # GMM cluster_ids every run, but nothing here refreshed the downstream
    # gate evidence. That left the BUY gate reading a stale cluster_id->id map
    # and frozen id_ic, or misattributing old rows to new cluster_ids (the
    # stale-map failure seen on staging). Rebuild the whole chain in order.

    # 1. Per-cutoff cluster_id -> stable id map, derived from the fresh
    #    walk_forward_results. walk_forward_ticker_ic.py HARD-DEPENDS on this
    #    (it raises if >1% of holdout rows fail to map), so it must run first.
    dbt_bash_map, dbt_env_map = get_inference_dbt_bash_command(
        runtime_env, "walk_forward_cluster_id_map"
    )
    dbt_run_cluster_id_map = BashOperator(
        task_id="dbt_run_walk_forward_cluster_id_map",
        bash_command=dbt_bash_map,
        env=dbt_env_map,
        append_env=True,
        do_xcom_push=False,
    )

    # 2. Recompute per-(ticker, cluster) IC + ranks for every gradable horizon.
    #    Writes walk_forward_ticker_ic / walk_forward_cluster_ic /
    #    walk_forward_ticker_rank (the script clears its exact scope per
    #    invocation before re-inserting, audit G1). Horizons match the serving
    #    cap (max_future_lag = 33): 54+ labels cannot realize inside the 4y
    #    holdout and sign-flip, so they are not graded (audit cap decision,
    #    shipped 2026-07-01).
    run_ticker_ic = BashOperator(
        task_id="run_walk_forward_ticker_ic",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            'if [ "${ENV:-}" = "prod" ] && [ -d .git ]; then '
            "  git fetch --quiet origin main && "
            "  git reset --quiet --hard origin/main; "
            "fi && "
            "for FL in 1 2 4 7 12 20 33; do "
            '  python scripts/walk_forward_ticker_ic.py --all --fut-lag "$FL"; '
            "done"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=4),
    )

    # 3. Rebuild the BUY-gate evidence (return_cluster_pair_recommendation reads
    #    walk_forward_id_ic) and the Rank-Stability dashboard feed
    #    (walk_forward_pctile_summary) from the fresh ranks + map.
    dbt_bash_gate, dbt_env_gate = get_inference_dbt_bash_command(
        runtime_env, "walk_forward_id_ic walk_forward_pctile_summary"
    )
    dbt_run_gate_evidence = BashOperator(
        task_id="dbt_run_gate_evidence",
        bash_command=dbt_bash_gate,
        env=dbt_env_gate,
        append_env=True,
        do_xcom_push=False,
    )

    # 4. Grade the SERVING score walk-forward (audit G4: estimand alignment).
    #    walk_forward_ticker_ic grades the evidence score; prod serves
    #    agg_directional_score, a different formula whose out-of-sample payoff
    #    was unmeasurable until this step exists. Writes
    #    walk_forward_serving_rank / walk_forward_serving_ic - the evidence
    #    base for any live-rank change (rank audit 2026-07-23: live agg_rank
    #    anti-correlates with its v4 twin in the largest clusters and ordered
    #    early ledger outcomes backwards; do not re-sort serving without this).
    run_serving_ic = BashOperator(
        task_id="run_walk_forward_serving_ic",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            'if [ "${ENV:-}" = "prod" ] && [ -d .git ]; then '
            "  git fetch --quiet origin main && "
            "  git reset --quiet --hard origin/main; "
            "fi && "
            "python scripts/walk_forward_serving_ic.py --all --variant both"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )

    # 5. Re-anchor the serving evidence_id remap: snapshot serving's
    #    (ticker, id) memberships AS OF this walk-forward build. Serving
    #    re-identifies clusters against this table when daily label re-mints
    #    drift (fix 2026-07-23); a stale snapshot mislabels clusters, so this
    #    MUST run every walk-forward - and only here, never daily.
    dbt_bash_snap, dbt_env_snap = get_inference_dbt_bash_command(
        runtime_env, "__snapshot_op__"
    )
    refresh_membership_snapshot = BashOperator(
        task_id="refresh_membership_snapshot",
        # reuse the helper's cd/git-reset/env plumbing, swap the dbt verb
        bash_command=dbt_bash_snap.replace(
            "dbt run --select __snapshot_op__",
            "dbt run-operation refresh_membership_snapshot",
        ),
        env=dbt_env_snap,
        append_env=True,
        do_xcom_push=False,
    )

    (
        walk_forward
        >> dbt_run_cluster_id_map
        >> run_ticker_ic
        >> dbt_run_gate_evidence
        >> run_serving_ic
        >> refresh_membership_snapshot
    )
