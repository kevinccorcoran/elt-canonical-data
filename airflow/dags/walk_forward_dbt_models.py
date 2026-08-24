from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from utils.alerting import on_failure_telegram
from utils.dbt_helpers import get_inference_dbt_bash_command

runtime_env = Variable.get("ENV", default_var="dev")

default_args = {
    "owner": "airflow",
    "on_failure_callback": on_failure_telegram,
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
    # STATIC start_date. The old pendulum.today().subtract(days=1) re-evaluated
    # on every parse, so with catchup=False the just-completed monthly interval
    # always began before the ever-advancing start_date and the scheduler
    # dropped it: this "monthly" DAG had ZERO scheduled runs ever (every run in
    # history was a manual trigger, and the Aug-1 2026 activation never fired).
    # Anchored at the 2026-08-01 boundary so the first auto-run is Sep-1 06:00Z
    # with no surprise catch-up run at deploy time.
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
) as dag:

    # 0. Snapshot the LIVE evidence generation before anything overwrites it.
    #    Incident 2026-08-13/14: the re-mint rebuilt validation.walk_forward_*
    #    in place, the build was degenerate (slot-evidence collapse), the board
    #    went empty, and there was nothing to restore from. The guard copies
    #    the 8 evidence tables + their index DDL to wf_evidence_prev; the
    #    verify task below restores from it if the new build fails sanity.
    snapshot_evidence = BashOperator(
        task_id="snapshot_evidence",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            "python scripts/wf_evidence_guard.py snapshot"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )

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

    # 3b. Sanity-gate the fresh build BEFORE the serving side can see it.
    #     Degenerate-build detectors (proven-id collapse, identical-score tie
    #     explosion, IC sign flip, zero-score takeover). On failure the script
    #     preserves the broken generation to wf_evidence_failed_<date>,
    #     restores wf_evidence_prev into validation, and exits non-zero — so
    #     the membership re-anchor and serving-IC grade below never run on a
    #     bad build and the board keeps reading the last good generation.
    verify_evidence = BashOperator(
        task_id="verify_evidence_or_restore",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            "python scripts/wf_evidence_guard.py verify"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=45),
    )

    # 4. Re-anchor the serving evidence_id remap: snapshot serving's
    #    (ticker, id) memberships AS OF this walk-forward build. Serving
    #    re-identifies clusters against this table when daily label re-mints
    #    drift (fix 2026-07-23); a stale snapshot mislabels clusters, so this
    #    MUST run every walk-forward - and only here, never daily. Ordered
    #    BEFORE the serving-IC grade (step 5) on purpose: this refresh is
    #    critical (a skip re-creates the 2026-07-22/23 BUY-gate collapse) and
    #    retries=0, so it must not sit downstream of the long, failure-prone
    #    grader and get skipped when that grader errors.
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

    # 5. Grade the SERVING score walk-forward (audit G4: estimand alignment).
    #    walk_forward_ticker_ic grades the evidence score; prod serves
    #    agg_directional_score, a different formula whose out-of-sample payoff
    #    was unmeasurable until this step exists. Writes
    #    walk_forward_serving_rank / walk_forward_serving_ic - the evidence
    #    base for any live-rank change (rank audit 2026-07-23: live agg_rank
    #    anti-correlates with its v4 twin in the largest clusters and ordered
    #    early ledger outcomes backwards; do not re-sort serving without this).
    #    Terminal NON-BLOCKING leaf: nothing inside this DAG consumes its
    #    output, so it runs last and its failure cannot strand the membership
    #    refresh above. Reads validation.walk_forward_ticker_holdout (the old
    #    inference.* schema was deprecated in the 2026-07-15 reorg).
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

    # 6. Overlap-chained cluster identity (label-jump fix 2026-07-25):
    #    positional ids swap physical clusters in 61-73% of quarter
    #    transitions (whole-ladder +/-1 shifts), so per-label histories blend
    #    clusters. The chain follows member overlap across cutoffs (same
    #    principle as the serving evidence_id remap) and must rebuild from the
    #    fresh ranks BEFORE the top-picks model that groups its trust gate by
    #    chain_id.
    run_cluster_chain = BashOperator(
        task_id="run_walk_forward_cluster_chain",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            'if [ "${ENV:-}" = "prod" ] && [ -d .git ]; then '
            "  git fetch --quiet origin main && "
            "  git reset --quiet --hard origin/main; "
            "fi && "
            "python scripts/walk_forward_cluster_chain.py --all"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )

    # 6b. AS-OF membership lineage for the cluster chains (split-orphan fix,
    #     2026-07-26): a cluster SPLIT orphans one child onto a fresh chain_id,
    #     starving it of the mature-cohort history its members earned under the
    #     parent chain, so the top-picks trust gate wrongly fails a strong,
    #     long-lived cluster. This resolves each chain's as-of predecessor spine
    #     (member overlap, settled-only) so the trust gate can credit that
    #     history. Runs AFTER the chain rebuild and BEFORE top_picks, which
    #     reads walk_forward_chain_ancestry.
    dbt_bash_ancestry, dbt_env_ancestry = get_inference_dbt_bash_command(
        runtime_env, "walk_forward_chain_ancestry"
    )
    dbt_run_chain_ancestry = BashOperator(
        task_id="dbt_run_walk_forward_chain_ancestry",
        bash_command=dbt_bash_ancestry,
        env=dbt_env_ancestry,
        append_env=True,
        do_xcom_push=False,
    )

    # 7. Reconstructed "model's top picks" per cutoff (2026-07-24): the honest
    #    past-date recommendation view - top 10 per cluster at each cutoff,
    #    cluster eligible only on evidence SETTLED by that cutoff (trailing
    #    trust gate, grouped by chain_id), outcomes graded on real 12mo
    #    prices. Reads walk_forward_ticker_rank + cluster_id_map +
    #    cluster_chain, so it re-runs each walk-forward. (The literal
    #    BUY-gate replay was rejected: the live gate pools IC across all
    #    years = lookahead on past dates.)
    dbt_bash_picks, dbt_env_picks = get_inference_dbt_bash_command(
        runtime_env, "walk_forward_top_picks"
    )
    dbt_run_top_picks = BashOperator(
        task_id="dbt_run_walk_forward_top_picks",
        bash_command=dbt_bash_picks,
        env=dbt_env_picks,
        append_env=True,
        do_xcom_push=False,
    )

    # 8. Append the newest cutoff's top picks to the IMMUTABLE forward ledger
    #    (monitoring.top_picks_ledger). walk_forward_top_picks is rebuilt in
    #    full every run and spans the in-sample 2012-2024 backtest, so it cannot
    #    itself be the out-of-sample record. This step freezes each new
    #    (>= freeze date) cutoff's picks + entry prices exactly once; re-runs
    #    insert nothing and alter nothing (cutoff guard + ON CONFLICT). Correct
    #    no-op until the walk-forward grid rolls a cutoff past the freeze.
    #    Non-blocking leaf: nothing downstream consumes it.
    capture_top_picks_ledger = BashOperator(
        task_id="capture_top_picks_ledger",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/elt-inference-models && "
            'if [ "${ENV:-}" = "prod" ] && [ -d .git ]; then '
            "  git fetch --quiet origin main && "
            "  git reset --quiet --hard origin/main; "
            "fi && "
            "python scripts/top_picks_ledger.py"
        ),
        env={"ENV": runtime_env},
        append_env=True,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=15),
    )

    (
        snapshot_evidence
        >> walk_forward
        >> dbt_run_cluster_id_map
        >> run_ticker_ic
        >> dbt_run_gate_evidence
        >> verify_evidence
        >> refresh_membership_snapshot
        >> run_serving_ic
    )
    # parallel non-blocking leaf chain: nothing in-DAG consumes these, so a
    # failure here can never strand the critical membership refresh
    (
        refresh_membership_snapshot
        >> run_cluster_chain
        >> dbt_run_chain_ancestry
        >> dbt_run_top_picks
        >> capture_top_picks_ledger
    )
