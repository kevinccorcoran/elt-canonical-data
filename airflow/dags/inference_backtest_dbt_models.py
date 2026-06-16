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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="inference_backtest_dbt_models",
    default_args=default_args,
    description=(
        "Backtest pipeline for the return-cluster inference model. Reads "
        "return_cluster_feature_set (built by inference_dbt_models), produces "
        "the train/holdout split scored output and train-vs-holdout agreement "
        "metrics. Does not feed production; safe to iterate on without "
        "affecting prod models."
    ),
    schedule_interval=None,
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    # Build the train-only feature_set first — scored_split reads from it
    # so validation_agreement's clusters don't peek at post-cutoff data.
    bash_fs, env_fs = get_inference_dbt_bash_command(
        runtime_env, "return_cluster_feature_set_train"
    )
    dbt_run_feature_set_train = BashOperator(
        task_id="dbt_run_return_cluster_feature_set_train",
        bash_command=bash_fs,
        env=env_fs,
        append_env=True,
        do_xcom_push=False,
    )

    bash_split, env_split = get_inference_dbt_bash_command(
        runtime_env, "return_cluster_transition_scored_split"
    )
    dbt_run_transition_scored_split = BashOperator(
        task_id="dbt_run_return_cluster_transition_scored_split",
        bash_command=bash_split,
        env=env_split,
        append_env=True,
        do_xcom_push=False,
    )

    bash_val, env_val = get_inference_dbt_bash_command(
        runtime_env, "return_cluster_validation_agreement"
    )
    dbt_run_validation_agreement = BashOperator(
        task_id="dbt_run_return_cluster_validation_agreement",
        bash_command=bash_val,
        env=env_val,
        append_env=True,
        do_xcom_push=False,
    )

    bash_pay, env_pay = get_inference_dbt_bash_command(
        runtime_env, "return_cluster_payoff_backtest"
    )
    dbt_run_payoff_backtest = BashOperator(
        task_id="dbt_run_return_cluster_payoff_backtest",
        bash_command=bash_pay,
        env=env_pay,
        append_env=True,
        do_xcom_push=False,
    )

    # Walk-forward: loops the train chain across semi-annual cutoffs
    # (2004-H1 through 2024-H2 = 42 cutoffs) and snapshots
    # validation_agreement into inference.walk_forward_results per cutoff.
    # Long-running (~7-10h on prod); manually triggered or scheduled
    # quarterly. Runs AFTER the single-cutoff validation_agreement so the
    # baseline cutoff stays the dbt-var default.
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
    )

    dbt_run_feature_set_train >> dbt_run_transition_scored_split >> dbt_run_validation_agreement
    dbt_run_transition_scored_split >> dbt_run_payoff_backtest
    dbt_run_validation_agreement >> walk_forward
