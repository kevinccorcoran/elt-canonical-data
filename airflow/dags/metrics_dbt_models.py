from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.dbt_helpers import get_dbt_bash_command

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
    dag_id="metrics_dbt_models",
    default_args=default_args,
    description="Run dbt metrics models",
    schedule_interval=None,
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    # ------------------------------------------------------------------
    # 1. Base + observation dates
    # ------------------------------------------------------------------
    bash_command, env_vars = get_dbt_bash_command(runtime_env, "intermediate_fibonacci_base")
    dbt_run_intermediate_fibonacci_base = BashOperator(
        task_id="dbt_run_intermediate_fibonacci_base",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "fibonacci_offset_observation_dates")
    dbt_run_fibonacci_offset_observation_dates = BashOperator(
        task_id="dbt_run_metrics_fibonacci_offset_observation_dates",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ------------------------------------------------------------------
    # 2. Weighted growth ranking + clustering
    # ------------------------------------------------------------------
    bash_command, env_vars = get_dbt_bash_command(runtime_env, "ticker_weighted_growth_ranking")
    dbt_run_ticker_weighted_growth_ranking = BashOperator(
        task_id="dbt_run_metrics_ticker_weighted_growth_ranking",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    python_run_ticker_cluster_segments = BashOperator(
        task_id="python_run_analysis_ticker_cluster_segments",
        bash_command=(
            "echo 'Running Python clustering analysis...' && "
            "python /Users/kevin/repos/ELT_private/src/datapipeline/analysis/ticker_cluster_segments.py"
        ),
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "ticker_cluster_volatility_summary")
    dbt_run_ticker_cluster_volatility_summary = BashOperator(
        task_id="dbt_run_metrics_ticker_cluster_volatility_summary",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ------------------------------------------------------------------
    # 3. Past / future avg prices
    # ------------------------------------------------------------------
    bash_command, env_vars = get_dbt_bash_command(runtime_env, "fibonacci_past_offset_avg_prices")
    dbt_run_metrics_fibonacci_past_offset_avg_prices = BashOperator(
        task_id="dbt_run_metrics_fibonacci_past_offset_avg_prices",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "fibonacci_future_offset_avg_prices")
    dbt_run_metrics_fibonacci_future_offset_avg_prices = BashOperator(
        task_id="dbt_run_metrics_fibonacci_future_offset_avg_prices",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ------------------------------------------------------------------
    # 4. Growth + excess return chain
    # ------------------------------------------------------------------
    bash_command, env_vars = get_dbt_bash_command(runtime_env, "fibonacci_offset_growth_rates")
    dbt_run_metrics_fibonacci_offset_growth_rates = BashOperator(
        task_id="dbt_run_metrics_fibonacci_offset_growth_rates",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "excess_return")
    dbt_run_metrics_excess_return = BashOperator(
        task_id="dbt_run_metrics_excess_return",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "excess_return_inc")
    dbt_run_metrics_excess_return_inc = BashOperator(
        task_id="dbt_run_metrics_excess_return_inc",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "excess_return_joined")
    dbt_run_excess_return_joined = BashOperator(
        task_id="dbt_run_excess_return_joined",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    bash_command, env_vars = get_dbt_bash_command(runtime_env, "excess_return_scored")
    dbt_run_excess_return_scored = BashOperator(
        task_id="dbt_run_excess_return_scored",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ------------------------------------------------------------------
    # 5. Downstream metrics model
    # ------------------------------------------------------------------
    bash_command, env_vars = get_dbt_bash_command(runtime_env, "return_likelihood_matrix")
    dbt_run_metrics_return_likelihood_matrix = BashOperator(
        task_id="dbt_run_metrics_return_likelihood_matrix",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # ------------------------------------------------------------------
    # Trigger inference DAG
    # ------------------------------------------------------------------
    trigger_inference_dbt_models = TriggerDagRunOperator(
        task_id="trigger_inference_dbt_models",
        trigger_dag_id="inference_dbt_models",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # ------------------------------------------------------------------
    # DEPENDENCIES
    # ------------------------------------------------------------------

    # Fibonacci base → observation dates
    dbt_run_intermediate_fibonacci_base >> dbt_run_fibonacci_offset_observation_dates

    # Weighted growth → clustering → volatility summary
    dbt_run_ticker_weighted_growth_ranking >> python_run_ticker_cluster_segments
    python_run_ticker_cluster_segments >> dbt_run_ticker_cluster_volatility_summary

    # Observation dates + ranking → past/future prices
    dbt_run_fibonacci_offset_observation_dates >> [
        dbt_run_metrics_fibonacci_past_offset_avg_prices,
        dbt_run_metrics_fibonacci_future_offset_avg_prices,
    ]

    dbt_run_ticker_weighted_growth_ranking >> [
        dbt_run_metrics_fibonacci_past_offset_avg_prices,
        dbt_run_metrics_fibonacci_future_offset_avg_prices,
    ]

    # Growth → excess return chain
    (
        [
            dbt_run_metrics_fibonacci_past_offset_avg_prices,
            dbt_run_metrics_fibonacci_future_offset_avg_prices,
        ]
        >> dbt_run_metrics_fibonacci_offset_growth_rates
        >> dbt_run_metrics_excess_return
        >> dbt_run_metrics_excess_return_inc
        >> dbt_run_excess_return_joined
        >> dbt_run_excess_return_scored
    )

    # 🚨 HARD GATE before likelihood matrix
    [
        dbt_run_excess_return_scored,
        dbt_run_ticker_cluster_volatility_summary,
    ] >> dbt_run_metrics_return_likelihood_matrix

    # Matrix → inference
    dbt_run_metrics_return_likelihood_matrix >> trigger_inference_dbt_models
