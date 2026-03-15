from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from utils.dbt_helpers import get_inference_dbt_bash_command, get_inference_dbt_deps_command

# Runtime environment (dev / staging / prod) is resolved at execution time
# from Airflow Variables, allowing the same DAG code to run across environments.
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
    dag_id="inference_dbt_models",
    default_args=default_args,
    description="Run dbt inference model: return_expectation_decomposition",

    # No schedule: this DAG is intended to be triggered explicitly
    # (e.g. after upstream data or inference readiness).
    schedule_interval=None,

    # Use a static past start date to satisfy Airflow,
    # without enabling historical backfills.
    start_date=pendulum.today("UTC").subtract(days=1),

    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    bash_command, env_vars = get_inference_dbt_deps_command(runtime_env)

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_expectation_decomposition_past ---
    bash_past, env_past = get_inference_dbt_bash_command(runtime_env, "return_expectation_decomposition_past")
    dbt_run_past = BashOperator(
        task_id="dbt_run_return_expectation_decomposition_past",
        bash_command=bash_past,
        env=env_past,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_expectation_decomposition_future ---
    bash_future, env_future = get_inference_dbt_bash_command(runtime_env, "return_expectation_decomposition_future")
    dbt_run_future = BashOperator(
        task_id="dbt_run_return_expectation_decomposition_future",
        bash_command=bash_future,
        env=env_future,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_expectation_decomposition_combined ---
    bash_combined, env_combined = get_inference_dbt_bash_command(runtime_env, "return_expectation_decomposition_combined")
    dbt_run_combined = BashOperator(
        task_id="dbt_run_return_expectation_decomposition_combined",
        bash_command=bash_combined,
        env=env_combined,
        append_env=True,
        do_xcom_push=False,
    )

    dbt_deps >> [dbt_run_past, dbt_run_future] >> dbt_run_combined
