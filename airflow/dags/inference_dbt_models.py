from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

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
    dag_id="inference_dbt_models",
    default_args=default_args,
    description="Run dbt inference model: return_expectation_decomposition",
    schedule_interval=None,  # manual / triggered
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    bash_command, env_vars = get_dbt_bash_command(
        runtime_env,
        "return_expectation_decomposition"
    )

    dbt_run_return_expectation_decomposition = BashOperator(
        task_id="dbt_run_return_expectation_decomposition",
        bash_command=bash_command,
        env=env_vars,
        append_env=True,
        do_xcom_push=False,
    )
