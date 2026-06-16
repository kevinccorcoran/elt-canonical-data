from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

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
        "42 semi-annual cutoffs (2004-H1 through 2024-H2) and snapshots "
        "return_cluster_validation_agreement into "
        "inference.walk_forward_results per cutoff. Long-running "
        "(~7-10h on prod). Decoupled from the daily inference_backtest "
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
        execution_timeout=timedelta(hours=14),
    )
