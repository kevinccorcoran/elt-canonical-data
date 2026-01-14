from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/Users/kevin/repos/elt-canonical-data"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_api_data_ingestion_bulk_polygon",
    description="Bulk Polygon load (full history for all tickers)",
    default_args=default_args,
    schedule=None,                     # ← IMPORTANT
    start_date=datetime(2025, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["polygon", "raw", "bulk"],
) as dag:

    fetch_polygon_data = BashOperator(
        task_id="fetch_polygon_data",
        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}"
        python -m datapipeline.ingestion.polygon_to_raw_etl
        """,
        env={
            "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
            "DATABASE_URL": "{{ var.value.DATABASE_URL_DEV }}",
            "MASSIVE_API_KEY": "{{ var.value.MASSIVE_API_KEY }}",
        },
        append_env=True,
        do_xcom_push=False,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_api_data_ingestion_polygon_inc",
        bash_command=f"""
        set -euxo pipefail
        cd "{PROJECT_ROOT}/dbt/src/app"
        dbt run --select api_data_ingestion_polygon_inc
        """,
        append_env=True,
        do_xcom_push=False,
    )

    fetch_polygon_data >> run_dbt


# from datetime import datetime, timedelta
# import logging

# from airflow import DAG
# from airflow.operators.bash import BashOperator

# # ───────────────────── Logging ─────────────────────
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # ───────────────────── Helpers ─────────────────────
# def select_cmd(local_cmd: str, heroku_cmd: str) -> str:
#     """
#     Choose bash command based on Airflow Variable DB_DATABASE.
#     Defaults to 'dev' if not set.
#     """
#     return (
#         "{% set env = (var.value.DB_DATABASE or 'dev') %}"
#         "{% if env in ['dev', 'staging'] %}"
#         + local_cmd +
#         "{% else %}"
#         + heroku_cmd +
#         "{% endif %}"
#     )

# # ───────────────────── Bash command templates ─────────────────────

# # LOCAL: runs inside repo using .envrc + .venv
# BASH_CMD_LOCAL = r"""
# set -euxo pipefail
# trap 'jobs -p | xargs -r kill' EXIT

# cd {{ var.value.PROJECT_ROOT }}

# python -m datapipeline.ingestion.polygon_to_raw_etl
# """

# # HEROKU: container runtime
# BASH_CMD_HEROKU = r"""
# set -euxo pipefail
# trap 'jobs -p | xargs -r kill' EXIT

# cd /app
# python -m datapipeline.ingestion.polygon_to_raw_etl
# """

# # DBT commands
# DBT_CMD_LOCAL = r"""
# set -euxo pipefail
# trap 'jobs -p | xargs -r kill' EXIT

# cd {{ var.value.PROJECT_ROOT }}/dbt/src/app
# dbt run --select api_data_ingestion_polygon_inc
# """

# DBT_CMD_HEROKU = r"""
# set -euxo pipefail
# trap 'jobs -p | xargs -r kill' EXIT

# cd /app
# dbt run --models api_data_ingestion_polygon_inc
# """

# # ───────────────────── DAG Defaults ─────────────────────
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# # ───────────────────── DAG Definition ─────────────────────
# with DAG(
#     dag_id="raw_api_data_ingestion_bulk_polygon",
#     default_args=default_args,
#     description="Fetch Polygon API data and update incremental backup model",
#     schedule=None,
#     start_date=datetime(2025, 8, 1),
#     catchup=False,
#     is_paused_upon_creation=False,
#     tags=["polygon", "raw", "dbt"],
#     max_active_runs=1,
# ) as dag:

#     fetch_polygon_data = BashOperator(
#         task_id="fetch_polygon_data",
#         bash_command=select_cmd(BASH_CMD_LOCAL, BASH_CMD_HEROKU),
#         env={
#             "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
#             "DATABASE_URL": (
#                 "{% set env = (var.value.DB_DATABASE or 'dev') %}"
#                 "{% if env == 'dev' %}{{ var.value.DATABASE_URL_DEV }}"
#                 "{% elif env == 'staging' %}{{ var.value.DATABASE_URL_STAGING }}"
#                 "{% else %}{{ var.value.DATABASE_URL }}{% endif %}"
#             ),
#             "MASSIVE_API_KEY": "{{ var.value.MASSIVE_API_KEY }}",
#         },
#         append_env=True,
#         do_xcom_push=False,
#     )

#     run_dbt_model = BashOperator(
#         task_id="run_dbt_api_data_ingestion_polygon_inc",
#         bash_command=select_cmd(DBT_CMD_LOCAL, DBT_CMD_HEROKU),
#         env={
#             "DB_DATABASE": "{{ var.value.DB_DATABASE | default('dev') }}",
#             "DATABASE_URL": (
#                 "{% set env = (var.value.DB_DATABASE or 'dev') %}"
#                 "{% if env == 'dev' %}{{ var.value.DATABASE_URL_DEV }}"
#                 "{% elif env == 'staging' %}{{ var.value.DATABASE_URL_STAGING }}"
#                 "{% else %}{{ var.value.DATABASE_URL }}{% endif %}"
#             ),
#         },
#         append_env=True,
#         do_xcom_push=False,
#     )

#     fetch_polygon_data >> run_dbt_model