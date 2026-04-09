from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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


def _get_db_conn():
    """Create a psycopg2 connection using Airflow variables and env vars."""
    import os
    import psycopg2

    password = Variable.get("DB_PASSWORD_PROD", default_var=os.getenv("DB_PASSWORD"))
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 25060)),
        user=os.getenv("DB_USER"),
        password=password,
        dbname=os.getenv("DB_DATABASE", "prod"),
        sslmode=os.getenv("DB_SSLMODE", "prefer"),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = True
    return conn


def batch_create_transition_scored(**context):
    """
    Batch-process return_cluster_transition_scored per id.
    Each id (~11.5M rows) runs as its own INSERT transaction.
    """
    import logging

    conn = _get_db_conn()
    cur = conn.cursor()

    logging.info("Creating empty transition_scored table...")
    cur.execute("DROP TABLE IF EXISTS inference.return_cluster_transition_scored CASCADE")
    cur.execute("""
        CREATE TABLE inference.return_cluster_transition_scored AS
        SELECT
            fs.id,
            fs.cluster_id,
            fs.fibonacci_lag_value,
            fs.past_excess_return_vs_spy,
            fs.future_fibonacci_lag_value,
            fs.future_excess_return_vs_spy,
            CAST(NULL AS bigint) AS past_record_count,
            CAST(NULL AS double precision) AS avg_past_excess_return_vs_spy,
            CAST(NULL AS double precision) AS stddev_past_excess_return_vs_spy,
            CAST(NULL AS double precision) AS past_num_stddevs_away,
            CAST(NULL AS text) AS past_excess_return_z_bucket,
            CAST(NULL AS int) AS past_excess_return_z_bucket_num
        FROM inference.return_cluster_feature_set fs
        WHERE false
    """)

    # Get distinct (id, cluster_id, fibonacci_lag_value)
    cur.execute("""
        SELECT DISTINCT c.id, c.cluster_id, e.fibonacci_lag_value 
        FROM metrics.ticker_cluster_volatility_summary c
        CROSS JOIN (SELECT DISTINCT fibonacci_lag_value FROM metrics.excess_return_joined) e
        ORDER BY c.id, c.cluster_id, e.fibonacci_lag_value
    """)
    id_clusters = cur.fetchall()
    logging.info(f"Found {len(id_clusters)} distinct (id, cluster_id, fibonacci_lag_value) tuples to process.")

    for batch_id, cluster_id, fib_val in id_clusters:
        logging.info(f"Processing id={batch_id}, cluster_id={cluster_id}, fibonacci={fib_val}...")
        cur.execute("""
            INSERT INTO inference.return_cluster_transition_scored
            WITH base AS (
                SELECT *
                FROM inference.return_cluster_feature_set
                WHERE id = %s AND cluster_id = %s AND fibonacci_lag_value = %s
            ),
            scored_past AS (
                SELECT
                    id,
                    cluster_id,
                    fibonacci_lag_value,
                    past_excess_return_vs_spy,
                    future_fibonacci_lag_value,
                    future_excess_return_vs_spy,
                    COUNT(past_excess_return_vs_spy) OVER w_past AS past_record_count,
                    AVG(past_excess_return_vs_spy) OVER w_past AS avg_past_excess_return_vs_spy,
                    STDDEV_SAMP(past_excess_return_vs_spy) OVER w_past AS stddev_past_excess_return_vs_spy
                FROM base
                WHERE id IS NOT NULL
                  AND cluster_id IS NOT NULL
                  AND fibonacci_lag_value IS NOT NULL
                  AND past_excess_return_vs_spy IS NOT NULL
                WINDOW w_past AS (PARTITION BY id, cluster_id, fibonacci_lag_value)
            ),
            bucketed_past AS (
                SELECT
                    *,
                    CASE
                        WHEN stddev_past_excess_return_vs_spy IS NULL
                          OR stddev_past_excess_return_vs_spy = 0
                        THEN NULL
                        ELSE (past_excess_return_vs_spy - avg_past_excess_return_vs_spy) / stddev_past_excess_return_vs_spy
                    END AS past_num_stddevs_away
                FROM scored_past
            )
            SELECT
                *,
                CASE
                    WHEN past_num_stddevs_away IS NULL THEN 'Unknown'
                    WHEN past_num_stddevs_away < -3 THEN '< -3 SD'
                    WHEN past_num_stddevs_away < -2 THEN '-3 to -2 SD'
                    WHEN past_num_stddevs_away < -1 THEN '-2 to -1 SD'
                    WHEN past_num_stddevs_away <  0 THEN '-1 to 0 SD'
                    WHEN past_num_stddevs_away <  1 THEN '0 to 1 SD'
                    WHEN past_num_stddevs_away <  2 THEN '1 to 2 SD'
                    WHEN past_num_stddevs_away <  3 THEN '2 to 3 SD'
                    ELSE '> 3 SD'
                END AS past_excess_return_z_bucket,
                CASE
                    WHEN past_num_stddevs_away IS NULL THEN 0
                    WHEN past_num_stddevs_away < -3 THEN 8
                    WHEN past_num_stddevs_away < -2 THEN 7
                    WHEN past_num_stddevs_away < -1 THEN 6
                    WHEN past_num_stddevs_away <  0 THEN 5
                    WHEN past_num_stddevs_away <  1 THEN 4
                    WHEN past_num_stddevs_away <  2 THEN 3
                    WHEN past_num_stddevs_away <  3 THEN 2
                    ELSE 1
                END AS past_excess_return_z_bucket_num
            FROM bucketed_past
        """, (batch_id, cluster_id, fib_val))
        logging.info(f"  id={batch_id}, cluster_id={cluster_id}, fibonacci={fib_val} done, rows inserted: {cur.rowcount}")

    # Create index
    logging.info("Creating index on transition_scored...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rcts_partition
        ON inference.return_cluster_transition_scored (id, cluster_id, fibonacci_lag_value, future_fibonacci_lag_value)
    """)
    logging.info("transition_scored complete!")

    cur.close()
    conn.close()


def batch_create_transition_distribution(**context):
    """
    Batch-process return_cluster_transition_distribution per id.
    Each id runs as its own INSERT transaction.
    """
    import logging

    conn = _get_db_conn()
    cur = conn.cursor()

    logging.info("Creating empty transition_distribution table...")
    cur.execute("DROP TABLE IF EXISTS inference.return_cluster_transition_distribution CASCADE")
    cur.execute("""
        CREATE TABLE inference.return_cluster_transition_distribution AS
        SELECT
            id,
            cluster_id,
            fibonacci_lag_value,
            past_excess_return_vs_spy,
            future_fibonacci_lag_value,
            future_excess_return_vs_spy,
            past_record_count,
            avg_past_excess_return_vs_spy,
            stddev_past_excess_return_vs_spy,
            past_num_stddevs_away,
            past_excess_return_z_bucket,
            past_excess_return_z_bucket_num,
            CAST(NULL AS bigint) AS future_record_count,
            CAST(NULL AS double precision) AS avg_future_excess_return_vs_spy,
            CAST(NULL AS double precision) AS stddev_future_excess_return_vs_spy,
            CAST(NULL AS double precision) AS future_num_stddevs_away,
            CAST(NULL AS text) AS future_excess_return_z_bucket,
            CAST(NULL AS int) AS future_excess_return_z_bucket_num,
            now() AS processed_at
        FROM inference.return_cluster_transition_scored
        WHERE false
    """)

    # Get distinct (id, cluster_id, fibonacci_lag_value)
    cur.execute("""
        SELECT DISTINCT c.id, c.cluster_id, e.fibonacci_lag_value 
        FROM metrics.ticker_cluster_volatility_summary c
        CROSS JOIN (SELECT DISTINCT fibonacci_lag_value FROM metrics.excess_return_joined) e
        ORDER BY c.id, c.cluster_id, e.fibonacci_lag_value
    """)
    id_clusters = cur.fetchall()
    logging.info(f"Found {len(id_clusters)} distinct (id, cluster_id, fibonacci_lag_value) tuples to process.")

    for batch_id, cluster_id, fib_val in id_clusters:
        logging.info(f"Processing id={batch_id}, cluster_id={cluster_id}, fibonacci={fib_val}...")
        cur.execute("""
            INSERT INTO inference.return_cluster_transition_distribution
            WITH base AS (
                SELECT *
                FROM inference.return_cluster_transition_scored
                WHERE id = %s AND cluster_id = %s AND fibonacci_lag_value = %s
            ),
            scored_future AS (
                SELECT
                    *,
                    COUNT(future_excess_return_vs_spy) OVER w_future AS future_record_count,
                    AVG(future_excess_return_vs_spy) OVER w_future AS avg_future_excess_return_vs_spy,
                    STDDEV_SAMP(future_excess_return_vs_spy) OVER w_future AS stddev_future_excess_return_vs_spy
                FROM base
                WINDOW w_future AS (PARTITION BY id, cluster_id, fibonacci_lag_value, past_excess_return_z_bucket_num, future_fibonacci_lag_value)
            ),
            final_bucketed AS (
                SELECT
                    *,
                    CASE
                        WHEN stddev_future_excess_return_vs_spy IS NULL
                          OR stddev_future_excess_return_vs_spy = 0
                          OR future_excess_return_vs_spy IS NULL
                        THEN NULL
                        ELSE (future_excess_return_vs_spy - avg_future_excess_return_vs_spy) / stddev_future_excess_return_vs_spy
                    END AS future_num_stddevs_away
                FROM scored_future
            )
            SELECT
                id,
                cluster_id,
                fibonacci_lag_value,
                past_excess_return_vs_spy,
                future_fibonacci_lag_value,
                future_excess_return_vs_spy,
                past_record_count,
                avg_past_excess_return_vs_spy,
                stddev_past_excess_return_vs_spy,
                past_num_stddevs_away,
                past_excess_return_z_bucket,
                past_excess_return_z_bucket_num,
                future_record_count,
                avg_future_excess_return_vs_spy,
                stddev_future_excess_return_vs_spy,
                future_num_stddevs_away,
                CASE
                    WHEN future_num_stddevs_away IS NULL THEN 'Unknown'
                    WHEN future_num_stddevs_away < -3 THEN '< -3 SD'
                    WHEN future_num_stddevs_away < -2 THEN '-3 to -2 SD'
                    WHEN future_num_stddevs_away < -1 THEN '-2 to -1 SD'
                    WHEN future_num_stddevs_away <  0 THEN '-1 to 0 SD'
                    WHEN future_num_stddevs_away <  1 THEN '0 to 1 SD'
                    WHEN future_num_stddevs_away <  2 THEN '1 to 2 SD'
                    WHEN future_num_stddevs_away <  3 THEN '2 to 3 SD'
                    ELSE '> 3 SD'
                END AS future_excess_return_z_bucket,
                CASE
                    WHEN future_num_stddevs_away IS NULL THEN 0
                    WHEN future_num_stddevs_away < -3 THEN 8
                    WHEN future_num_stddevs_away < -2 THEN 7
                    WHEN future_num_stddevs_away < -1 THEN 6
                    WHEN future_num_stddevs_away <  0 THEN 5
                    WHEN future_num_stddevs_away <  1 THEN 4
                    WHEN future_num_stddevs_away <  2 THEN 3
                    WHEN future_num_stddevs_away <  3 THEN 2
                    ELSE 1
                END AS future_excess_return_z_bucket_num,
                now() AS processed_at
            FROM final_bucketed
        """, (batch_id, cluster_id, fib_val))
        logging.info(f"  id={batch_id}, cluster_id={cluster_id}, fibonacci={fib_val} done, rows inserted: {cur.rowcount}")

    # Create index
    logging.info("Creating index on transition_distribution...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rctd_partition
        ON inference.return_cluster_transition_distribution
        (id, cluster_id, fibonacci_lag_value, past_excess_return_z_bucket_num, future_fibonacci_lag_value)
    """)
    logging.info("transition_distribution complete!")

    cur.close()
    conn.close()


with DAG(
    dag_id="inference_dbt_models",
    default_args=default_args,
    description="Run dbt inference models sequentially",
    schedule_interval=None,  # manual / triggered
    start_date=pendulum.today("UTC").subtract(days=1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    # --- return_cluster_feature_set (VIEW - instant) ---
    bash_fs, env_fs = get_inference_dbt_bash_command(runtime_env, "return_cluster_feature_set")
    dbt_run_feature_set = BashOperator(
        task_id="dbt_run_return_cluster_feature_set",
        bash_command=bash_fs,
        env=env_fs,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_transition_scored (BATCH per id) ---
    batch_transition_scored = PythonOperator(
        task_id="batch_return_cluster_transition_scored",
        python_callable=batch_create_transition_scored,
        execution_timeout=timedelta(hours=4),
    )

    # --- return_cluster_transition_distribution (BATCH per id) ---
    batch_transition_distribution = PythonOperator(
        task_id="batch_return_cluster_transition_distribution",
        python_callable=batch_create_transition_distribution,
        execution_timeout=timedelta(hours=4),
    )

    # --- return_cluster_past_bucket_stats ---
    bash_stats, env_stats = get_inference_dbt_bash_command(runtime_env, "return_cluster_past_bucket_stats")
    dbt_run_past_bucket_stats = BashOperator(
        task_id="dbt_run_return_cluster_past_bucket_stats",
        bash_command=bash_stats,
        env=env_stats,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_future_bucket_stats ---
    bash_fstats, env_fstats = get_inference_dbt_bash_command(runtime_env, "return_cluster_future_bucket_stats")
    dbt_run_future_bucket_stats = BashOperator(
        task_id="dbt_run_return_cluster_future_bucket_stats",
        bash_command=bash_fstats,
        env=env_fstats,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_combined_bucket_stats ---
    bash_cstats, env_cstats = get_inference_dbt_bash_command(runtime_env, "return_cluster_combined_bucket_stats")
    dbt_run_combined_bucket_stats = BashOperator(
        task_id="dbt_run_return_cluster_combined_bucket_stats",
        bash_command=bash_cstats,
        env=env_cstats,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_lag_viability ---
    bash_viab, env_viab = get_inference_dbt_bash_command(runtime_env, "return_cluster_lag_viability")
    dbt_run_lag_viability = BashOperator(
        task_id="dbt_run_return_cluster_lag_viability",
        bash_command=bash_viab,
        env=env_viab,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_transition_confidence ---
    bash_conf, env_conf = get_inference_dbt_bash_command(runtime_env, "return_cluster_transition_confidence")
    dbt_run_transition_confidence = BashOperator(
        task_id="dbt_run_return_cluster_transition_confidence",
        bash_command=bash_conf,
        env=env_conf,
        append_env=True,
        do_xcom_push=False,
    )

    # DEPENDENCIES
    dbt_run_feature_set >> [batch_transition_scored, dbt_run_lag_viability]
    batch_transition_scored >> batch_transition_distribution >> [dbt_run_past_bucket_stats, dbt_run_future_bucket_stats] >> dbt_run_combined_bucket_stats
    [dbt_run_combined_bucket_stats, dbt_run_lag_viability] >> dbt_run_transition_confidence
