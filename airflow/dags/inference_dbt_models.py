from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

    # Performance tuning for the per-partition window/aggregate work. Big
    # clusters (id=4 ~1.6M rows, id=7 ~15M rows) otherwise spill the
    # DISTINCT/sort to disk at the managed-PG default work_mem (a few MB).
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 4")

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
                  AND past_excess_return_vs_spy IS NOT NULL
            ),
            -- Robust center/scale for the (id, cluster, lag) partition using
            -- median + MAD (median absolute deviation) instead of mean + SD.
            -- A single outlier pulls mean/SD enough that most observations
            -- z-score to the same bucket; median/MAD ignore the outlier so
            -- bucket coverage stays well-distributed.
            -- The 1.4826 multiplier makes MAD asymptotically equal to SD for
            -- normal data, so the downstream ±1/±2/±3 SD thresholds keep their
            -- meaning. Column names stay 'avg_*' and 'stddev_*' for backward
            -- compatibility with downstream models.
            distinct_past AS (
                SELECT DISTINCT past_excess_return_vs_spy FROM base
            ),
            past_center AS (
                SELECT
                    COUNT(*) AS past_record_count,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY past_excess_return_vs_spy) AS past_median
                FROM distinct_past
            ),
            past_abs_dev AS (
                SELECT ABS(dp.past_excess_return_vs_spy - pc.past_median) AS abs_dev
                FROM distinct_past dp
                CROSS JOIN past_center pc
            ),
            past_mad AS (
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY abs_dev) AS mad
                FROM past_abs_dev
            ),
            past_stats AS (
                SELECT
                    pc.past_record_count,
                    pc.past_median AS avg_past_excess_return_vs_spy,
                    1.4826 * pm.mad AS stddev_past_excess_return_vs_spy
                FROM past_center pc
                CROSS JOIN past_mad pm
            ),
            bucketed_past AS (
                SELECT
                    b.id,
                    b.cluster_id,
                    b.fibonacci_lag_value,
                    b.past_excess_return_vs_spy,
                    b.future_fibonacci_lag_value,
                    b.future_excess_return_vs_spy,
                    ps.past_record_count,
                    ps.avg_past_excess_return_vs_spy,
                    ps.stddev_past_excess_return_vs_spy,
                    CASE
                        WHEN ps.stddev_past_excess_return_vs_spy IS NULL
                          OR ps.stddev_past_excess_return_vs_spy = 0
                        THEN NULL
                        ELSE (b.past_excess_return_vs_spy - ps.avg_past_excess_return_vs_spy) / ps.stddev_past_excess_return_vs_spy
                    END AS past_num_stddevs_away
                FROM base b
                CROSS JOIN past_stats ps
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

    # --- return_cluster_feature_set_current (latest obs per ticker/lag) ---
    bash_fsc, env_fsc = get_inference_dbt_bash_command(runtime_env, "return_cluster_feature_set_current")
    dbt_run_feature_set_current = BashOperator(
        task_id="dbt_run_return_cluster_feature_set_current",
        bash_command=bash_fsc,
        env=env_fsc,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_transition_scored_current (cross-sectional z-bucket) ---
    bash_tsc, env_tsc = get_inference_dbt_bash_command(runtime_env, "return_cluster_transition_scored_current")
    dbt_run_transition_scored_current = BashOperator(
        task_id="dbt_run_return_cluster_transition_scored_current",
        bash_command=bash_tsc,
        env=env_tsc,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_transition_scored (BATCH per id) ---
    batch_transition_scored = PythonOperator(
        task_id="batch_return_cluster_transition_scored",
        python_callable=batch_create_transition_scored,
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
    bash_lv, env_lv = get_inference_dbt_bash_command(runtime_env, "return_cluster_lag_viability")
    dbt_run_lag_viability = BashOperator(
        task_id="dbt_run_return_cluster_lag_viability",
        bash_command=bash_lv,
        env=env_lv,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_transition_confidence ---
    bash_tc, env_tc = get_inference_dbt_bash_command(runtime_env, "return_cluster_transition_confidence")
    dbt_run_transition_confidence = BashOperator(
        task_id="dbt_run_return_cluster_transition_confidence",
        bash_command=bash_tc,
        env=env_tc,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_cell_score ---
    bash_cs, env_cs = get_inference_dbt_bash_command(runtime_env, "return_cluster_cell_score")
    dbt_run_cell_score = BashOperator(
        task_id="dbt_run_return_cluster_cell_score",
        bash_command=bash_cs,
        env=env_cs,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_pair_recommendation (use_pair flag per id/past_lag/future_lag) ---
    bash_pr, env_pr = get_inference_dbt_bash_command(runtime_env, "return_cluster_pair_recommendation")
    dbt_run_pair_recommendation = BashOperator(
        task_id="dbt_run_return_cluster_pair_recommendation",
        bash_command=bash_pr,
        env=env_pr,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_ticker_pair_current (pair-grain landing: ticker x pair x bucket) ---
    bash_tpc, env_tpc = get_inference_dbt_bash_command(runtime_env, "return_cluster_ticker_pair_current")
    dbt_run_ticker_pair_current = BashOperator(
        task_id="dbt_run_return_cluster_ticker_pair_current",
        bash_command=bash_tpc,
        env=env_tpc,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_ticker_summary_current (ticker-grain rollup; dashboard landing) ---
    bash_tsc, env_tsc = get_inference_dbt_bash_command(runtime_env, "return_cluster_ticker_summary_current")
    dbt_run_ticker_summary_current = BashOperator(
        task_id="dbt_run_return_cluster_ticker_summary_current",
        bash_command=bash_tsc,
        env=env_tsc,
        append_env=True,
        do_xcom_push=False,
    )

    # --- return_cluster_ticker_global_action_current (global BUY/SELL/SKIP across clusterings) ---
    bash_tga, env_tga = get_inference_dbt_bash_command(runtime_env, "return_cluster_ticker_global_action_current")
    dbt_run_ticker_global_action_current = BashOperator(
        task_id="dbt_run_return_cluster_ticker_global_action_current",
        bash_command=bash_tga,
        env=env_tga,
        append_env=True,
        do_xcom_push=False,
    )

    # --- prediction_ledger (append-only forward log of live BUY/SELL calls) ---
    bash_pl, env_pl = get_inference_dbt_bash_command(runtime_env, "prediction_ledger")
    dbt_run_prediction_ledger = BashOperator(
        task_id="dbt_run_prediction_ledger",
        bash_command=bash_pl,
        env=env_pl,
        append_env=True,
        do_xcom_push=False,
    )

    # --- prediction_ledger_scored (running scorecard vs SPY) ---
    bash_pls, env_pls = get_inference_dbt_bash_command(runtime_env, "prediction_ledger_scored")
    dbt_run_prediction_ledger_scored = BashOperator(
        task_id="dbt_run_prediction_ledger_scored",
        bash_command=bash_pls,
        env=env_pls,
        append_env=True,
        do_xcom_push=False,
    )

    # --- trigger inference_backtest_dbt_models after prod refresh completes ---
    trigger_inference_backtest = TriggerDagRunOperator(
        task_id="trigger_inference_backtest_dbt_models",
        trigger_dag_id="inference_backtest_dbt_models",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # DEPENDENCIES
    dbt_run_feature_set >> batch_transition_scored >> [dbt_run_past_bucket_stats, dbt_run_future_bucket_stats] >> dbt_run_combined_bucket_stats
    dbt_run_feature_set >> dbt_run_lag_viability
    dbt_run_feature_set >> dbt_run_feature_set_current >> dbt_run_transition_scored_current
    [dbt_run_combined_bucket_stats, dbt_run_lag_viability] >> dbt_run_transition_confidence
    dbt_run_transition_confidence >> dbt_run_cell_score >> dbt_run_pair_recommendation
    [dbt_run_transition_scored_current, dbt_run_cell_score, dbt_run_pair_recommendation] >> dbt_run_ticker_pair_current >> dbt_run_ticker_summary_current >> dbt_run_ticker_global_action_current >> trigger_inference_backtest
    dbt_run_ticker_global_action_current >> dbt_run_prediction_ledger >> dbt_run_prediction_ledger_scored
