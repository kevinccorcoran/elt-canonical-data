{% do log("Current ENV: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database     = env_var('DB_DATABASE'),
    schema       = 'inference',
    on_schema_change = 'sync_all_columns',
    post_hook = [
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_id_fibo_pair
        ON {{ this }} (id, fibonacci_lag_value, future_fibonacci_lag_value);
      """
    ]
  )
}}

-- --------------------------------------------------------------
-- Return likelihood bucket contribution & pair-level estimation
-- --------------------------------------------------------------

WITH run_time AS (
    SELECT now() AS processed_at
),

-- --------------------------------------------------------------
-- Base data (ref, not hard-coded)
-- --------------------------------------------------------------
base_data AS (
    SELECT
        id,
        fibonacci_lag_value,
        future_fibonacci_lag_value,
        base_stddev_bucket,
        base_stddev_bucket_num,
        record_count,
        record_pct_per_id,
        median_future_excess_return_vs_spy
    FROM {{ ref('return_likelihood_matrix') }}
),

-- --------------------------------------------------------------
-- Bucket-level classification & expected contribution
-- --------------------------------------------------------------
expected_contribution AS (
    SELECT
        *,
        CASE bucket_role
            WHEN 'CORE_PLUS'     THEN 1
            WHEN 'CORE'          THEN 2
            WHEN 'TAIL_UPSIDE'   THEN 3
            WHEN 'SECONDARY'     THEN 4
            WHEN 'TAIL_DOWNSIDE' THEN 5
        END AS bucket_role_rank
    FROM (
        SELECT
            id,
            fibonacci_lag_value,
            future_fibonacci_lag_value,
            base_stddev_bucket,
            base_stddev_bucket_num,
            record_count,

            -- raw values
            record_pct_per_id AS record_pct_raw,
            median_future_excess_return_vs_spy
                AS median_future_excess_return_raw,

            -- display values
            ROUND(record_pct_per_id::numeric, 2)
                AS record_pct_display,
            ROUND(median_future_excess_return_vs_spy::numeric, 2)
                AS median_future_excess_return_display,

            -- expected contribution
            ROUND(
                (record_pct_per_id / 100.0)
                * median_future_excess_return_vs_spy,
                2
            ) AS bucket_expected_return,

            -- bucket role classification
            CASE
                WHEN record_pct_per_id < 1
                     AND median_future_excess_return_vs_spy >= 10
                    THEN 'TAIL_UPSIDE'
                WHEN record_pct_per_id < 1
                     AND median_future_excess_return_vs_spy <= -5
                    THEN 'TAIL_DOWNSIDE'
                WHEN record_pct_per_id >= 10
                     AND median_future_excess_return_vs_spy >= 15
                    THEN 'CORE_PLUS'
                WHEN record_pct_per_id >= 5
                     AND median_future_excess_return_vs_spy > 0
                    THEN 'CORE'
                ELSE 'SECONDARY'
            END AS bucket_role
        FROM base_data
        WHERE (fibonacci_lag_value, future_fibonacci_lag_value) IN (
            (1, 2),
            (1, 4),
            (2, 7),
            (4, 7),
            (4, 12),
            (12, 20),
            (12, 33),
            (33, 54)
        )
    ) t
),

-- --------------------------------------------------------------
-- Pair-level aggregation (final output)
-- --------------------------------------------------------------
estimated_value AS (
    SELECT
        ec.id,
        ec.fibonacci_lag_value,
        ec.future_fibonacci_lag_value,
        SUM(ec.bucket_expected_return)
            AS estimated_future_excess_return_vs_spy,
        SUM(ec.record_pct_raw)
            AS total_record_pct_per_id,
        SUM(ec.record_count)
            AS total_record_count,
        rt.processed_at
    FROM expected_contribution ec
    CROSS JOIN run_time rt
    GROUP BY
        ec.id,
        ec.fibonacci_lag_value,
        ec.future_fibonacci_lag_value,
        rt.processed_at
)

-- --------------------------------------------------------------
-- Final model select
-- --------------------------------------------------------------
SELECT
    *
FROM estimated_value