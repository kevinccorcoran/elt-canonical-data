{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    post_hook=[
      "
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_fibonacci_pair
      ON {{ this }} (fibonacci_lag_value, future_fibonacci_lag_value)
      ",
      "
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_cluster_bucket
      ON {{ this }} (id, past_excess_return_z_bucket_num)
      ",
      "
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at
      ON {{ this }} (processed_at DESC)
      "
    ]
  )
}}

WITH run_time AS (
    SELECT now() AS ts
),

/* ───────────────────────── Expanded clusters ───────────────────────── */
expanded AS (
    SELECT DISTINCT
      id,
      monthly_growth_vol_z_bucket,
      monthly_growth_vol_z_bucket_num,
      cluster_id,
      ticker_count,
      percent_of_bucket,
      percent_of_total,
      avg_months_count,
      min_months_count,
      max_months_count,
      --avg_growth_pct_per_month,
      min_growth_pct_per_month,
      max_growth_pct_per_month,
      unnest(tickers) AS ticker
    FROM {{ ref('ticker_cluster_volatility_summary') }}
),

/* ───────────────────────── Base excess return data ───────────────────────── */
base AS (
    SELECT
        ticker,
        observation_date,
        fibonacci_lag_value,
        past_excess_return_vs_spy,
        future_fibonacci_lag_value,
        future_excess_return_vs_spy,
        num_stddevs_away,
        past_excess_return_z_bucket,
        past_excess_return_z_bucket_num,
        processed_at,
        calendar_month
    FROM {{ ref('excess_return_scored') }}
),

/* ───────────────────────── Join clusters to returns ───────────────────────── */
joined AS (
    SELECT
        e.id,
        e.monthly_growth_vol_z_bucket,
        e.monthly_growth_vol_z_bucket_num,
        e.cluster_id,
        e.ticker_count,
        e.percent_of_bucket,
        e.percent_of_total,
        e.avg_months_count,
        e.min_months_count,
        e.max_months_count,

        -- Cluster descriptors (monthly growth)
        --e.avg_growth_pct_per_month,
        e.min_growth_pct_per_month,
        e.max_growth_pct_per_month,

        b.ticker,
        b.observation_date,
        b.fibonacci_lag_value,
        b.past_excess_return_vs_spy,
        b.future_fibonacci_lag_value,
        b.future_excess_return_vs_spy,
        b.num_stddevs_away,
        b.past_excess_return_z_bucket,
        b.past_excess_return_z_bucket_num,
        b.processed_at,
        b.calendar_month
    FROM base b
    JOIN expanded e
      ON b.ticker = e.ticker
),

/* ───────────────────────── Aggregate by lag pair ───────────────────────── */
grouped AS (
    SELECT
        id,
        monthly_growth_vol_z_bucket,
        monthly_growth_vol_z_bucket_num,
        cluster_id,
        ticker_count,
        percent_of_bucket,
        percent_of_total,
        avg_months_count,
        min_months_count,
        max_months_count,

        -- Carry cluster descriptors through
        --avg_growth_pct_per_month,
        min_growth_pct_per_month,
        max_growth_pct_per_month,

        fibonacci_lag_value,
        future_fibonacci_lag_value,
        past_excess_return_z_bucket,
        past_excess_return_z_bucket_num,

        COUNT(*) AS record_count,

        ROUND(AVG(past_excess_return_vs_spy)::numeric, 2)
            AS avg_past_excess_return_vs_spy,

        ROUND(AVG(future_excess_return_vs_spy)::numeric, 2)
            AS avg_future_excess_return_vs_spy,

        ROUND(
            percentile_cont(0.5)
            WITHIN GROUP (ORDER BY past_excess_return_vs_spy)::numeric,
            2
        ) AS median_past_excess_return_vs_spy,

        ROUND(
            percentile_cont(0.5)
            WITHIN GROUP (ORDER BY future_excess_return_vs_spy)::numeric,
            2
        ) AS median_future_excess_return_vs_spy,

        ROUND(
            percentile_cont(0.05)
            WITHIN GROUP (ORDER BY past_excess_return_vs_spy)::numeric,
            2
        ) AS p05_past_excess_return_vs_spy,

        ROUND(
            percentile_cont(0.95)
            WITHIN GROUP (ORDER BY past_excess_return_vs_spy)::numeric,
            2
        ) AS p95_past_excess_return_vs_spy,

        CASE
            WHEN ABS(
                percentile_cont(0.05)
                WITHIN GROUP (ORDER BY future_excess_return_vs_spy)
            ) < 1
            THEN ROUND(
                percentile_cont(0.05)
                WITHIN GROUP (ORDER BY future_excess_return_vs_spy)::numeric,
                2
            )
            ELSE ROUND(
                percentile_cont(0.05)
                WITHIN GROUP (ORDER BY future_excess_return_vs_spy)::numeric
            )::int
        END AS p05_future_excess_return_vs_spy,

        CASE
            WHEN ABS(
                percentile_cont(0.95)
                WITHIN GROUP (ORDER BY future_excess_return_vs_spy)
            ) < 1
            THEN ROUND(
                percentile_cont(0.95)
                WITHIN GROUP (ORDER BY future_excess_return_vs_spy)::numeric,
                2
            )
            ELSE ROUND(
                percentile_cont(0.95)
                WITHIN GROUP (ORDER BY future_excess_return_vs_spy)::numeric
            )::int
        END AS p95_future_excess_return_vs_spy

    FROM joined
    GROUP BY
        id,
        monthly_growth_vol_z_bucket,
        monthly_growth_vol_z_bucket_num,
        cluster_id,
        ticker_count,
        percent_of_bucket,
        percent_of_total,
        avg_months_count,
        min_months_count,
        max_months_count,
        --avg_growth_pct_per_month,
        min_growth_pct_per_month,
        max_growth_pct_per_month,
        fibonacci_lag_value,
        future_fibonacci_lag_value,
        past_excess_return_z_bucket,
        past_excess_return_z_bucket_num
),

/* ───────────────────────── Range calculations ───────────────────────── */
ranges AS (
    SELECT
        g.*,
        (p95_past_excess_return_vs_spy - p05_past_excess_return_vs_spy)
            AS past_range_width,
        (p95_future_excess_return_vs_spy - p05_future_excess_return_vs_spy)
            AS future_range_width,
        MIN(p05_past_excess_return_vs_spy) OVER () AS global_min_past,
        MAX(p95_past_excess_return_vs_spy) OVER () AS global_max_past,
        MIN(p05_future_excess_return_vs_spy) OVER () AS global_min_future,
        MAX(p95_future_excess_return_vs_spy) OVER () AS global_max_future
    FROM grouped g
),

/* ───────────────────────── Final ───────────────────────── */
final AS (
    SELECT
        *,
        rt.ts AS processed_at
    FROM ranges
    CROSS JOIN run_time rt
)

SELECT
    id,
    monthly_growth_vol_z_bucket,
    monthly_growth_vol_z_bucket_num,
    cluster_id,
    ticker_count,
    percent_of_bucket,
    percent_of_total,
    avg_months_count,
    min_months_count,
    max_months_count,
    --avg_growth_pct_per_month,
    min_growth_pct_per_month,
    max_growth_pct_per_month,

    fibonacci_lag_value,
    future_fibonacci_lag_value,
    past_excess_return_z_bucket,
    past_excess_return_z_bucket_num,
    record_count,

    -- % of total records for this lag pair
    CASE
        WHEN (
            100.0 * record_count
            / NULLIF(
                SUM(record_count) OVER (
                    PARTITION BY fibonacci_lag_value, future_fibonacci_lag_value
                ),
                0
            )
        ) < 1
        THEN ROUND(
            (
                100.0 * record_count
                / NULLIF(
                    SUM(record_count) OVER (
                        PARTITION BY fibonacci_lag_value, future_fibonacci_lag_value
                    ),
                    0
                )
            )::numeric,
            3
        )
        ELSE ROUND(
            (
                100.0 * record_count
                / NULLIF(
                    SUM(record_count) OVER (
                        PARTITION BY fibonacci_lag_value, future_fibonacci_lag_value
                    ),
                    0
                )
            )::numeric
        )::int
    END AS record_pct_total,

    -- % of records within cluster ID
    CASE
        WHEN (
            100.0 * record_count
            / NULLIF(
                SUM(record_count) OVER (
                    PARTITION BY id, fibonacci_lag_value, future_fibonacci_lag_value
                ),
                0
            )
        ) < 1
        THEN ROUND(
            (
                100.0 * record_count
                / NULLIF(
                    SUM(record_count) OVER (
                        PARTITION BY id, fibonacci_lag_value, future_fibonacci_lag_value
                    ),
                    0
                )
            )::numeric,
            2
        )
        ELSE ROUND(
            (
                100.0 * record_count
                / NULLIF(
                    SUM(record_count) OVER (
                        PARTITION BY id, fibonacci_lag_value, future_fibonacci_lag_value
                    ),
                    0
                )
            )::numeric
        )::int
    END AS record_pct_per_id,

    past_range_width,
    p05_past_excess_return_vs_spy,
    p95_past_excess_return_vs_spy,
    median_future_excess_return_vs_spy,
    p05_future_excess_return_vs_spy,
    p95_future_excess_return_vs_spy,
    future_range_width,
    processed_at
FROM final
WHERE fibonacci_lag_value <> future_fibonacci_lag_value
ORDER BY id, past_excess_return_z_bucket_num