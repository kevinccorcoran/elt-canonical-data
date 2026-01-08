{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{ 
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_bucket_cluster ON {{ this }} (monthly_growth_vol_z_bucket_num, cluster_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at ON {{ this }} (processed_at)"
    ]
  ) 
}}

WITH run_time AS (
  SELECT now() AS ts
),

base AS (
  SELECT
    ticker,
    months_count,
    growth_pct_per_month,
    monthly_growth_vol_z_bucket,
    monthly_growth_vol_z_bucket_num,
    cluster_id
  FROM {{ source('analysis', 'ticker_cluster_segments') }}
),

aggregated AS (
  SELECT
    monthly_growth_vol_z_bucket,
    monthly_growth_vol_z_bucket_num,
    cluster_id,

    COUNT(*) AS ticker_count,

    ROUND(AVG(months_count))::int AS avg_months_count,
    MIN(months_count) AS min_months_count,
    MAX(months_count) AS max_months_count,

    -- Descriptive: simple average of per-ticker monthly growth
    ROUND(AVG(growth_pct_per_month)::numeric, 2)
      AS avg_ticker_growth_pct_per_month,

    -- Finance-correct: time-weighted compounded monthly growth
    ROUND(
      (
        (
          EXP(
            SUM(LN(1 + growth_pct_per_month / 100.0) * months_count)
            /
            SUM(months_count)
          ) - 1
        ) * 100
      )::numeric,
      2
    ) AS avg_weighted_growth,

    MIN(growth_pct_per_month) AS min_growth_pct_per_month,
    MAX(growth_pct_per_month) AS max_growth_pct_per_month,

    ARRAY_AGG(DISTINCT ticker ORDER BY ticker)::text[] AS tickers

  FROM base
  GROUP BY
    monthly_growth_vol_z_bucket,
    monthly_growth_vol_z_bucket_num,
    cluster_id
),

cte AS (
  SELECT
    monthly_growth_vol_z_bucket,
    monthly_growth_vol_z_bucket_num,
    cluster_id,
    ticker_count,

    CASE
      WHEN (100.0 * ticker_count / SUM(ticker_count) OVER (PARTITION BY monthly_growth_vol_z_bucket)) < 1
        THEN ROUND(
          (100.0 * ticker_count / SUM(ticker_count) OVER (PARTITION BY monthly_growth_vol_z_bucket))::numeric,
          1
        )
      ELSE ROUND(
        (100.0 * ticker_count / SUM(ticker_count) OVER (PARTITION BY monthly_growth_vol_z_bucket))::numeric
      )::int
    END AS percent_of_bucket,

    CASE
      WHEN (100.0 * ticker_count / SUM(ticker_count) OVER ()) < 1
        THEN ROUND(
          (100.0 * ticker_count / SUM(ticker_count) OVER ())::numeric,
          2
        )
      ELSE ROUND(
        (100.0 * ticker_count / SUM(ticker_count) OVER ())::numeric
      )::int
    END AS percent_of_total,

    avg_months_count,
    min_months_count,
    max_months_count,

    avg_ticker_growth_pct_per_month,
    avg_weighted_growth,

    min_growth_pct_per_month,
    max_growth_pct_per_month,

    tickers
  FROM aggregated
  ORDER BY
    monthly_growth_vol_z_bucket_num,
    cluster_id
),

final AS (
  SELECT
    ROW_NUMBER() OVER () AS id,
    cte.*,
    rt.ts AS processed_at
  FROM cte
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
  avg_weighted_growth,
  min_growth_pct_per_month,
  max_growth_pct_per_month,
  tickers,
  processed_at
FROM final
ORDER BY id