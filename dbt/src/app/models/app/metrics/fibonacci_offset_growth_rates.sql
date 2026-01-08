{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    post_hook=[
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_fib_lag_value
      ON {{ this }} (fibonacci_lag_value DESC)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_offset_type_fib_lag
      ON {{ this }} (offset_type, fibonacci_lag_value DESC)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
      ON {{ this }} (ticker ASC)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at
      ON {{ this }} (processed_at)
      """
    ]
  )
}}

{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

WITH run_time AS (
    SELECT now() AS ts
),

union_ AS (
    SELECT * FROM {{ ref('fibonacci_past_offset_avg_prices') }}
    UNION ALL
    SELECT * FROM {{ ref('fibonacci_future_offset_avg_prices') }}
),

ranked AS (
    SELECT
        ticker,
        observation_date,
        ticker_date_id,
        offset_observation_date,
        fibonacci_index,
        fibonacci_lag_value,
        latest_adj_close,
        offset_adj_close,
        CASE
            WHEN offset_type = 'past' THEN
                ROW_NUMBER() OVER (
                    PARTITION BY ticker_date_id, offset_type
                    ORDER BY observation_date DESC, offset_observation_date DESC
                )
            ELSE
                ROW_NUMBER() OVER (
                    PARTITION BY ticker_date_id, offset_type
                    ORDER BY observation_date ASC, offset_observation_date ASC
                )
        END AS row_in_group,
        CASE
            WHEN offset_type = 'past' THEN
                FIRST_VALUE(offset_adj_close) OVER (
                    PARTITION BY ticker_date_id, offset_type
                    ORDER BY observation_date DESC, offset_observation_date DESC
                )
            ELSE
                FIRST_VALUE(offset_adj_close) OVER (
                    PARTITION BY ticker_date_id, offset_type
                    ORDER BY observation_date ASC, offset_observation_date ASC
                )
        END AS baseline_lagged_adj_close,
        offset_type
    FROM union_
),

fib_lag_growth_from_history AS (
    SELECT
        ticker,
        observation_date,
        ticker_date_id,
        offset_observation_date,
        fibonacci_index,
        fibonacci_lag_value,
        latest_adj_close,
        offset_adj_close,
        row_in_group,
        baseline_lagged_adj_close,
        CASE
            WHEN row_in_group = 1 OR offset_adj_close = 0 OR baseline_lagged_adj_close = 0 THEN NULL
            WHEN offset_type = 'past' THEN
                ROUND(((baseline_lagged_adj_close / offset_adj_close) - 1) * 100, 2)
            ELSE
                ROUND(((offset_adj_close / baseline_lagged_adj_close) - 1) * 100, 2)
        END AS growth_rate_pct,
        offset_type
    FROM ranked
    ORDER BY ticker, observation_date DESC, offset_observation_date DESC
),

fib_growth_window AS (
    SELECT 
        ticker,
        ticker_date_id,
        observation_date,
        offset_observation_date,
        fibonacci_lag_value,
        offset_type,
        baseline_lagged_adj_close,
        offset_adj_close,
        COALESCE(growth_rate_pct, 0) AS growth_rate_pct
    FROM fib_lag_growth_from_history
),

final_output AS (
    SELECT
        fg.*,
        rt.ts AS processed_at
    FROM fib_growth_window fg
    CROSS JOIN run_time rt
)

SELECT
    ticker,
    ticker_date_id,
    observation_date,
    offset_observation_date,
    fibonacci_lag_value,
    offset_type,
    growth_rate_pct,
    processed_at
FROM final_output
WHERE fibonacci_lag_value <> 0
  AND growth_rate_pct <> 0
ORDER BY ticker_date_id, offset_observation_date DESC