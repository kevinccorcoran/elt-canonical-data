{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    post_hook=[
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_date_id
      ON {{ this }} (ticker_date_id)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_offset_type_fib_lag
      ON {{ this }} (offset_type, fibonacci_lag_value DESC)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at
      ON {{ this }} (processed_at)
      """
    ]
  )
}}

WITH run_time AS (
  SELECT now() AS ts
),

api_data_ingestion_y_p AS (
  SELECT
    "date",
    ticker,
    processed_at,
    adj_close,
    ticker_date_id
  FROM {{ ref('api_data_ingestion_y_p') }}
),

offset_observation_date AS (
  SELECT
    ticker,
    observation_date,
    fibonacci_index,
    fibonacci_lag_value,
    latest_adj_close,
    (observation_date + (fibonacci_lag_value || ' months')::interval)::date AS offset_observation_date
  FROM {{ ref('fibonacci_offset_observation_dates') }}
),

-- 5 relative offsets in months: -6, -3, 0, +3, +6
offsets AS (
  SELECT unnest(ARRAY[-6, -3, 0, 3, 6])::int AS month_lag
),

-- expand each offset_observation_date into 5 target dates
expanded AS (
  SELECT
    fud.ticker,
    fud.observation_date,
    fud.offset_observation_date,
    fud.fibonacci_index,
    fud.fibonacci_lag_value,
    fud.latest_adj_close,
    (fud.offset_observation_date + (offsets.month_lag || ' months')::interval)::date AS target_date
  FROM offset_observation_date AS fud
  CROSS JOIN offsets
),

-- join once and aggregate once per (ticker, observation_date, offset_observation_date, fib fields)
joined_future_prices AS (
  SELECT
    e.ticker,
    e.observation_date,
    e.offset_observation_date,
    e.fibonacci_index,
    e.fibonacci_lag_value,
    e.latest_adj_close,
    ROUND(AVG(a.adj_close)::numeric, 2) AS offset_adj_close,
    'future' AS offset_type,
    rt.ts AS processed_at
  FROM expanded AS e
  LEFT JOIN api_data_ingestion_y_p AS a
    ON a.ticker = e.ticker
   AND a.date   = e.target_date
  CROSS JOIN run_time AS rt
  GROUP BY
    e.ticker,
    e.observation_date,
    e.offset_observation_date,
    e.fibonacci_index,
    e.fibonacci_lag_value,
    e.latest_adj_close,
    rt.ts
)

SELECT
  ticker,
  observation_date,
  offset_observation_date,
  (ticker || '_' || observation_date)::text AS ticker_date_id,
  fibonacci_index,
  fibonacci_lag_value,
  latest_adj_close,
  offset_adj_close,
  offset_type,
  processed_at
FROM joined_future_prices
