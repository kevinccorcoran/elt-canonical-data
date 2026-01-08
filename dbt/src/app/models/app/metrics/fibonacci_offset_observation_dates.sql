{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    post_hook=[
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_obs_date
      ON {{ this }} (ticker, observation_date)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_fibonacci_lag_value
      ON {{ this }} (fibonacci_lag_value DESC)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_fib_lag
      ON {{ this }} (ticker, fibonacci_lag_value)
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

intermediate_fibonacci_base AS (
    SELECT *
    FROM {{ ref('intermediate_fibonacci_base') }}
),

fib_seq AS (
    SELECT *
    FROM {{ source('raw', 'fib_seq') }}
),

cdm_api_data_ingestion AS (
    SELECT
        a."date",
        a.ticker,
        processed_at,
        adj_close,
        a.ticker_date_id
    FROM {{ ref('api_data_ingestion_y_p') }} a),

fibonacci_lagged_returns AS (
    SELECT
        jb.ticker,
        jb.ticker_date_id,
        jb.fibonacci_lag,
        jb.latest_observation,
        jb.latest_adj_close,
        jb.observation_date,
        f.adj_close AS historical_adj_close
    FROM intermediate_fibonacci_base jb
    LEFT JOIN cdm_api_data_ingestion f ON f.ticker_date_id = jb.ticker_date_id
),

observation_fib_lags AS (
    SELECT
        fr.ticker,
        fr.ticker_date_id,
        fr.observation_date,
        fr.latest_adj_close,
        fs.n AS fibonacci_index,
        fs.fibonacci_lag_value
    FROM fibonacci_lagged_returns fr
    CROSS JOIN fib_seq fs
    ORDER BY fr.ticker, fr.observation_date DESC, fs.n
),

offset_observation_date AS (
    SELECT
        ofl.ticker,
        ofl.ticker_date_id,
        ofl.observation_date,
        ofl.fibonacci_index,
        ofl.fibonacci_lag_value,
        ofl.latest_adj_close,
        (ofl.observation_date - (ofl.fibonacci_lag_value || ' months')::interval)::date AS offset_observation_date,
        rt.ts AS processed_at
    FROM observation_fib_lags ofl
    CROSS JOIN run_time rt
)

SELECT distinct *
FROM offset_observation_date