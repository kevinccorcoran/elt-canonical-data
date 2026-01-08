{{-
  config(
    materialized='incremental',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    incremental_strategy='append',
    on_schema_change='sync_all_columns',

    post_hook="
      create index if not exists idx_{{ this.identifier }}_main
        on {{ this }} (
          offset_type,
          ticker,
          observation_date,
          fibonacci_lag_value
        );
    "
  )
-}}


-- Record consistent runtime for entire load
WITH run_time AS (
    SELECT now() AS ts
),

base AS (
    SELECT
        ticker,
        ticker_date_id,
        observation_date,
        offset_observation_date,
        fibonacci_lag_value,
        offset_type,
        growth_rate_pct,
        excess_return_vs_spy
    FROM {{ ref('excess_return') }}
    {% if is_incremental() %}
      WHERE processed_at > COALESCE((SELECT MAX(processed_at) FROM {{ this }}), '0001-01-01')
    {% endif %}
)

SELECT 
    ticker,
    ticker_date_id,
    observation_date,
    offset_observation_date,
    fibonacci_lag_value,
    offset_type,
    ROUND(AVG(excess_return_vs_spy), 2) AS excess_return_vs_spy,
    MAX(rt.ts) AS processed_at
FROM base
CROSS JOIN run_time rt
GROUP BY 
    ticker, 
    ticker_date_id, 
    observation_date, 
    offset_observation_date, 
    fibonacci_lag_value, 
    offset_type