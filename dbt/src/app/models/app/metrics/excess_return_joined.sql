{{-
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    on_schema_change='sync_all_columns',
    post_hook="
    create index if not exists idx_{{ this.identifier }}_ticker_obs
      on {{ this }} (ticker, observation_date);

    create index if not exists idx_{{ this.identifier }}_lag_pair
      on {{ this }} (fibonacci_lag_value, future_fibonacci_lag_value);
  "
  )
-}}

WITH past AS (
  SELECT
    ticker,
    observation_date,
    offset_observation_date,
    fibonacci_lag_value,
    excess_return_vs_spy AS past_excess_return_vs_spy
  FROM {{ ref('excess_return_inc') }}
  WHERE offset_type = 'past'
),

future AS (
  SELECT
    ticker,
    observation_date,
    offset_observation_date,
    fibonacci_lag_value,
    excess_return_vs_spy AS future_excess_return_vs_spy
  FROM {{ ref('excess_return_inc') }}
  WHERE offset_type = 'future'
)

SELECT
  p.ticker,
  p.observation_date,
  p.offset_observation_date,
  p.fibonacci_lag_value,
  p.past_excess_return_vs_spy,
  f.fibonacci_lag_value AS future_fibonacci_lag_value,
  f.future_excess_return_vs_spy
FROM past p
JOIN future f
  ON p.ticker = f.ticker
 AND p.observation_date = f.observation_date