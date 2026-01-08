{{-
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    on_schema_change='sync_all_columns',
    post_hook="
      create index if not exists idx_{{ this.identifier }}_ticker_obs
        on {{ this }} (ticker, observation_date);
      create index if not exists idx_{{ this.identifier }}_lag
        on {{ this }} (fibonacci_lag_value);
      create index if not exists idx_{{ this.identifier }}_processed_at
        on {{ this }} (processed_at);
    "
  )
-}}

{% do log("Current ENV: " ~ env_var('DB_DATABASE'), info=true) %}

WITH run_time AS (
  SELECT now() AS ts
),

past AS (
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
),

joined AS (
  SELECT
    p.ticker,
    p.observation_date,
    p.fibonacci_lag_value,
    p.past_excess_return_vs_spy,
    f.fibonacci_lag_value AS future_fibonacci_lag_value,
    f.future_excess_return_vs_spy
  FROM past p
  JOIN future f
    ON  p.ticker          = f.ticker
    AND p.observation_date = f.observation_date
),

labeled AS (
  SELECT
    j.*,
    ROUND(
      (j.past_excess_return_vs_spy - AVG(j.past_excess_return_vs_spy) OVER w)
      / NULLIF(STDDEV_POP(j.past_excess_return_vs_spy) OVER w, 0),
      2
    ) AS num_stddevs_away
  FROM joined j
  WINDOW w AS (PARTITION BY j.ticker, j.fibonacci_lag_value)
),

final_output AS (
  SELECT
    l.*,
    CASE
      WHEN num_stddevs_away < -3 THEN '< -3 SD'
      WHEN num_stddevs_away < -2 THEN '-3 to -2 SD'
      WHEN num_stddevs_away < -1 THEN '-2 to -1 SD'
      WHEN num_stddevs_away <  0 THEN '-1 to 0 SD'
      WHEN num_stddevs_away <  1 THEN '0 to 1 SD'
      WHEN num_stddevs_away <  2 THEN '1 to 2 SD'
      WHEN num_stddevs_away <  3 THEN '2 to 3 SD'
      ELSE '> 3 SD'
    END AS stddev_bucket,
    rt.ts AS processed_at,
    to_char(rt.ts, 'YYYY-MM') AS calendar_month
  FROM labeled l
  CROSS JOIN run_time rt
)

SELECT
  ticker,
  observation_date,
  fibonacci_lag_value,
  past_excess_return_vs_spy,
  future_fibonacci_lag_value,
  future_excess_return_vs_spy,
  num_stddevs_away,
  stddev_bucket,
  CASE
    WHEN stddev_bucket = '< -3 SD' THEN 8
    WHEN stddev_bucket = '-3 to -2 SD' THEN 7
    WHEN stddev_bucket = '-2 to -1 SD' THEN 6
    WHEN stddev_bucket = '-1 to 0 SD' THEN 5
    WHEN stddev_bucket = '0 to 1 SD' THEN 4
    WHEN stddev_bucket = '1 to 2 SD' THEN 3
    WHEN stddev_bucket = '2 to 3 SD' THEN 2
    WHEN stddev_bucket = '> 3 SD' THEN 1
    ELSE 0
  END AS stddev_bucket_num,
  processed_at,
  calendar_month
FROM final_output
