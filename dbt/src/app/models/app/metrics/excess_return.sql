{{ 
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='metrics',
    post_hook=[
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_spy_lookup
      ON {{ this }} (ticker, observation_date, offset_observation_date, offset_type)
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_order
      ON {{ this }} (ticker_date_id, offset_observation_date DESC)
      """
    ]
  ) 
}}

-- consistent runtime for the full model
WITH run_time AS (
    SELECT now() AS ts
),

base AS (
    SELECT
        fgw.ticker,
        fgw.ticker_date_id,
        fgw.observation_date,
        fgw.offset_observation_date,
        fgw.fibonacci_lag_value,
        fgw.offset_type,
        fgw.growth_rate_pct,
        spy.growth_rate_pct AS growth_rate_pct_base,        -- SPY baseline
        ROUND(fgw.growth_rate_pct - spy.growth_rate_pct, 4) AS excess_return_vs_spy,
        rt.ts AS processed_at                                -- timestamp added here
    FROM {{ ref('fibonacci_offset_growth_rates') }} fgw
    JOIN {{ ref('fibonacci_offset_growth_rates') }} spy
        ON fgw.observation_date = spy.observation_date
       AND fgw.offset_observation_date = spy.offset_observation_date
       AND fgw.offset_type = spy.offset_type
    CROSS JOIN run_time rt
    WHERE fgw.ticker <> 'SPY'
      AND spy.ticker = 'SPY'
)

SELECT *
FROM base
ORDER BY ticker_date_id, offset_observation_date DESC
