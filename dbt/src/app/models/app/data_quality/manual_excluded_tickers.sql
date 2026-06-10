{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker ON {{ this }} (ticker)"
    ]
  )
}}

-- Hand-curated ticker exclusion list. Use for tickers with real but extreme
-- price events that distort downstream cluster distributions (e.g. one-day
-- 400%+ spikes from acquisitions or biotech catalysts). Filtered out of
-- ingest_combined entirely, so all downstream models stop seeing them.
--
-- To add a ticker: add a row below and re-run cdm_ingest_massive + the
-- inference DAG. To remove: delete the row.

SELECT ticker, reason FROM (VALUES
    ('LBPH', '436% one-day excess return on 2024-04-01 (Lundbeck acquisition rumor) distorts cluster 3 z-scores')
) AS t(ticker, reason)
