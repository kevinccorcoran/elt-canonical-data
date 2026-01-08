{% do log("Current ENV: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    schema = 'cdm',
    on_schema_change = 'sync_all_columns',
    post_hook = [
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_date_desc_ticker
        ON {{ this }} (date DESC, ticker);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_source
        ON {{ this }} (source);
      """
    ]
  )
}}



/* --------------------------------------------------------------
   ENRICHED: Massive YFinance Data + Index Membership
   - Filters: NVDA, SPY, INR only
   - Excludes tickers in quality.excluded_tickers_massive
   - Joins index membership (S&P, NASDAQ, etc.)
   - Adds query runtime
   - Only includes tickers with known indices
   Output: cdm.api_data_ingestion_massive_enriched
   -------------------------------------------------------------- */

WITH run_time AS (
    SELECT NOW() AS query_run_time
),

-- Base: filtered massive data with source tag
massive_base AS (
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        'massive' AS source,
        date_type
    FROM {{ source('cdm', 'api_data_ingestion_massive') }}
    WHERE "date" IS NOT NULL
      AND adj_close IS NOT NULL
),

-- Index membership lookup
ticker_index_summary AS (
    SELECT
        ticker
    FROM {{ ref('ticker_index_summary') }}
)

-- Final enriched result
SELECT
    mb."date",
    mb.ticker,
    mb.adj_close,
    mb.processed_at,
    mb.ticker_date_id,
    mb.source,
    mb.date_type,
    rt.query_run_time
FROM massive_base mb
JOIN ticker_index_summary tis 
    ON mb.ticker = tis.ticker
CROSS JOIN run_time rt
ORDER BY mb."date" DESC, mb.ticker