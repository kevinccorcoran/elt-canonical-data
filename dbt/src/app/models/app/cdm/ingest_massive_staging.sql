{# Log the active target database at compile/run time for quick debugging #}
{% do log("Current ENV / DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',

    database = env_var('DB_DATABASE'),

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

-- --------------------------------------------------------------
-- Enriched: Massive data filtered to tickers that have index membership
-- Adds: source tag + query runtime (query_run_time)
-- Output: cdm.<model_name> (materialized table)
-- --------------------------------------------------------------

WITH run_time AS (
    -- Captures when the query ran (same value for every row in this run)
    SELECT NOW() AS query_run_time
),

massive_base AS (
    -- Base: massive ingestion table filtered for non-null essentials
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

ticker_index_summary AS (
    -- Keep only tickers that appear in your index membership summary
    -- (this is what enforces "only tickers with known indices")
    SELECT
        ticker
    FROM {{ ref('ticker_index_summary') }}
)

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