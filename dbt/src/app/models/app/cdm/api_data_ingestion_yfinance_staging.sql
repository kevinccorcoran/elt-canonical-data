{% do log("Current ENV: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='cdm',
    on_schema_change='sync_all_columns',
    post_hook=[
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
   ENRICHED Yfinance + YFinance Data WITH Index Membership
   -------------------------------------------------------------- */

WITH run_time AS (
    SELECT NOW() AS query_run_time
),

yfinance_base AS (
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        'yfinance' AS source,
        date_type
    FROM {{ source('cdm', 'api_data_ingestion_yfinance') }}
    WHERE "date" IS NOT NULL
      AND adj_close IS NOT NULL
),

ticker_index_summary AS (
    SELECT
        ticker
    FROM {{ ref('ticker_index_summary') }}
)

SELECT
    mb.date,
    mb.ticker,
    mb.adj_close,
    mb.processed_at,
    mb.ticker_date_id,
    mb.source,
    mb.date_type,
    rt.query_run_time
FROM yfinance_base mb
JOIN ticker_index_summary tis 
       ON mb.ticker = tis.ticker
CROSS JOIN run_time rt
