{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    post_hook = [
      """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """
    ]
  )
}}

/* --------------------------------------------------------------
   INVALID PRICE DETECTION
   Detects tickers where adj_close is NULL or zero.
   Source: staging.cdm.api_data_ingestion_yfinance_staging
   Output: quality.invalid_price_tickers
   -------------------------------------------------------------- */

WITH invalid_prices AS (
    SELECT DISTINCT
        ticker
    FROM {{ ref('api_data_ingestion_yfinance_staging') }} a
    WHERE adj_close IS NULL
       OR adj_close = 0
)

SELECT
    ticker
FROM invalid_prices
ORDER BY ticker