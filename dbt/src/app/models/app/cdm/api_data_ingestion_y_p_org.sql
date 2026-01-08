{% do log("Current ENV: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    schema='cdm',
    on_schema_change='sync_all_columns',
    post_hook=[
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_date
      ON {{ this }} (ticker, date);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_date_desc
      ON {{ this }} (ticker, date DESC);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_date_id
      ON {{ this }} (ticker_date_id);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_source
      ON {{ this }} (source);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at_desc
      ON {{ this }} (processed_at DESC);
      """
    ]
  )
}}

-- runtime timestamp for ALL rows (fixed)
WITH run_time AS (
    SELECT NOW() AS ts
),

-- PRIMARY PROVIDER: MASSIVE
massive AS (
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        'massive' AS source
    FROM {{ ref('api_data_ingestion_massive_staging') }}
),

-- EXCLUSION LISTS
excluded_massive AS (
    SELECT ticker FROM {{ ref('excluded_tickers_massive') }}
),
excluded_yfinance AS (
    SELECT ticker FROM {{ ref('excluded_tickers_yfinance') }}
),

-- SECONDARY PROVIDER: YFINANCE (with SPY adjustment + filter out bad tickers)
yfinance AS (
    SELECT
        "date",
        CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker,
        CASE WHEN ticker = '^GSPC' THEN adj_close / 10.0 ELSE adj_close END AS adj_close,
        'yfinance' AS source,
        (CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END || '_' || "date") AS ticker_date_id
    FROM {{ ref('api_data_ingestion_yfinance_staging') }} yf
    WHERE NOT EXISTS (
        SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = yf.ticker
    )
),

-- MAIN MERGE LOGIC
combined AS (
    -- 1. All Massive records where ticker is NOT excluded → use Massive
    SELECT
        m."date",
        m.ticker,
        m.adj_close,
        m.source,
        rt.ts AS processed_at,
        m.ticker_date_id
    FROM massive m
    CROSS JOIN run_time rt
    WHERE NOT EXISTS (
        SELECT 1 FROM excluded_massive ex WHERE ex.ticker = m.ticker
    )

    UNION ALL

    -- 2. For tickers excluded from Massive → use YFinance (but only if not also excluded from YFinance)
    SELECT
        y."date",
        y.ticker,
        y.adj_close,
        y.source,
        rt.ts AS processed_at,
        y.ticker_date_id
    FROM yfinance y
    CROSS JOIN run_time rt
    WHERE EXISTS (
        SELECT 1 FROM excluded_massive ex WHERE ex.ticker = y.ticker
    )
    AND NOT EXISTS (
        SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = y.ticker
    )

    UNION ALL

    -- 3. Tickers NOT in Massive at all → use YFinance (if not excluded from YFinance)
    SELECT
        y."date",
        y.ticker,
        y.adj_close,
        y.source,
        rt.ts AS processed_at,
        y.ticker_date_id
    FROM yfinance y
    CROSS JOIN run_time rt
    WHERE NOT EXISTS (
        SELECT 1 
        FROM massive m 
        WHERE m.ticker = y.ticker 
          AND m."date" = y."date"
    )
    AND NOT EXISTS (
        SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = y.ticker
    )
)

-- FINAL DEDUPLICATION (just in case of overlap, though logic above should prevent it)
SELECT DISTINCT ON (ticker, "date")
    ticker,
    "date",
    adj_close,
    source,
    processed_at,
    ticker_date_id
FROM combined
ORDER BY ticker, "date", 
         CASE WHEN source = 'massive' THEN 1 ELSE 2 END