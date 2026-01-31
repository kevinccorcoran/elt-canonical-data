{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
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

{% do log("Current ENV: " ~ env_var('DB_DATABASE'), info=true) %}


-- One shared timestamp for the entire run (so processed_at is consistent across all rows)
WITH run_time AS (
    SELECT NOW() AS ts
),

-- PRIMARY PROVIDER: Massive (preferred source when available and not excluded)
massive AS (
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        'massive' AS source
    FROM {{ ref('ingest_massive_staging') }}
),

-- Exclusion lists allow you to explicitly force a provider choice per ticker
excluded_massive AS (
    SELECT ticker FROM {{ ref('excluded_tickers_massive') }}
),
excluded_yfinance AS (
    SELECT ticker FROM {{ ref('excluded_tickers_yfinance') }}
),

-- SECONDARY PROVIDER: YFinance (with special-case mapping for ^GSPC → SPY)
yfinance AS (
    SELECT
        "date",

        -- Normalize ticker names for downstream consistency
        CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker,

        -- Adjust ^GSPC pricing to match SPY scale (domain-specific normalization)
        CASE WHEN ticker = '^GSPC' THEN adj_close / 10.0 ELSE adj_close END AS adj_close,

        'yfinance' AS source,

        (CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END || '_' || "date") AS ticker_date_id
    FROM {{ ref('ingest_yfinance_staging') }} yf
    WHERE NOT EXISTS (SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = yf.ticker)
),

combined AS (

    -- 1) Use Massive when allowed (ticker not excluded from Massive)
    SELECT
        m."date",
        m.ticker,
        m.adj_close,
        m.source,
        rt.ts AS processed_at,
        m.ticker_date_id
    FROM massive m
    CROSS JOIN run_time rt
    WHERE NOT EXISTS (SELECT 1 FROM excluded_massive ex WHERE ex.ticker = m.ticker)

    UNION ALL

    -- 2) Massive excluded → use YFinance for that ticker (if YFinance not excluded)
    SELECT
        y."date",
        y.ticker,
        y.adj_close,
        y.source,
        rt.ts AS processed_at,
        y.ticker_date_id
    FROM yfinance y
    CROSS JOIN run_time rt
    WHERE EXISTS (SELECT 1 FROM excluded_massive ex WHERE ex.ticker = y.ticker)
      AND NOT EXISTS (SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = y.ticker)

    UNION ALL

    -- 3) Missing from Massive entirely → use YFinance
    -- (only if the ticker is not excluded from either provider)
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
      AND NOT EXISTS (SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = y.ticker)
      AND NOT EXISTS (SELECT 1 FROM excluded_massive ex WHERE ex.ticker = y.ticker)
)

SELECT *
FROM combined
