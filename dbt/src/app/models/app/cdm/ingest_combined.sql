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

-- PRIMARY PROVIDER: Massive (preferred when not in a bad range)
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

-- Per-(ticker, date-range) bad-data exclusions from the data_quality model
bad_ranges AS (
    SELECT ticker, bad_start, bad_end
    FROM {{ ref('excluded_tickers_massive') }}
),

excluded_yfinance AS (
    SELECT ticker FROM {{ ref('excluded_tickers_yfinance') }}
),

-- SECONDARY PROVIDER: YFinance (with special-case mapping for ^GSPC → SPY)
yfinance AS (
    SELECT
        "date",
        CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker,
        CASE WHEN ticker = '^GSPC' THEN adj_close / 10.0 ELSE adj_close END AS adj_close,
        'yfinance' AS source,
        (CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END || '_' || "date") AS ticker_date_id
    FROM {{ ref('ingest_yfinance_staging') }} yf
    WHERE NOT EXISTS (SELECT 1 FROM excluded_yfinance ey WHERE ey.ticker = yf.ticker)
),

combined AS (

    -- 1) Use Massive when (ticker, date) is NOT in any bad range
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
        SELECT 1 FROM bad_ranges br
        WHERE br.ticker = m.ticker
          AND m."date" BETWEEN br.bad_start AND br.bad_end
    )

    UNION ALL

    -- 2) Use YFinance to fill in dates that ARE in a bad range
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
        SELECT 1 FROM bad_ranges br
        WHERE br.ticker = y.ticker
          AND y."date" BETWEEN br.bad_start AND br.bad_end
    )

    UNION ALL

    -- 3) Use YFinance when Massive doesn't have the (ticker, date) at all
    --    AND it's not already covered by case 2 (date is not in a bad range)
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
              SELECT 1 FROM bad_ranges br
              WHERE br.ticker = y.ticker
                AND y."date" BETWEEN br.bad_start AND br.bad_end
          )
)

SELECT *
FROM combined
