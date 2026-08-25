{{
  config(
    materialized='table',
    database=env_var('DB_DATABASE'),
    on_schema_change='sync_all_columns',
    post_hook=[
      """
      DROP INDEX IF EXISTS {{ this.schema }}.idx_{{ this.identifier }}_ticker_date;
      CREATE INDEX idx_{{ this.identifier }}_ticker_date
      ON {{ this }} (ticker, date);
      """,
      """
      DROP INDEX IF EXISTS {{ this.schema }}.idx_{{ this.identifier }}_ticker_date_desc;
      CREATE INDEX idx_{{ this.identifier }}_ticker_date_desc
      ON {{ this }} (ticker, date DESC);
      """,
      """
      DROP INDEX IF EXISTS {{ this.schema }}.idx_{{ this.identifier }}_ticker_date_id;
      CREATE INDEX idx_{{ this.identifier }}_ticker_date_id
      ON {{ this }} (ticker_date_id);
      """,
      """
      DROP INDEX IF EXISTS {{ this.schema }}.idx_{{ this.identifier }}_source;
      CREATE INDEX idx_{{ this.identifier }}_source
      ON {{ this }} (source);
      """,
      """
      DROP INDEX IF EXISTS {{ this.schema }}.idx_{{ this.identifier }}_processed_at_desc;
      CREATE INDEX idx_{{ this.identifier }}_processed_at_desc
      ON {{ this }} (processed_at DESC);
      """,
      "ANALYZE {{ this }};"
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
        source
    FROM {{ ref('ingest_massive_staging') }}
),

-- Per-(ticker, date-range) bad-data exclusions from the data_quality model
bad_ranges AS (
    SELECT ticker, bad_start, bad_end
    FROM {{ ref('excluded_tickers_massive') }}
),

yfinance_bad_ranges AS (
    SELECT ticker, bad_start, bad_end
    FROM {{ ref('excluded_tickers_yfinance') }}
),

-- Manual ticker exclusions: hand-curated list of tickers to drop entirely
-- (typically real-but-extreme price events that distort cluster z-scores).
manual_excluded AS (
    SELECT ticker FROM {{ ref('manual_excluded_tickers') }}
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
    WHERE NOT EXISTS (
        SELECT 1 FROM yfinance_bad_ranges br
        WHERE br.ticker = CASE WHEN yf.ticker = '^GSPC' THEN 'SPY' ELSE yf.ticker END
          AND yf."date" BETWEEN br.bad_start AND br.bad_end
    )
),

-- BASIS RESCALE (2026-08-24): the two providers adjust prices on different
-- bases (dividends, missed splits), so wherever yfinance fills history that
-- massive doesn't cover, the series used to jump at the hand-over — census
-- found 697/1406 junctions off by >10% (worst 137x), batch-fingerprinted at
-- 2003-09-10 and 2021-07/08. Measure the ratio on the earliest dates BOTH
-- providers cover (pure basis, no market move) and rescale every yfinance
-- fill row onto the massive basis. Guards: needs >=3 overlap days, and only
-- fires when the bases actually differ (>2%); otherwise factor stays 1.
-- CRITICAL: the massive side must exclude bad_ranges rows — those carry a
-- known-garbage basis (the reason yfinance fills them), and measuring the
-- factor on them would rescale good yfinance data onto garbage (caught in
-- pre-ship simulation: would have broken META/MS/DUK by 20-280x).
basis_overlap AS (
    SELECT
        y.ticker,
        m.adj_close / NULLIF(y.adj_close, 0) AS ratio,
        ROW_NUMBER() OVER (PARTITION BY y.ticker ORDER BY y."date") AS rn
    FROM yfinance y
    JOIN massive m
      ON m.ticker = y.ticker AND m."date" = y."date"
    WHERE y.adj_close > 0 AND m.adj_close > 0
      AND NOT EXISTS (
          SELECT 1 FROM bad_ranges br
          WHERE br.ticker = m.ticker
            AND m."date" BETWEEN br.bad_start AND br.bad_end
      )
),

-- A true basis factor is a CONSTANT, so the overlap ratios must be tight
-- (p75/p25 < 1.05). Wide spread means the series is internally inconsistent
-- (e.g. two interleaved listings under one ticker) and no single factor is
-- honest — leave those rows alone rather than over-correct (AROC case).
basis_factor AS (
    SELECT
        ticker,
        PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY ratio) AS factor,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ratio) AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ratio) AS p75,
        COUNT(*) AS n_overlap
    FROM basis_overlap
    WHERE rn <= 20
    GROUP BY ticker
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
    --    (rescaled onto the massive basis, see basis_factor above)
    SELECT
        y."date",
        y.ticker,
        y.adj_close * COALESCE(
            CASE WHEN bf.n_overlap >= 3 AND ABS(bf.factor - 1) > 0.02
                  AND bf.p75 / NULLIF(bf.p25, 0) < 1.05
                 THEN bf.factor END, 1.0) AS adj_close,
        y.source,
        rt.ts AS processed_at,
        y.ticker_date_id
    FROM yfinance y
    LEFT JOIN basis_factor bf ON bf.ticker = y.ticker
    CROSS JOIN run_time rt
    WHERE EXISTS (
        SELECT 1 FROM bad_ranges br
        WHERE br.ticker = y.ticker
          AND y."date" BETWEEN br.bad_start AND br.bad_end
    )

    UNION ALL

    -- 3) Use YFinance when Massive doesn't have the (ticker, date) at all
    --    AND it's not already covered by case 2 (date is not in a bad range)
    --    (rescaled onto the massive basis, see basis_factor above)
    SELECT
        y."date",
        y.ticker,
        y.adj_close * COALESCE(
            CASE WHEN bf.n_overlap >= 3 AND ABS(bf.factor - 1) > 0.02
                  AND bf.p75 / NULLIF(bf.p25, 0) < 1.05
                 THEN bf.factor END, 1.0) AS adj_close,
        y.source,
        rt.ts AS processed_at,
        y.ticker_date_id
    FROM yfinance y
    LEFT JOIN basis_factor bf ON bf.ticker = y.ticker
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

SELECT c.*
FROM combined c
WHERE NOT EXISTS (
    SELECT 1 FROM manual_excluded me WHERE me.ticker = c.ticker
)
