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

-- PRIMARY PROVIDER: Massive (preferred when not in a bad range), on a
-- total-return basis like the yfinance history it is joined to
-- (ingest_massive_total_return: dividends reinvested; the data_quality rules
-- still read the split-only ingest_massive_staging)
massive AS (
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        source
    FROM {{ ref('ingest_massive_total_return') }}
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

-- yfinance closes carry dividends up to each ticker's last yfinance date (the
-- fetch), the massive bars up to today. This is the massive dividend factor
-- on the first massive bar on or after that date: the dividends paid since
-- (with no massive bar in between there is no dividend in between). It puts
-- both providers on one basis, so the basis rescale below sees only real
-- breaks again; measured on the overlap instead, the dividends since the
-- fetch (often under its 2% guard) became a step at each hand-over.
yfinance_dividend_anchor AS (
    SELECT a.ticker, COALESCE(m.dividend_factor, 1.0) AS dividend_factor
    FROM (
        SELECT ticker, MAX("date") AS last_date
        FROM {{ ref('ingest_yfinance_staging') }}
        WHERE ticker <> '^GSPC'
          AND (date_type = 'natural' OR date_type IS NULL)
        GROUP BY ticker
    ) a
    LEFT JOIN LATERAL (
        SELECT t.dividend_factor
        FROM {{ ref('ingest_massive_total_return') }} t
        WHERE t.ticker = a.ticker
          AND t."date" >= a.last_date
        ORDER BY t."date"
        LIMIT 1
    ) m ON TRUE
),

-- SECONDARY PROVIDER: YFinance (with special-case mapping for ^GSPC → SPY).
-- The ^GSPC rows are the S&P 500 price index; spy_proxy_total_return_factor
-- adds SPY's dividends from 1993 so they match the total-return massive SPY.
yfinance AS (
    SELECT
        yf."date",
        yf.ticker,
        yf.adj_close * COALESCE(da.dividend_factor, 1.0) AS adj_close,
        'yfinance' AS source,
        (yf.ticker || '_' || yf."date") AS ticker_date_id
    FROM {{ ref('ingest_yfinance_staging') }} yf
    LEFT JOIN yfinance_dividend_anchor da
      ON da.ticker = yf.ticker
    WHERE yf.ticker <> '^GSPC'
      AND NOT EXISTS (
          SELECT 1 FROM yfinance_bad_ranges br
          WHERE br.ticker = yf.ticker
            AND yf."date" BETWEEN br.bad_start AND br.bad_end
      )

    UNION ALL

    SELECT
        yf."date",
        'SPY' AS ticker,
        yf.adj_close / 10.0 * COALESCE(sp.factor, 1.0) AS adj_close,
        'yfinance' AS source,
        ('SPY_' || yf."date") AS ticker_date_id
    FROM {{ ref('ingest_yfinance_staging') }} yf
    LEFT JOIN {{ ref('spy_proxy_total_return_factor') }} sp
      ON yf."date" < sp.valid_to
     AND (sp.valid_from IS NULL OR yf."date" >= sp.valid_from)
    WHERE yf.ticker = '^GSPC'
      AND NOT EXISTS (
          SELECT 1 FROM yfinance_bad_ranges br
          WHERE br.ticker = 'SPY'
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
