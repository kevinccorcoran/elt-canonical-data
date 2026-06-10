{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    post_hook = [
      """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """,
      """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_range
        ON {{ this }} (ticker, bad_start, bad_end);
      """
    ]
  )
}}

/* --------------------------------------------------------------
   BAD-DATA DATE RANGES IN MASSIVE FEED

   Emits one row per (ticker, bad_start, bad_end) date range that
   should be dropped from the massive source. Downstream
   `ingest_combined` falls back to yfinance for those ranges
   but keeps massive for everything else.

   Five rules:
     1) Stagnation: ≥ 20 consecutive trading days at same price
     2) Zero close: any row where adj_close = 0
     3) Feed discrepancy: massive shows a month-over-month min/max
        move ≥ 100% AND that jump is more than 5x the same jump
        seen in yfinance for the same (ticker, month). Targets
        unadjusted splits / corporate actions in massive — real
        moves get corroborated by yfinance so they're never flagged.
        Drops the entire ticker history from massive; yfinance
        provides correctly-adjusted prices via ingest_combined.
     4) No-yfinance huge jump: ticker has any monthly jump ≥ 100%
        AND ticker has zero coverage in yfinance. Fallback for the
        discrepancy rule when yfinance can't corroborate at all —
        defaults to exclusion since there's no second source to
        validate against. Removes the ticker entirely.
        Catches: GEHI (0.375 → 9.53 in 2022-10, no yfinance data).
     5) Ticker reuse: massive's history for the ticker starts more
        than 365 days before yfinance's history. The pre-yfinance
        portion almost always belongs to a different (delisted)
        company that previously held the ticker. Drops the
        pre-yfinance-start portion of massive only.
        Catches: MRNA (Moderna IPO 2018, massive has data from 2008),
        COIN (Coinbase IPO 2021), and similar reuses.
   -------------------------------------------------------------- */

WITH prices AS (
    SELECT
        ticker,
        date,
        adj_close,
        LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM {{ ref('ingest_massive_staging') }}
),

runs AS (
    SELECT
        ticker,
        date,
        adj_close,
        CASE WHEN adj_close = prev_close THEN 0 ELSE 1 END AS new_run_flag
    FROM prices
),

run_groups AS (
    SELECT
        ticker,
        adj_close,
        date,
        SUM(new_run_flag) OVER (
            PARTITION BY ticker ORDER BY date
            ROWS UNBOUNDED PRECEDING
        ) AS run_id
    FROM runs
),

stagnation_ranges AS (
    SELECT
        ticker,
        MIN(date) AS bad_start,
        MAX(date) AS bad_end,
        'stagnation' AS reason
    FROM run_groups
    GROUP BY ticker, run_id
    HAVING COUNT(*) >= 20
),

zero_close_ranges AS (
    SELECT
        ticker,
        date AS bad_start,
        date AS bad_end,
        'zero_close' AS reason
    FROM {{ ref('ingest_massive_staging') }}
    WHERE adj_close = 0
),

massive_monthly AS (
    SELECT
        ticker,
        DATE_TRUNC('month', date)::date AS month,
        MIN(adj_close) AS min_price,
        MAX(adj_close) AS max_price
    FROM {{ ref('ingest_massive_staging') }}
    WHERE adj_close > 0
    GROUP BY ticker, DATE_TRUNC('month', date)
),

massive_jumps AS (
    SELECT
        ticker,
        month,
        GREATEST(
            ABS((LEAD(min_price) OVER (PARTITION BY ticker ORDER BY month) - min_price)
                / NULLIF(min_price, 0)),
            ABS((LEAD(max_price) OVER (PARTITION BY ticker ORDER BY month) - max_price)
                / NULLIF(max_price, 0))
        ) AS jump_pct
    FROM massive_monthly
),

yfinance_monthly AS (
    SELECT
        CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker,
        DATE_TRUNC('month', date)::date AS month,
        MIN(adj_close) AS min_price,
        MAX(adj_close) AS max_price
    FROM {{ ref('ingest_yfinance_staging') }}
    WHERE adj_close > 0
    GROUP BY CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END, DATE_TRUNC('month', date)
),

yfinance_jumps AS (
    SELECT
        ticker,
        month,
        GREATEST(
            ABS((LEAD(min_price) OVER (PARTITION BY ticker ORDER BY month) - min_price)
                / NULLIF(min_price, 0)),
            ABS((LEAD(max_price) OVER (PARTITION BY ticker ORDER BY month) - max_price)
                / NULLIF(max_price, 0))
        ) AS jump_pct
    FROM yfinance_monthly
),

discrepancy_flagged_tickers AS (
    SELECT DISTINCT m.ticker
    FROM massive_jumps m
    INNER JOIN yfinance_jumps y USING (ticker, month)
    WHERE m.jump_pct >= 1.0
      AND y.jump_pct IS NOT NULL
      AND m.jump_pct > 5.0 * y.jump_pct
),

discrepancy_ranges AS (
    SELECT
        s.ticker,
        MIN(s.date) AS bad_start,
        MAX(s.date) AS bad_end,
        'feed_discrepancy' AS reason
    FROM {{ ref('ingest_massive_staging') }} s
    JOIN discrepancy_flagged_tickers d USING (ticker)
    GROUP BY s.ticker
),

yfinance_tickers AS (
    SELECT DISTINCT CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker
    FROM {{ ref('ingest_yfinance_staging') }}
),

no_yfinance_huge_jump_flagged AS (
    -- When yfinance can't corroborate, default to exclusion at the
    -- same 100% threshold as the discrepancy rule's floor. Cheaper
    -- to lose a few real movers than to keep unadjusted-split garbage
    -- that has no backstop source for correction.
    SELECT mj.ticker
    FROM massive_jumps mj
    LEFT JOIN yfinance_tickers yt USING (ticker)
    WHERE yt.ticker IS NULL
    GROUP BY mj.ticker
    HAVING MAX(mj.jump_pct) >= 1.0
),

no_yfinance_huge_jump_ranges AS (
    SELECT
        s.ticker,
        MIN(s.date) AS bad_start,
        MAX(s.date) AS bad_end,
        'no_yfinance_huge_jump' AS reason
    FROM {{ ref('ingest_massive_staging') }} s
    JOIN no_yfinance_huge_jump_flagged n USING (ticker)
    GROUP BY s.ticker
),

massive_ticker_starts AS (
    SELECT ticker, MIN(date) AS massive_start
    FROM {{ ref('ingest_massive_staging') }}
    GROUP BY ticker
),

yfinance_ticker_starts AS (
    SELECT
        CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker,
        MIN(date) AS yfinance_start
    FROM {{ ref('ingest_yfinance_staging') }}
    GROUP BY CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END
),

ticker_reuse_ranges AS (
    SELECT
        m.ticker,
        m.massive_start AS bad_start,
        (y.yfinance_start - INTERVAL '1 day')::date AS bad_end,
        'ticker_reuse' AS reason
    FROM massive_ticker_starts m
    JOIN yfinance_ticker_starts y USING (ticker)
    WHERE m.massive_start < y.yfinance_start - INTERVAL '365 days'
)

SELECT ticker, bad_start, bad_end, reason FROM stagnation_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM zero_close_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM discrepancy_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM no_yfinance_huge_jump_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM ticker_reuse_ranges
