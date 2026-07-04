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

   Six rules:
     1) Stagnation: ≥ 20 consecutive trading days at same price
     2) Invalid price: any row where adj_close IS NULL, = 0, or
        sub-nickel (< $0.05). Cent-rounded feeds print 0.01-0.04
        artifacts whose divide-by-tiny denominators produced labels
        up to 259,200% downstream (audit L6); sub-nickel prints are
        not tradeable prices.
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
     5) Ticker reuse (feed-level): massive's history for the ticker
        starts more than 365 days before yfinance's history. The
        pre-yfinance portion almost always belongs to a different
        (delisted) company. Drops the pre-yfinance-start portion only.
        Catches: MRNA (Moderna IPO 2018), COIN (Coinbase IPO 2021).
     6) Regime shift: ticker's yearly median price jumps ≥ 50x
        year-over-year AND remains elevated the next year. Catches
        post-bankruptcy mergers / corporate identity changes where
        both feeds carry the legacy entity's prices under the new
        ticker. Drops everything before the shift year.
        Catches: CHRD (Chord Energy formed July 2022 from Whiting
        bankruptcy + Oasis merger; pre-2022 data is Whiting at $0.06).
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
        'invalid_price' AS reason
    FROM {{ ref('ingest_massive_staging') }}
    -- audit L6: sub-nickel prints (< $0.05) are degenerate denominators, not
    -- tradeable prices; was `= 0` only
    WHERE adj_close IS NULL OR adj_close < 0.05
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

-- yfinance coverage horizon per ticker (its last available bar). Used to cap the
-- feed_discrepancy exclusion so massive's post-yfinance tail is preserved.
yfinance_ticker_ends AS (
    SELECT
        CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END AS ticker,
        MAX(date) AS yfinance_end
    FROM {{ ref('ingest_yfinance_staging') }}
    GROUP BY CASE WHEN ticker = '^GSPC' THEN 'SPY' ELSE ticker END
),

discrepancy_ranges AS (
    -- Cap the exclusion at the yfinance coverage horizon instead of dropping the
    -- ENTIRE massive history. The defect for these tickers is OLD stagnation
    -- plateaus / a drifting adjustment basis, not recent data: yfinance carries
    -- the correctly-adjusted history it covers, and massive's post-yfinance tail
    -- is clean. Verified on prod across all 69 flagged tickers: the massive/yfinance
    -- splice at the yfinance horizon agrees within 5% (within 2% for the 66 ranked),
    -- every tail extends past the horizon, and no tail is a plateau or carries a
    -- >=50% single-day split move. Dropping the WHOLE history forced a yfinance
    -- fallback that is dead at 2025-07-23, freezing 66 live large-caps
    -- (META/DUK/RTX/LIN/...). Keeping the post-horizon massive tail un-freezes them
    -- with a clean splice; yfinance still fills everything it covers. If a ticker has
    -- no yfinance end (never happens here, since the flag requires yfinance), bad_end
    -- falls back to the massive max (whole-history drop, unchanged behavior).
    SELECT
        s.ticker,
        MIN(s.date) AS bad_start,
        LEAST(MAX(s.date), COALESCE(ye.yfinance_end, MAX(s.date))) AS bad_end,
        'feed_discrepancy' AS reason
    FROM {{ ref('ingest_massive_staging') }} s
    JOIN discrepancy_flagged_tickers d USING (ticker)
    LEFT JOIN yfinance_ticker_ends ye USING (ticker)
    GROUP BY s.ticker, ye.yfinance_end
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
),

yearly_median AS (
    SELECT
        ticker,
        EXTRACT(YEAR FROM date)::int AS yr,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY adj_close) AS median_price
    FROM {{ ref('ingest_massive_staging') }}
    WHERE adj_close > 0
    GROUP BY ticker, EXTRACT(YEAR FROM date)
),

yearly_with_neighbors AS (
    SELECT
        ticker,
        yr,
        median_price,
        LAG(median_price)  OVER (PARTITION BY ticker ORDER BY yr) AS prev_median,
        LEAD(median_price) OVER (PARTITION BY ticker ORDER BY yr) AS next_median
    FROM yearly_median
),

regime_shift_years AS (
    SELECT ticker, MAX(yr) AS shift_year
    FROM yearly_with_neighbors
    WHERE prev_median IS NOT NULL
      AND median_price / NULLIF(prev_median, 0) >= 50.0
      AND (next_median IS NULL OR next_median >= 0.5 * median_price)
    GROUP BY ticker
),

regime_shift_ranges AS (
    SELECT
        s.ticker,
        MIN(s.date) AS bad_start,
        (MAKE_DATE(rs.shift_year, 1, 1) - INTERVAL '1 day')::date AS bad_end,
        'regime_shift' AS reason
    FROM {{ ref('ingest_massive_staging') }} s
    JOIN regime_shift_years rs USING (ticker)
    GROUP BY s.ticker, rs.shift_year
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
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM regime_shift_ranges
