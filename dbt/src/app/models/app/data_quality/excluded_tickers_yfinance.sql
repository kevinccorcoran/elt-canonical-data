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
   BAD-DATA DATE RANGES IN YFINANCE FEED

   Emits one row per (ticker, bad_start, bad_end) range that should
   be dropped from the yfinance source. Downstream `ingest_combined`
   skips those rows.

   Three rules:
     1) Invalid price: adj_close IS NULL or = 0 → drop that day
     2) Stagnation: ≥ 20 consecutive trading days at same price.
        Mirrors the massive feed's stagnation rule. Catches
        forward-filled / unadjusted-split artifacts in yfinance
        (e.g. HUBB pre-1995 stuck at $0.69 then jumped to $10+).
        Excluding the bad range lets the rest of the ticker's
        history flow through cleanly.
     3) Regime shift: ticker's yearly median price jumps ≥ 50x
        year-over-year AND remains elevated the next year. Catches
        post-bankruptcy mergers / corporate identity changes where
        the legacy entity's pre-shift prices distort downstream
        excess-return calculations.
        Catches: CHRD (Whiting bankruptcy at $0.06 → Chord at $130+).
   -------------------------------------------------------------- */

WITH invalid_price_ranges AS (
    SELECT
        ticker,
        date AS bad_start,
        date AS bad_end,
        'invalid_price' AS reason
    FROM {{ ref('ingest_yfinance_staging') }}
    WHERE adj_close IS NULL OR adj_close = 0
),

stagnation_prices AS (
    SELECT
        ticker,
        date,
        adj_close,
        LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM {{ ref('ingest_yfinance_staging') }}
    WHERE adj_close > 0
),

stagnation_runs AS (
    SELECT
        ticker,
        date,
        adj_close,
        SUM(CASE WHEN adj_close = prev_close THEN 0 ELSE 1 END) OVER (
            PARTITION BY ticker ORDER BY date ROWS UNBOUNDED PRECEDING
        ) AS run_id
    FROM stagnation_prices
),

stagnation_ranges AS (
    SELECT
        ticker,
        MIN(date) AS bad_start,
        MAX(date) AS bad_end,
        'stagnation' AS reason
    FROM stagnation_runs
    GROUP BY ticker, run_id
    HAVING COUNT(*) >= 20
),

yearly_median AS (
    SELECT
        ticker,
        EXTRACT(YEAR FROM date)::int AS yr,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY adj_close) AS median_price
    FROM {{ ref('ingest_yfinance_staging') }}
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
    FROM {{ ref('ingest_yfinance_staging') }} s
    JOIN regime_shift_years rs USING (ticker)
    GROUP BY s.ticker, rs.shift_year
)

SELECT ticker, bad_start, bad_end, reason FROM invalid_price_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM stagnation_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM regime_shift_ranges
