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

   Two rules:
     1) Invalid price: adj_close IS NULL or = 0 → drop that day
     2) Regime shift: ticker's yearly median price jumps ≥ 50x
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
SELECT ticker, bad_start, bad_end, reason FROM regime_shift_ranges
