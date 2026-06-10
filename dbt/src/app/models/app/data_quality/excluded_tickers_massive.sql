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

   Three rules:
     1) Stagnation: ≥ 20 consecutive trading days at same price
     2) Zero close: any row where adj_close = 0
     3) Huge jump: ticker has any month-over-month min/max move
        ≥ 100% (2x). Conservative threshold — catches unadjusted
        splits, corporate actions, and high-volatility tickers
        (penny stocks, post-IPO pops, meme moves). Drops the entire
        ticker history from massive; yfinance provides
        correctly-adjusted prices for the dropped tickers.
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

monthly AS (
    SELECT
        ticker,
        DATE_TRUNC('month', date)::date AS month,
        MIN(adj_close) AS min_price,
        MAX(adj_close) AS max_price
    FROM {{ ref('ingest_massive_staging') }}
    WHERE adj_close > 0
    GROUP BY ticker, DATE_TRUNC('month', date)
),

monthly_jumps AS (
    SELECT
        ticker,
        GREATEST(
            ABS((LEAD(min_price) OVER (PARTITION BY ticker ORDER BY month) - min_price)
                / NULLIF(min_price, 0)),
            ABS((LEAD(max_price) OVER (PARTITION BY ticker ORDER BY month) - max_price)
                / NULLIF(max_price, 0))
        ) AS jump_pct
    FROM monthly
),

flagged_jump_tickers AS (
    SELECT ticker
    FROM monthly_jumps
    GROUP BY ticker
    HAVING MAX(jump_pct) >= 1.0
),

jump_ranges AS (
    SELECT
        s.ticker,
        MIN(s.date) AS bad_start,
        MAX(s.date) AS bad_end,
        'huge_jump' AS reason
    FROM {{ ref('ingest_massive_staging') }} s
    JOIN flagged_jump_tickers f USING (ticker)
    GROUP BY s.ticker
)

SELECT ticker, bad_start, bad_end, reason FROM stagnation_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM zero_close_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM jump_ranges
