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

   Two rules:
     1) Stagnation: ≥ 20 consecutive trading days at same price
     2) Zero close: any row where adj_close = 0
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
)

SELECT ticker, bad_start, bad_end, reason FROM stagnation_ranges
UNION ALL
SELECT ticker, bad_start, bad_end, reason FROM zero_close_ranges
