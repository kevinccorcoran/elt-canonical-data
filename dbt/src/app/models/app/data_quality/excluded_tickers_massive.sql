{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    post_hook = [
      """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """
    ]
  )
}}

/* --------------------------------------------------------------
   PRICE STAGNATION + FIRST adj_close = 0 DETECTION

   Flags tickers that exhibit suspicious price behavior:
     1) Price does not change for ≥ 20 consecutive trading days
     2) First observed adj_close value equals 0 (invalid starting price)

   Output: DISTINCT list of affected tickers.
   -------------------------------------------------------------- */

-- 1) Prepare price history with previous-day comparison
WITH prices AS (
    SELECT
        ticker,
        date,
        adj_close,
        LAG(adj_close) OVER (
            PARTITION BY ticker ORDER BY date
        ) AS prev_close
    FROM {{ ref('api_data_ingestion_massive_staging') }}
),

-- 2) Mark boundaries where a new price run begins
runs AS (
    SELECT
        ticker,
        date,
        adj_close,
        CASE
            WHEN adj_close = prev_close THEN 0
            ELSE 1
        END AS new_run_flag
    FROM prices
),

-- 3) Assign a unique run ID per uninterrupted price sequence
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

-- 4) Measure how long each price run lasts
run_lengths AS (
    SELECT
        ticker,
        run_id,
        COUNT(*) AS run_length
    FROM run_groups
    GROUP BY ticker, run_id
),

-- 5) Flag tickers with long flat-price runs
stagnation_flag AS (
    SELECT DISTINCT ticker
    FROM run_lengths
    WHERE run_length >= 20   -- stagnation threshold
),

-- 6) Detect tickers whose first recorded adj_close is zero
first_adj_close_0 AS (
    SELECT DISTINCT ticker
    FROM (
        SELECT
            a.ticker,
            FIRST_VALUE(a.adj_close) OVER (
                PARTITION BY a.ticker ORDER BY a.date
            ) AS first_adj_close
        FROM {{ ref('api_data_ingestion_massive_staging') }} a
    ) t
    WHERE first_adj_close = 0
)

-- 7) Final result: union of all detected issues
SELECT DISTINCT ticker
FROM stagnation_flag

UNION

SELECT DISTINCT ticker
FROM first_adj_close_0
