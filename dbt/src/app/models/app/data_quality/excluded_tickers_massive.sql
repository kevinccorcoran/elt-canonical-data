{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    schema = 'data_quality',
    post_hook = [
      """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """
    ]
  )
}}

/* --------------------------------------------------------------
   PRICE STAGNATION + FIRST ADJ_CLOSE = 0 DETECTION
   Detects:
     1) Tickers whose price stays flat ≥ 20 consecutive days
     2) Tickers whose first adj_close value = 0 (bad data)
   Output: DISTINCT tickers flagged by either test.
   -------------------------------------------------------------- */

-- 1. Detect flat price runs (stagnation)

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

run_lengths AS (
    SELECT
        ticker,
        run_id,
        COUNT(*) AS run_length
    FROM run_groups
    GROUP BY ticker, run_id
),

stagnation_flag AS (
    SELECT DISTINCT ticker
    FROM run_lengths
    WHERE run_length >= 20   -- stagnation threshold
),

-- 2. Detect tickers whose first adj_close = 0

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

-- 3. Final output: union of both tests

SELECT DISTINCT ticker
FROM stagnation_flag

UNION

SELECT DISTINCT ticker
FROM first_adj_close_0