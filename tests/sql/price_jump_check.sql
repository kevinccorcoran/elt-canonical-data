/* --------------------------------------------------------------
   LARGEST MONTHLY % PRICE CHANGE PER TICKER
   + GLOBAL Z-SCORE + **GRANULAR 0.5-SD BUCKETS** (18 total)
   + bucket_number in CTE → safe WHERE / GROUP BY
   -------------------------------------------------------------- */
WITH monthly AS (
    SELECT
        ticker,
        DATE_TRUNC('month', "date")::DATE AS month,
        MIN(adj_close) AS min_price,
        MAX(adj_close) AS max_price
    FROM cdm.api_data_ingestion_yfinance_staging
    WHERE date_type = 'natural'
    --and ticker = 'PLTR'
    GROUP BY ticker, DATE_TRUNC('month', "date")
),

with_next AS (
    SELECT
        ticker,
        month,
        min_price,
        max_price,
        LEAD(min_price) OVER (PARTITION BY ticker ORDER BY month) AS next_min,
        LEAD(max_price) OVER (PARTITION BY ticker ORDER BY month) AS next_max
    FROM monthly
),

changes AS (
    SELECT
        ticker,
        month,
        ABS((next_min - min_price) / NULLIF(min_price, 0)) AS min_change_pct,
        ABS((next_max - max_price) / NULLIF(max_price, 0)) AS max_change_pct
    FROM with_next
    WHERE next_min IS NOT NULL
),

largest_jump AS (
    SELECT
        ticker,
        month,
        GREATEST(min_change_pct, max_change_pct) AS largest_jump_pct
    FROM changes
),

top_jump AS (
    SELECT
        ticker,
        month AS jump_month,
        largest_jump_pct,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY largest_jump_pct DESC) AS rn
    FROM largest_jump
),

top_one AS (
    SELECT ticker, jump_month, largest_jump_pct
    FROM top_jump
    WHERE rn = 1
),

stats AS (
    SELECT
        AVG(largest_jump_pct) AS mean_jump,
        STDDEV_POP(largest_jump_pct) AS std_jump
    FROM top_one
),

with_zscore AS (
    SELECT
        t.ticker,
        t.jump_month,
        t.largest_jump_pct,
        s.mean_jump,
        s.std_jump,
        ROUND(
            ((t.largest_jump_pct - s.mean_jump) / NULLIF(s.std_jump, 0))::numeric,
            2
        ) AS num_stddevs_away
    FROM top_one t
    CROSS JOIN stats s
),

/* =============== GRANULAR BUCKETS IN CTE =============== */
bucketed AS (
    SELECT
        ticker,
        jump_month,
        ROUND((largest_jump_pct * 100)::numeric, 2) AS largest_jump_pct,
        num_stddevs_away,

        /* TEXT BUCKET: 0.5-SD steps */
        CASE
            WHEN num_stddevs_away < -4.0 THEN '< -4.0 SD'
            WHEN num_stddevs_away < -3.5 THEN '-4.0 to -3.5 SD'
            WHEN num_stddevs_away < -3.0 THEN '-3.5 to -3.0 SD'
            WHEN num_stddevs_away < -2.5 THEN '-3.0 to -2.5 SD'
            WHEN num_stddevs_away < -2.0 THEN '-2.5 to -2.0 SD'
            WHEN num_stddevs_away < -1.5 THEN '-2.0 to -1.5 SD'
            WHEN num_stddevs_away < -1.0 THEN '-1.5 to -1.0 SD'
            WHEN num_stddevs_away < -0.5 THEN '-1.0 to -0.5 SD'
            WHEN num_stddevs_away <  0.0 THEN '-0.5 to  0.0 SD'
            WHEN num_stddevs_away <  0.5 THEN ' 0.0 to  0.5 SD'
            WHEN num_stddevs_away <  1.0 THEN ' 0.5 to  1.0 SD'
            WHEN num_stddevs_away <  1.5 THEN ' 1.0 to  1.5 SD'
            WHEN num_stddevs_away <  2.0 THEN ' 1.5 to  2.0 SD'
            WHEN num_stddevs_away <  2.5 THEN ' 2.0 to  2.5 SD'
            WHEN num_stddevs_away <  3.0 THEN ' 2.5 to  3.0 SD'
            WHEN num_stddevs_away <  3.5 THEN ' 3.0 to  3.5 SD'
            WHEN num_stddevs_away <  4.0 THEN ' 3.5 to  4.0 SD'
            ELSE                              '> 4.0 SD'
        END AS stddev_bucket,

        /* NUMERIC BUCKET: 1 = most extreme positive */
        CASE
            WHEN num_stddevs_away >= 4.0 THEN 1
            WHEN num_stddevs_away >= 3.5 THEN 2
            WHEN num_stddevs_away >= 3.0 THEN 3
            WHEN num_stddevs_away >= 2.5 THEN 4
            WHEN num_stddevs_away >= 2.0 THEN 5
            WHEN num_stddevs_away >= 1.5 THEN 6
            WHEN num_stddevs_away >= 1.0 THEN 7
            WHEN num_stddevs_away >= 0.5 THEN 8
            WHEN num_stddevs_away >= 0.0 THEN 9
            WHEN num_stddevs_away >= -0.5 THEN 10
            WHEN num_stddevs_away >= -1.0 THEN 11
            WHEN num_stddevs_away >= -1.5 THEN 12
            WHEN num_stddevs_away >= -2.0 THEN 13
            WHEN num_stddevs_away >= -2.5 THEN 14
            WHEN num_stddevs_away >= -3.0 THEN 15
            WHEN num_stddevs_away >= -3.5 THEN 16
            WHEN num_stddevs_away >= -4.0 THEN 17
            ELSE 0  -- < -4.0 SD
        END AS bucket_number

    FROM with_zscore
)

/* ====================== FINAL OUTPUT ====================== */
SELECT
    ticker,
    jump_month,
    largest_jump_pct,
    num_stddevs_away,
    stddev_bucket,
    bucket_number
FROM bucketed
-- Example filter: only top 3 extreme buckets (≥ 3.0 SD)
--WHERE bucket_number <= 3
ORDER BY largest_jump_pct DESC, ticker