{{ 
  config(
    materialized = 'table',
    database     = env_var('DB_DATABASE'),
    schema       = 'metrics',
    post_hook = [
      "CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker ON {{ this }} (ticker)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_rank ON {{ this }} (rank)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at ON {{ this }} (processed_at)"
    ]
  ) 
}}

{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

-- consistent runtime for the entire model
WITH run_time AS (
    SELECT now() AS ts
),

clean_prices AS (
    -- Remove any duplicate (ticker, date) if they exist
    SELECT DISTINCT
        ticker,
        date,
        adj_close
    FROM {{ ref('api_data_ingestion_y_p') }}
),

first_last AS (
    -- Use window functions so each ticker produces exactly ONE row
    SELECT
        ticker,
        MIN(date) OVER (PARTITION BY ticker) AS min_date,
        MAX(date) OVER (PARTITION BY ticker) AS max_date,
        FIRST_VALUE(adj_close) OVER (PARTITION BY ticker ORDER BY date ASC)  AS first_adj_close,
        FIRST_VALUE(adj_close) OVER (PARTITION BY ticker ORDER BY date DESC) AS last_adj_close
    FROM clean_prices
),

dedup_first_last AS (
    -- Defensive dedup
    SELECT DISTINCT
        ticker,
        min_date,
        max_date,
        first_adj_close,
        last_adj_close
    FROM first_last
    WHERE
        first_adj_close > 0
        AND last_adj_close  > 0
        AND max_date > min_date
),

growth AS (
    -- Compute month count and monthly CAGR
    SELECT
        ticker,
        min_date,
        max_date,
        first_adj_close,
        last_adj_close,

        (
            EXTRACT(YEAR FROM AGE(max_date, min_date)) * 12 +
            EXTRACT(MONTH FROM AGE(max_date, min_date))
        )::int AS months_count,

        (
            POWER(
                last_adj_close / first_adj_close,
                1.0 / NULLIF(
                    (
                        EXTRACT(YEAR FROM AGE(max_date, min_date)) * 12 +
                        EXTRACT(MONTH FROM AGE(max_date, min_date))
                    ),
                    0
                )
            ) - 1
        ) * 100 AS growth_pct_per_month
    FROM dedup_first_last
),

weighted AS (
    SELECT
        *,
        ROUND((growth_pct_per_month * SQRT(months_count))::numeric, 2) AS weighted_growth
    FROM growth
),

stats AS (
    SELECT
        AVG(weighted_growth)      AS mean_wg,
        STDDEV_POP(weighted_growth) AS std_wg
    FROM weighted
),

labeled AS (
    SELECT
        w.*,
        ROUND(
            (w.weighted_growth - s.mean_wg) / NULLIF(s.std_wg, 0),
            2
        ) AS num_stddevs_away
    FROM weighted w
    CROSS JOIN stats s
),

bucketed AS (
    SELECT
        l.*,
        CASE
        WHEN num_stddevs_away < -2.0 THEN '< -2.0 SD'
        WHEN num_stddevs_away < -1.0 THEN '-2.0 to -1.0 SD'
        WHEN num_stddevs_away < -0.5 THEN '-1.0 to -0.5 SD'
        WHEN num_stddevs_away <  0.5 THEN '-0.5 to 0.5 SD'
        WHEN num_stddevs_away <  1.0 THEN '0.5 to 1.0 SD'
        WHEN num_stddevs_away <  2.0 THEN '1.0 to 2.0 SD'
        ELSE '> 2.0 SD'
        END AS monthly_growth_vol_z_bucket
        ,
        CASE
        WHEN num_stddevs_away < -2.0 THEN 7
        WHEN num_stddevs_away < -1.0 THEN 6
        WHEN num_stddevs_away < -0.5 THEN 5
        WHEN num_stddevs_away <  0.5 THEN 4
        WHEN num_stddevs_away <  1.0 THEN 3
        WHEN num_stddevs_away <  2.0 THEN 2
        ELSE 1
    END AS monthly_growth_vol_z_bucket_num
    FROM labeled l
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY weighted_growth DESC) AS rank
    FROM bucketed
),

final AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY monthly_growth_vol_z_bucket_num
            ORDER BY weighted_growth DESC
        ) AS rank_within_bucket,
        rt.ts AS processed_at
    FROM ranked r
    CROSS JOIN run_time rt
)

SELECT *
FROM final
ORDER BY monthly_growth_vol_z_bucket_num, rank_within_bucket