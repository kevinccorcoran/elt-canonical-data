{% do log("Current DB_DATABASE: " ~ env_var('DB_DATABASE'), info = true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    schema = 'metrics',
    post_hook = [
        """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_obs_date
        ON {{ this }} (ticker, observation_date)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_fibonacci_lag
        ON {{ this }} (fibonacci_lag DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_fib_lag
        ON {{ this }} (ticker, fibonacci_lag)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_processed_at
        ON {{ this }} (processed_at)
        """
    ]
  )
}}

-- consistent runtime for the full model
WITH run_time AS (
    SELECT now() AS ts
),

fib_seq AS (
    SELECT *
    FROM {{ source('raw', 'fib_seq') }}
),

/* -------------------------------------------------------
   cdm_api_data_ingestion (replaced lateral with window functions)
   EXACT match to original logic using forward-fill FIRST_VALUE
-------------------------------------------------------- */

cdm_api_data_ingestion AS (
    SELECT
        date,
        ticker,
        ticker_date_id,

        /* forward-fill date for correct alignment */
        COALESCE(
            date,
            FIRST_VALUE(date) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ) AS date_filled,

        /* forward-fill adj_close */
        COALESCE(
            adj_close,
            FIRST_VALUE(adj_close) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ) AS adj_close,

        /* forward-fill processed_at */
        COALESCE(
            processed_at,
            FIRST_VALUE(processed_at) OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ) AS processed_at

    FROM {{ ref('api_data_ingestion_y_p') }}
),

/* -------------------------------------------------------
   latest + earliest observation dates
-------------------------------------------------------- */

latest_observation AS (
    SELECT
        ticker,
        date AS latest_observation,
        adj_close AS latest_adj_close
    FROM (
        SELECT
            ticker,
            date,
            adj_close,
            ROW_NUMBER() OVER (
                PARTITION BY ticker ORDER BY date DESC
            ) AS rn
        FROM {{ ref('api_data_ingestion_y_p') }}          -- IMPORTANT
    ) x
    WHERE rn = 1
),

min_observation_date AS (
    SELECT
        ticker,
        date AS min_observation_date,
        adj_close AS min_date_adj_close,
        ticker || '_' || date AS ticker_date_id
    FROM (
        SELECT
            ticker,
            date,
            adj_close,
            ROW_NUMBER() OVER (
                PARTITION BY ticker ORDER BY date ASC
            ) AS rn
        FROM cdm_api_data_ingestion
    ) x
    WHERE rn = 1
),

/* -------------------------------------------------------
   ticker baseline
-------------------------------------------------------- */

ticker_baseline AS (
    SELECT
        latest.ticker,
        latest.latest_observation,
        latest.latest_adj_close,
        min_obs.min_observation_date,
        min_obs.min_date_adj_close
    FROM latest_observation latest
    JOIN min_observation_date min_obs USING (ticker)
),

/* -------------------------------------------------------
   main join: Fibonacci offset match
-------------------------------------------------------- */

joined_base AS (
    SELECT *
    FROM (
        SELECT
            tb.ticker,
            tb.latest_observation,

            /* observation date: Fibonacci offset or min date fallback */
            COALESCE(cdi.date, tb.min_observation_date) AS observation_date,

            tb.latest_adj_close,
            tb.min_date_adj_close,
            fib.fibonacci_lag_value AS fibonacci_lag,

            tb.ticker || '_' || COALESCE(cdi.date, tb.min_observation_date)
                AS ticker_date_id,

            ROW_NUMBER() OVER (
                PARTITION BY
                    tb.ticker,
                    COALESCE(cdi.date, tb.min_observation_date),
                    fib.fibonacci_lag_value
                ORDER BY
                    tb.ticker,
                    COALESCE(cdi.date, tb.min_observation_date),
                    fib.fibonacci_lag_value
            ) AS rn

        FROM ticker_baseline tb
        CROSS JOIN fib_seq fib

        LEFT JOIN cdm_api_data_ingestion cdi
            ON cdi.ticker = tb.ticker
           AND cdi.date = (
                tb.latest_observation
                - (fib.fibonacci_lag_value || ' month')::interval
            )::date

    ) AS sub
    WHERE rn = 1
)

SELECT
    jb.*,
    rt.ts AS processed_at
FROM joined_base jb
CROSS JOIN run_time rt