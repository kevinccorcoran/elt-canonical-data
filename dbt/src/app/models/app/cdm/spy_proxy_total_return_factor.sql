{% do log("Current ENV / DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',
    database = env_var('DB_DATABASE'),
    on_schema_change = 'sync_all_columns'
  )
}}

-- --------------------------------------------------------------
-- SPY before the massive feed (2003-09-10) is the S&P 500 price index
-- (^GSPC / 10 in ingest_combined), which leaves dividends out. This puts it
-- on the same total-return basis as the massive SPY bars
-- (ingest_massive_total_return), with SPY's own dividends from 1993: seed
-- spy_dividends_pre_massive, from Yahoo, pulled once on 2026-10-27 (the
-- vendor's dividend history starts 2003-03). Before SPY's first dividend
-- (1993-03-19) the index stays price-only.
--
-- One row per dividend: a ^GSPC bar dated in [valid_from, valid_to) is
-- multiplied by factor (valid_from NULL = open start; a bar on or after the
-- last valid_to keeps factor 1). Dated the day after the last natural
-- session before the ex date, as in ingest_massive_total_return.
-- --------------------------------------------------------------

WITH gspc AS (
    SELECT "date", adj_close / 10.0 AS close, date_type
    FROM {{ ref('ingest_yfinance_staging') }}
    WHERE ticker = '^GSPC'
      AND adj_close > 0
),

dividends AS (
    SELECT ex_date, cash_amount
    FROM {{ ref('spy_dividends_pre_massive') }}
    WHERE cash_amount > 0
),

prior_close AS (
    SELECT
        d.ex_date,
        d.cash_amount,
        g."date" AS prior_date,
        g.close,
        ROW_NUMBER() OVER (PARTITION BY d.ex_date ORDER BY g."date" DESC) AS rn
    FROM dividends d
    CROSS JOIN generate_series(1, 10) AS s(k)
    JOIN gspc g
      ON g."date" = d.ex_date - s.k
     AND (g.date_type = 'natural' OR g.date_type IS NULL)
),

events AS (
    SELECT
        prior_date + 1 AS event_date,
        LN(1.0 - cash_amount / close) AS lf
    FROM prior_close
    WHERE rn = 1
      AND cash_amount / close > 0
      AND cash_amount / close < 0.5
)

SELECT
    LAG(event_date) OVER (ORDER BY event_date) AS valid_from,
    event_date AS valid_to,
    EXP(SUM(lf) OVER (ORDER BY event_date DESC ROWS UNBOUNDED PRECEDING)) AS factor
FROM events
