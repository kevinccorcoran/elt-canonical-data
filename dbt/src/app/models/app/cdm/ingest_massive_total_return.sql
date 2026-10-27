{% do log("Current ENV / DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',

    database = env_var('DB_DATABASE'),

    on_schema_change = 'sync_all_columns',

    post_hook = [
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker_date
        ON {{ this }} (ticker, date);
      """
    ]
  )
}}

-- --------------------------------------------------------------
-- Massive bars on a total-return basis (dividends reinvested).
--
-- The massive aggregates are adjusted for splits only, so
-- ingest_massive_staging leaves dividends out, while the yfinance history
-- ingest_combined joins them to folds dividends in: from 2003 to 2021 most
-- names carried dividends and SPY did not (elt-inference-models
-- docs/lookahead_audit_2026-10-20.md, "Stock clock"). This model multiplies
-- every bar by the dividends paid after it,
--     factor(d) = product over later ex dates of (1 - dividend / prior close),
-- so the latest bar keeps its price and every return across an ex date
-- includes the dividend. ingest_massive_staging keeps the split-only closes
-- for the data_quality rules; ingest_combined reads this model.
--
-- Dividends come from raw.massive_dividends (massive_corporate_actions.py).
-- The vendor's amounts are as declared, so each is scaled by every later
-- split (raw.massive_splits) to match the split-adjusted closes. Kept: cash,
-- USD, ex date inside the bar history (an announced future dividend is
-- skipped), and a yield above 0 and below 50% of the prior close (larger is a
-- data error).
--
-- Synthetic (weekend / holiday) bars hold the NEXT session's close, so each
-- dividend is dated the day after the last natural session before its ex
-- date: the synthetic bars in between already trade without it.
-- --------------------------------------------------------------

WITH splits AS (
    SELECT ticker, execution_date, split_from / split_to AS f
    FROM {{ source('raw', 'massive_splits') }}
    WHERE split_from > 0 AND split_to > 0
),

dividends AS (
    -- one amount per (ticker, ex date): same-day regular and special dividends add up.
    -- A plain OR, not COALESCE(currency, 'USD'): the planner has no statistics
    -- for the expression, guessed ~600 of 126k rows and joined every dividend
    -- to every bar of its ticker (20 minutes in staging)
    SELECT ticker, ex_dividend_date AS ex_date, SUM(cash_amount) AS cash
    FROM {{ source('raw', 'massive_dividends') }}
    WHERE cash_amount > 0
      AND (currency = 'USD' OR currency IS NULL)
    GROUP BY ticker, ex_dividend_date
),

dividends_split_adj AS (
    SELECT
        d.ticker,
        d.ex_date,
        d.cash * COALESCE(EXP(SUM(LN(s.f))), 1.0) AS cash
    FROM dividends d
    LEFT JOIN splits s
      ON s.ticker = d.ticker
     AND s.execution_date > d.ex_date
    GROUP BY d.ticker, d.ex_date, d.cash
),

span AS (
    SELECT ticker, MAX("date") AS last_date
    FROM {{ ref('ingest_massive_staging') }}
    GROUP BY ticker
),

candidates AS MATERIALIZED (
    -- the 10 days before each ex date inside the bar history, built first so
    -- the bar lookup is one equality join on (ticker, date)
    SELECT d.ticker, d.ex_date, d.cash, d.ex_date - g.k AS day
    FROM dividends_split_adj d
    JOIN span sp
      ON sp.ticker = d.ticker
     AND d.ex_date <= sp.last_date
    CROSS JOIN generate_series(1, 10) AS g(k)
),

prior_close AS (
    -- the last natural session before the ex date, within 10 days
    SELECT
        c.ticker,
        c.ex_date,
        c.cash,
        b."date" AS prior_date,
        b.adj_close AS prior_close,
        ROW_NUMBER() OVER (PARTITION BY c.ticker, c.ex_date ORDER BY b."date" DESC) AS rn
    FROM candidates c
    JOIN {{ ref('ingest_massive_staging') }} b
      ON b.ticker = c.ticker
     AND b."date" = c.day
    WHERE (b.date_type = 'natural' OR b.date_type IS NULL)
      AND b.adj_close > 0
),

events AS (
    -- one step per (ticker, date); two ex dates after the same session add up
    SELECT
        ticker,
        prior_date + 1 AS event_date,
        SUM(LN(1.0 - cash / prior_close)) AS lf
    FROM prior_close
    WHERE rn = 1
      AND cash / prior_close > 0
      AND cash / prior_close < 0.5
    GROUP BY ticker, prior_date + 1
),

steps AS (
    -- lf_after = the log factor of this and every later dividend: a bar dated
    -- in [prev_date, event_date) carries exp(lf_after), a bar on or after the
    -- last event date carries none
    SELECT
        ticker,
        event_date,
        LAG(event_date) OVER (PARTITION BY ticker ORDER BY event_date) AS prev_date,
        SUM(lf) OVER (PARTITION BY ticker ORDER BY event_date DESC ROWS UNBOUNDED PRECEDING) AS lf_after
    FROM events
)

SELECT
    b."date",
    b.ticker,
    b.adj_close * EXP(COALESCE(s.lf_after, 0.0)) AS adj_close,
    b.adj_close AS adj_close_split_only,
    EXP(COALESCE(s.lf_after, 0.0)) AS dividend_factor,
    b.processed_at,
    b.ticker_date_id,
    b.source,
    b.date_type,
    b.query_run_time
FROM {{ ref('ingest_massive_staging') }} b
LEFT JOIN steps s
  ON s.ticker = b.ticker
 AND b."date" < s.event_date
 AND (s.prev_date IS NULL OR b."date" >= s.prev_date)
