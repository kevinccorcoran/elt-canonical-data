-- Personal portfolio persistence for the AlphaStream Lifecycle dashboard.
--
-- Small, single-user, single-writer app state. It lives in its OWN schema
-- alongside the model tables so the transition history can join to
-- serving/validation/monitoring/qual. The dashboard reads+writes ONLY this
-- schema; every other schema stays read-only to it.
--
-- Run this on each database the dashboard should track a portfolio in. Prod is
-- the shared copy: both the local and the server dashboard connect to prod by
-- default, so one prod schema gives them a single authoritative portfolio.
--
-- Claude is policy-blocked from writing to prod, so YOU run this (psql or your
-- SQL GUI). It is idempotent (IF NOT EXISTS), so re-running is safe.

CREATE SCHEMA IF NOT EXISTS portfolio;

-- One row per buy plan. Column set mirrors the app's position record exactly.
CREATE TABLE IF NOT EXISTS portfolio.positions (
    id            text PRIMARY KEY,
    ticker        text        NOT NULL,
    amount_usd    numeric     NOT NULL CHECK (amount_usd > 0),
    cadence       text        NOT NULL DEFAULT 'monthly',   -- once | monthly | semimonthly
    day1          integer,
    day2          integer,
    start_date    date,
    end_date      date,
    sold_date     date,
    sold_fraction numeric,
    mode          text        NOT NULL DEFAULT 'manual',    -- manual | model
    adopted_at    timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS positions_ticker_idx ON portfolio.positions (ticker);

-- Tickers the user explicitly removed, so the qualstream auto-seed won't
-- resurrect them on the next Generate.
CREATE TABLE IF NOT EXISTS portfolio.dismissed (
    ticker       text        PRIMARY KEY,
    dismissed_at timestamptz NOT NULL DEFAULT now()
);

-- Transition log: one snapshot per ticker per Generate, so a position's
-- Buy -> Hold -> Sell -> Closed path is reviewable over time. Upsert on
-- (ticker, as_of, hz): re-Generating the same day refreshes that row; new days
-- accumulate. state is one of buy | hold | sell | closed.
CREATE TABLE IF NOT EXISTS portfolio.state_history (
    ticker      text        NOT NULL,
    as_of       date        NOT NULL,       -- the Generate date
    hz          integer     NOT NULL,       -- hold horizon (months) at capture
    state       text        NOT NULL,
    why         text,
    grade       numeric,                    -- qualstream grade at capture, if any
    captured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, as_of, hz)
);
CREATE INDEX IF NOT EXISTS state_history_ticker_idx ON portfolio.state_history (ticker, as_of);

-- ---------------------------------------------------------------------------
-- OPTIONAL hardening (recommended). Instead of the dashboard writing as the DB
-- admin, create a scoped role that can touch ONLY the portfolio schema, then
-- point the dashboard's prod connection at it. A dashboard bug then cannot
-- affect model data even in principle.
--
-- CREATE ROLE portfolio_app LOGIN PASSWORD '<choose-a-strong-one>';
-- GRANT USAGE ON SCHEMA portfolio TO portfolio_app;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA portfolio TO portfolio_app;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA portfolio
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO portfolio_app;
-- -- read-only on the schemas the dashboard queries for model data:
-- GRANT USAGE ON SCHEMA serving, validation, monitoring, qual, cdm, raw TO portfolio_app;
-- GRANT SELECT ON ALL TABLES IN SCHEMA serving, validation, monitoring, qual, cdm, raw TO portfolio_app;
