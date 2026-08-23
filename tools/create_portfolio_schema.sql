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

-- Realized trades archive: a row moves here (and out of positions) when the
-- user clicks "Archive sold" after actually selling. Read-only in the app -
-- the "my info" record. outcome splits profit/loss; all money figures are the
-- simulated-fill numbers frozen at archive time. The app also auto-creates
-- these two tables (CREATE IF NOT EXISTS) on connect, so running this file is
-- only needed for a fresh database.
CREATE TABLE IF NOT EXISTS portfolio.closed_positions (
    id           text        NOT NULL,
    ticker       text        NOT NULL,
    cluster_id   integer,
    cadence      text,
    amount_usd   numeric,
    invested_usd numeric,
    value_usd    numeric,
    pnl_usd      numeric,
    ret_pct      numeric,
    spy_ret_pct  numeric,
    vs_spy_pp    numeric,
    vs_lump_pp   numeric,
    entry_date   date,
    sold_date    date        NOT NULL,
    outcome      text        NOT NULL CHECK (outcome IN ('profit','loss')),
    archived_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, sold_date)
);
CREATE INDEX IF NOT EXISTS closed_positions_ticker_idx
    ON portfolio.closed_positions (ticker, sold_date);

-- Tiny app settings store (auto_adopt_amount: the $ the portfolio_sync DAG
-- adopts each new BUY at; recoup_threshold_pct: the gain at which the one-time
-- stake-back ping fires). UNSET auto_adopt_amount = the DAG adopts nothing and
-- pings a warning; it never falls back to a default amount.
CREATE TABLE IF NOT EXISTS portfolio.app_settings (
    key        text        PRIMARY KEY,
    value      text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- portfolio_sync's once-per-position recoup stamp: set the day the high-water
-- "sell ~N% to take your stake back" ping fires, so it can never nag twice.
ALTER TABLE portfolio.positions ADD COLUMN IF NOT EXISTS recoup_pinged_at date;

-- portfolio_sync run log + durable ping outbox. One row per day: qs_buys is
-- the day's capped adopt queue (the R/Python parity record), msgs the Telegram
-- texts committed WITH the day's writes, sent flips true only after delivery -
-- a Telegram outage delays pings instead of losing them. The deadman cron
-- (tools/portfolio_deadman.sh) alerts when today's row is missing.
CREATE TABLE IF NOT EXISTS portfolio.sync_runs (
    as_of      date        PRIMARY KEY,
    qs_buys    text[],
    n_adopt    integer,
    n_snap     integer,
    n_arch     integer,
    n_recoup   integer,
    msgs       jsonb,
    sent       boolean     NOT NULL DEFAULT false,
    sent_n     integer     NOT NULL DEFAULT 0,
    attempts   integer     NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- per-message delivery cursor + give-up counter (2026-08-23): sent_n counts
-- the delivered prefix of msgs so a mid-batch Telegram failure never
-- re-delivers what already went out; attempts counts failed delivery runs -
-- after 10 the day is abandoned (sent=true) with a warning ping so one
-- poison message can't dam the queue forever.
ALTER TABLE portfolio.sync_runs ADD COLUMN IF NOT EXISTS sent_n   integer NOT NULL DEFAULT 0;
ALTER TABLE portfolio.sync_runs ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0;
