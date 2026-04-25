-- Ticker universe metadata (active + delisted).
-- Phase 1 upserts rows here; Phase 2 reads rows where
-- delisted_utc IS NOT NULL AND aggregates_fetched = FALSE,
-- fetches daily bars into raw.api_data_ingestion_massive_delisted,
-- then flips aggregates_fetched = TRUE on success.
-- Delisted bars live in a SEPARATE table from the active bars
-- (raw.api_data_ingestion_massive) so the live-ingestion table
-- stays survivor-only and the historical backfill can be rebuilt
-- independently.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ticker_metadata (
    ticker              TEXT        PRIMARY KEY,
    name                TEXT,
    primary_exchange    TEXT,
    currency_name       TEXT,
    locale              TEXT,
    market              TEXT,
    type                TEXT,
    active              BOOLEAN,
    delisted_utc        TIMESTAMPTZ,
    cik                 TEXT,
    composite_figi      TEXT,
    share_class_figi    TEXT,
    sic_code            TEXT,
    list_date           DATE,
    -- Heuristic categorization of WHY the ticker delisted. Populated by
    -- tools/classify_delisting_categories.sql (re-runnable). Polygon
    -- doesn't expose a delisting reason, so this is a derived best-guess
    -- from sic_code + name + last close from the bars table. Values:
    -- SPAC, BANKRUPTCY, M&A_OR_PRIVATE, DISTRESSED, SHELL_OR_FAILED_IPO,
    -- UNKNOWN. NULL = not yet classified.
    delisting_category  TEXT,
    aggregates_fetched  BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_refreshed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Backward-compat migrations (idempotent).
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS type               TEXT;
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS cik                TEXT;
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS composite_figi     TEXT;
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS share_class_figi   TEXT;
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS sic_code           TEXT;
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS list_date          DATE;
ALTER TABLE raw.ticker_metadata ADD COLUMN IF NOT EXISTS delisting_category TEXT;

CREATE INDEX IF NOT EXISTS idx_ticker_metadata_delisted_unfetched
    ON raw.ticker_metadata (delisted_utc)
    WHERE delisted_utc IS NOT NULL AND aggregates_fetched = FALSE;

CREATE INDEX IF NOT EXISTS idx_ticker_metadata_delisted_utc
    ON raw.ticker_metadata (delisted_utc);
