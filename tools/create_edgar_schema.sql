-- SEC EDGAR fundamentals, raw layer.
--
-- Source: the free XBRL APIs on data.sec.gov (companyfacts, submissions)
-- plus www.sec.gov/files/company_tickers.json. Loaded by
-- src/datapipeline/ingestion/edgar_to_raw_etl.py (bulk once, then daily).
-- Doc: docs/edgar_fundamentals.md.
--
-- Rules this schema enforces:
--   * Facts are stored exactly as reported. No cleaning, no tag mapping.
--   * edgar.facts and edgar.filings are INSERT-ONLY. A number repeated or
--     restated in a later filing is a new row with its own accession and
--     filed_date; the earlier row is never touched. That history is what
--     makes point-in-time (as-of) queries possible.
--   * Loads are idempotent: the unique keys below plus ON CONFLICT DO NOTHING
--     mean a rerun adds no rows.
--   * Size: the full facts table is ~125M rows (~30 GB with indexes, 2026-10).
--   * cik is TEXT, 10 digits, zero padded ('0000320193'), the same format as
--     raw.ticker_metadata.cik, so the two join directly.
--
-- Idempotent (IF NOT EXISTS), safe to rerun. Run by hand on each database:
--   psql "$DATABASE_URL" -f tools/create_edgar_schema.sql

CREATE SCHEMA IF NOT EXISTS edgar;

-- One row per CIK that has XBRL facts. Reference data, refreshed on every
-- load (latest attributes only; name / SIC / tickers are NOT point-in-time).
CREATE TABLE IF NOT EXISTS edgar.companies (
    cik                     TEXT        PRIMARY KEY CHECK (cik ~ '^[0-9]{10}$'),
    entity_name             TEXT,
    entity_type             TEXT,
    sic                     TEXT,
    sic_description         TEXT,
    category                TEXT,           -- filer status, e.g. 'Large accelerated filer'
    fiscal_year_end         TEXT,           -- MMDD, e.g. '0926'
    state_of_incorporation  TEXT,
    tickers                 TEXT[],         -- SEC's CURRENT tickers for the CIK
    exchanges               TEXT[],
    former_names            JSONB,          -- [{name, from, to}] as SEC reports it
    first_seen_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_refreshed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- SEC's current ticker map (company_tickers.json). Upserted on every load, so
-- first_seen_at / last_seen_at build a history of mappings over time. Rows
-- whose last_seen_at is older than the latest load are no longer in the map.
CREATE TABLE IF NOT EXISTS edgar.company_tickers (
    cik             TEXT        NOT NULL CHECK (cik ~ '^[0-9]{10}$'),
    ticker          TEXT        NOT NULL,
    title           TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, ticker)
);

CREATE INDEX IF NOT EXISTS idx_edgar_company_tickers_ticker
    ON edgar.company_tickers (ticker);

-- One row per (cik, accession) from the submissions feed, filing_date >=
-- 2009-01-01, every form type. Keyed on both because a combined filing
-- (parent + subsidiary registrants) carries one accession for several CIKs.
-- acceptance_datetime_raw is SEC's submissions string as sent. It is NOT a
-- reliable clock: checked against SEC's RSS (exact New York times), it is
-- true UTC for some filings and off by +/- the New York UTC offset for
-- others. Do not use it for intraday point-in-time logic (see the doc).
CREATE TABLE IF NOT EXISTS edgar.filings (
    cik                     TEXT        NOT NULL CHECK (cik ~ '^[0-9]{10}$'),
    accession_number        TEXT        NOT NULL,   -- '0000320193-20-000096'
    form                    TEXT,
    filing_date             DATE        NOT NULL,
    report_date             DATE,
    acceptance_datetime_raw TEXT,                   -- e.g. '2020-10-29T22:06:25.000Z', as sent
    act                     TEXT,
    file_number             TEXT,
    film_number             TEXT,
    items                   TEXT,                   -- 8-K item list, e.g. '2.02,9.01'
    size_bytes              BIGINT,
    is_xbrl                 BOOLEAN,
    is_inline_xbrl          BOOLEAN,
    primary_document        TEXT,
    primary_doc_description TEXT,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, accession_number)
);

CREATE INDEX IF NOT EXISTS idx_edgar_filings_filing_date
    ON edgar.filings (filing_date);

-- Every version of every tagged number in companyfacts (us-gaap, ifrs-full,
-- dei, srt, ...). Columns map 1:1 to the companyfacts JSON:
--   start -> period_start (NULL for instant facts such as balances),
--   end -> period_end, val -> value, accn -> accession_number,
--   fy -> fiscal_year, fp -> fiscal_period, filed -> filed_date.
-- CAUTION: fiscal_year / fiscal_period describe the FILING the number came
-- from, not the period it measures (a FY2022 10-K repeats FY2020 revenue with
-- fiscal_year = 2022). Select periods by period_start / period_end.
-- CAUTION: frame is assigned by SEC after the fact to one version per
-- calendar period. It is hindsight; never use it to pick the as-of value.
CREATE TABLE IF NOT EXISTS edgar.facts (
    cik                 TEXT        NOT NULL CHECK (cik ~ '^[0-9]{10}$'),
    taxonomy            TEXT        NOT NULL,       -- 'us-gaap', 'ifrs-full', 'dei', ...
    tag                 TEXT        NOT NULL,       -- 'Revenues', 'EarningsPerShareDiluted', ...
    unit                TEXT        NOT NULL,       -- 'USD', 'USD/shares', 'shares', 'EUR', ...
    value               NUMERIC     NOT NULL,
    period_start        DATE,
    period_end          DATE        NOT NULL,
    fiscal_year         INTEGER,
    fiscal_period       TEXT,                       -- 'FY', 'Q1'..'Q4', 'H1', ...
    form                TEXT        NOT NULL,       -- '10-K', '10-Q', '10-K/A', '20-F', ...
    accession_number    TEXT        NOT NULL,
    filed_date          DATE        NOT NULL,       -- EDGAR filing date; usable from the NEXT trading day
    frame               TEXT,                       -- e.g. 'CY2020', 'CY2020Q4I' (hindsight)
    -- Identity of a fact: one value for one concept, unit and period in one
    -- filing. md5 of the natural key, computed by Postgres. period_start is
    -- '' for instants so NULL cannot slip past the unique index. Dates enter
    -- as day numbers because date::text depends on DateStyle (not immutable).
    -- value is in the key so two different numbers for the same period in
    -- the same filing are both kept, as reported. frame is not in the key
    -- (SEC can reassign it).
    fact_key            UUID GENERATED ALWAYS AS (md5(
                            cik || '|' || taxonomy || '|' || tag || '|' || unit || '|' ||
                            accession_number || '|' ||
                            (period_end - DATE '2000-01-01')::text || '|' ||
                            coalesce((period_start - DATE '2000-01-01')::text, '') || '|' ||
                            value::text
                        )::uuid) STORED,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- A number cannot be known before its period closes. SEC data holds
    -- ~25k rows (0.02%) that break this: forecasts ("expected contributions
    -- next year") and context-date typos. The loader routes those to
    -- edgar.facts_quarantine, so this table can never leak them.
    CONSTRAINT ck_edgar_facts_period_end_not_after_filed CHECK (period_end <= filed_date)
);

-- Idempotency key (ON CONFLICT DO NOTHING). Leading cik keeps the bulk load
-- (companies arrive in CIK order) appending to one region of the index, and
-- doubles as the per-company lookup path.
CREATE UNIQUE INDEX IF NOT EXISTS uq_edgar_facts
    ON edgar.facts (cik, fact_key);

-- Cross-section path for the indicator layer: one tag across all companies.
CREATE INDEX IF NOT EXISTS idx_edgar_facts_tag_cik
    ON edgar.facts (tag, cik);

-- Facts whose period ends after the filing that reported them: SEC's
-- as-reported forecasts and tagging typos. Same columns and key as
-- edgar.facts; kept so raw stays complete, kept apart so no backtest can
-- pick them up by accident.
CREATE TABLE IF NOT EXISTS edgar.facts_quarantine (
    LIKE edgar.facts INCLUDING DEFAULTS INCLUDING GENERATED,
    CONSTRAINT ck_edgar_facts_quarantine_cik CHECK (cik ~ '^[0-9]{10}$'),
    CONSTRAINT ck_edgar_facts_quarantine_period_end_after_filed CHECK (period_end > filed_date)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_edgar_facts_quarantine
    ON edgar.facts_quarantine (cik, fact_key);

-- Bookkeeping: one row per loader run. The daily load resumes from the
-- newest successful window_end, so a missed day is picked up next run.
CREATE TABLE IF NOT EXISTS edgar.load_runs (
    run_id              BIGSERIAL   PRIMARY KEY,
    mode                TEXT        NOT NULL,       -- 'bulk' | 'daily'
    status              TEXT        NOT NULL,       -- 'running' | 'success' | 'failed'
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    source_snapshot     TIMESTAMPTZ,                -- bulk: Last-Modified of companyfacts.zip
    window_start        DATE,
    window_end          DATE,
    ciks_processed      INTEGER,
    companies_upserted  INTEGER,
    filings_inserted    BIGINT,
    facts_inserted      BIGINT,
    quarantined         BIGINT,                     -- rows sent to edgar.facts_quarantine
    error               TEXT
);
