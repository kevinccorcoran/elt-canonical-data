-- SEC EDGAR full financial statements, raw layer (fsn_* tables).
--
-- Source: SEC's Financial Statement and Notes data sets
-- (https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets),
-- quarterly zips 2009q1..2025q3, monthly since 2025_10. Loaded by
-- src/datapipeline/ingestion/edgar_statements_to_raw_etl.py.
-- Doc: docs/edgar_fundamentals.md ("Full statements").
--
-- What this adds over edgar.facts (companyfacts): the splits inside a line
-- (Products / Services, segments, regions), company-specific custom tags,
-- and the statement layout (which statement, line order, the company's own
-- label).
--
-- Scope (2026-10 decision): filings by CIKs in raw.ticker_metadata or SEC's
-- ticker map (edgar.company_tickers); the five primary statements (balance
-- sheet, income, cash flow, equity, comprehensive income); every number
-- whose tag is a line on one of those statements, with all its splits.
-- Note-only numbers are left out. ~225M numbers, ~40 GB with indexes
-- (measured on 2024q4: 3.5M numbers, 136 bytes each).
--
-- Compact integer ids (sub_id, tag_id, dim_id) keep fsn_numbers small;
-- edgar.fsn_statement_lines is the readable view.
--
-- Rules: insert-only, idempotent (ON CONFLICT DO NOTHING on source keys),
-- values as SEC publishes them. SEC itself rounds ddate to the nearest
-- month end, qtrs to whole quarters and value to 4 decimals; datp / durp
-- carry the offsets. SUB.prevrpt is not loaded: it flags filings amended
-- LATER, which is hindsight.
--
-- Idempotent (IF NOT EXISTS), safe to rerun. Run by hand on each database
-- after tools/create_edgar_schema.sql:
--   psql "$DATABASE_URL" -f tools/create_edgar_statements_schema.sql

CREATE SCHEMA IF NOT EXISTS edgar;

-- One row per data-set zip, so a rerun skips what is already loaded and a
-- republished zip (new Last-Modified) is loaded again (insert-only, free).
CREATE TABLE IF NOT EXISTS edgar.fsn_files (
    file_name               TEXT        PRIMARY KEY,    -- '2024q4_notes', '2026_09_notes'
    url                     TEXT        NOT NULL,
    last_modified           TIMESTAMPTZ,
    size_bytes              BIGINT,
    status                  TEXT        NOT NULL,       -- 'running' | 'success' | 'failed'
    started_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at             TIMESTAMPTZ,
    submissions_inserted    INTEGER,
    presentation_inserted   INTEGER,
    numbers_inserted        BIGINT,
    error                   TEXT
);

-- SUB: one row per XBRL filing in scope.
-- accepted_at: SUB.accepted is New York wall time (checked 2026-10-20:
-- 7,031 of 7,031 filings match SEC's RSS acceptance time to the minute),
-- stored here converted to timestamptz. Unlike
-- edgar.filings.acceptance_datetime_raw it is a reliable clock.
CREATE TABLE IF NOT EXISTS edgar.fsn_submissions (
    sub_id              INTEGER     GENERATED ALWAYS AS IDENTITY UNIQUE,
    accession_number    TEXT        PRIMARY KEY,        -- adsh
    cik                 TEXT        NOT NULL CHECK (cik ~ '^[0-9]{10}$'),
    name                TEXT,                           -- as of the filing date
    sic                 TEXT,                           -- as of the filing date
    countryba           TEXT,
    stprba              TEXT,
    countryinc          TEXT,
    afs                 TEXT,                           -- filer status at filing: 1-LAF .. 5-SML
    wksi                BOOLEAN,
    fye                 TEXT,                           -- MMDD
    form                TEXT        NOT NULL,
    period              DATE,                           -- balance sheet date
    fy                  INTEGER,                        -- fiscal year focus of the FILING
    fp                  TEXT,                           -- fiscal period focus of the FILING
    filed               DATE        NOT NULL,
    accepted_at         TIMESTAMPTZ,
    detail              BOOLEAN,
    instance            TEXT,
    nciks               INTEGER,
    aciks               TEXT,                           -- co-registrant CIKs, space separated
    pubfloatusd         NUMERIC,
    floatdate           DATE,
    source_file         TEXT        NOT NULL,           -- fsn_files.file_name
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_edgar_fsn_submissions_cik
    ON edgar.fsn_submissions (cik, filed);

-- TAG: every tag referenced by a loaded number or line. Custom tags are
-- per filing (version = the accession that defined them). The long
-- definition text (TAG.doc) is not loaded; tlabel and the statement's own
-- plabel cover display.
CREATE TABLE IF NOT EXISTS edgar.fsn_tags (
    tag_id      INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag         TEXT        NOT NULL,
    version     TEXT        NOT NULL,   -- 'us-gaap/2024', or an accession for custom tags
    custom      BOOLEAN,
    abstract    BOOLEAN,
    datatype    TEXT,
    iord        TEXT,                   -- 'I' instant, 'D' duration
    crdr        TEXT,                   -- 'C' credit, 'D' debit
    tlabel      TEXT,
    UNIQUE (tag, version)
);

-- DIM: one row per combination of axis=member pairs. dim_id 0 is "no
-- dimension": the plain, consolidated total.
CREATE TABLE IF NOT EXISTS edgar.fsn_dimensions (
    dim_id      INTEGER     GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    dimh        TEXT        NOT NULL UNIQUE,    -- SEC's md5 of segments, '0x...'
    segments    TEXT        NOT NULL,           -- 'ProductOrService=Products;'
    segt        BOOLEAN                         -- segments truncated at 1024 chars
);

INSERT INTO edgar.fsn_dimensions (dim_id, dimh, segments, segt)
VALUES (0, '0x00000000', '', FALSE)
ON CONFLICT DO NOTHING;

-- NUM: every number (all periods, all splits) whose tag is a line on one of
-- the five statements of that filing. Key = SEC's NUM key.
CREATE TABLE IF NOT EXISTS edgar.fsn_numbers (
    sub_id      INTEGER     NOT NULL,
    tag_id      INTEGER     NOT NULL,
    ddate       DATE        NOT NULL,   -- period end, rounded by SEC to month end
    qtrs        SMALLINT    NOT NULL,   -- 0 = instant, 1 = quarter, 4 = year; 2/3 = YTD
    uom         TEXT        NOT NULL,
    dim_id      INTEGER     NOT NULL,   -- 0 = no dimension (the total)
    iprx        SMALLINT    NOT NULL,   -- 0 = primary; >0 = same fact at lower precision
    value       NUMERIC,                -- NULL when the filer tagged it nil
    coreg       TEXT,                   -- co-registrant; NULL = consolidated entity
    footnote    TEXT,
    durp        REAL,                   -- duration offset vs qtrs, fraction
    datp        REAL,                   -- date offset vs ddate, fraction of the month
    dcml        SMALLINT,               -- XBRL decimals; 32767 = INF
    PRIMARY KEY (sub_id, tag_id, ddate, qtrs, uom, dim_id, iprx)
);

-- PRE: the statement layout. One row per line of the five statements.
CREATE TABLE IF NOT EXISTS edgar.fsn_presentation (
    sub_id      INTEGER     NOT NULL,
    report      INTEGER     NOT NULL,   -- R-file number in the filing
    line        INTEGER     NOT NULL,   -- order within the report
    stmt        TEXT        NOT NULL,   -- BS | IS | CF | EQ | CI
    inpth       BOOLEAN,                -- shown in parentheses
    tag_id      INTEGER     NOT NULL,
    prole       TEXT,
    plabel      TEXT,                   -- the company's own line label
    negating    BOOLEAN,                -- shown with the sign flipped
    PRIMARY KEY (sub_id, report, line)
);

-- Readable view: one row per statement line x number (period, split).
-- A tag that sits on two statements (net income on IS and CF) appears on
-- both; a split of a line's tag that is shown only in the notes (segment
-- revenue) comes along with that line, with its segments text.
CREATE OR REPLACE VIEW edgar.fsn_statement_lines AS
SELECT s.cik,
       s.name,
       s.accession_number,
       s.form,
       s.fy,
       s.fp,
       s.period,
       s.filed,
       s.accepted_at,
       p.stmt,
       p.report,
       p.line,
       p.plabel,
       p.negating,
       p.inpth,
       t.tag,
       t.version,
       t.custom,
       n.ddate,
       n.qtrs,
       n.uom,
       d.segments,
       n.coreg,
       n.iprx,
       n.value
FROM edgar.fsn_presentation p
JOIN edgar.fsn_submissions  s ON s.sub_id = p.sub_id
JOIN edgar.fsn_tags         t ON t.tag_id = p.tag_id
JOIN edgar.fsn_numbers      n ON n.sub_id = p.sub_id AND n.tag_id = p.tag_id
JOIN edgar.fsn_dimensions   d ON d.dim_id = n.dim_id;
