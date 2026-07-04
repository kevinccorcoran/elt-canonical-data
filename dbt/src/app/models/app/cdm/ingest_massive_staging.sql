{# Log the active target database at compile/run time for quick debugging #}
{% do log("Current ENV / DB_DATABASE: " ~ env_var('DB_DATABASE'), info=true) %}

{{
  config(
    materialized = 'table',

    database = env_var('DB_DATABASE'),

    on_schema_change = 'sync_all_columns',

    post_hook = [
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_date_desc_ticker
        ON {{ this }} (date DESC, ticker);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_ticker
        ON {{ this }} (ticker);
      """,
      """
      CREATE INDEX IF NOT EXISTS idx_{{ this.identifier }}_source
        ON {{ this }} (source);
      """
    ]
  )
}}

-- --------------------------------------------------------------
-- Enriched: Massive data filtered to tickers that have index membership
-- Adds: source tag + query runtime (query_run_time)
-- Output: cdm.<model_name> (materialized table)
-- --------------------------------------------------------------

WITH run_time AS (
    -- Captures when the query ran (same value for every row in this run)
    SELECT NOW() AS query_run_time
),

massive_base AS (
    -- Base: massive ingestion table filtered for non-null essentials
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        'massive' AS source,
        date_type
    FROM {{ source('cdm', 'api_data_ingestion_massive') }}
    WHERE "date" IS NOT NULL
      AND adj_close IS NOT NULL
),

ticker_index_summary AS (
    -- Keep only tickers that appear in your index membership summary
    -- (this is what enforces "only tickers with known indices")
    SELECT
        ticker
    FROM {{ ref('ticker_index_summary') }}
),

-- Delisted bars (survivorship-bias-free history). NOT filtered by the
-- index-membership allowlist, since dead tickers aren't in it. Only the
-- backtest-relevant categories (include_for_backtest excludes SPACs/shells).
delisted AS (
    SELECT
        "date",
        ticker,
        adj_close,
        processed_at,
        ticker_date_id,
        'massive_delisted' AS source,
        NULL AS date_type
    FROM {{ ref('ingest_massive_delisted_inc') }}
    WHERE include_for_backtest = TRUE
)

-- Active universe: massive bars filtered to the index-membership allowlist
SELECT
    mb."date",
    mb.ticker,
    mb.adj_close,
    mb.processed_at,
    mb.ticker_date_id,
    mb.source,
    mb.date_type,
    rt.query_run_time
FROM massive_base mb
JOIN ticker_index_summary tis
    ON mb.ticker = tis.ticker
CROSS JOIN run_time rt

UNION ALL

-- Delisted universe: not allowlist-filtered (dead tickers aren't in it).
-- Anti-join against the active universe so a (ticker, date) that exists in
-- both feeds is emitted ONCE, from the active (massive) side. ~50 tickers
-- appear in both feeds; without this the UNION ALL double-counts every
-- overlapping bar (n_obs inflation) and, where the two feeds disagree on
-- adj_close (~3.9% of overlaps, up to ~7%), leaves duplicate (ticker, date)
-- rows that make downstream FIRST_VALUE / DISTINCT tie-break nondeterministically.
-- Active wins because it is the continuously split/dividend-adjusted feed;
-- the delisted feed is a static monthly backfill. Every dual-feed ticker's
-- delisted date range is a strict subset of its active range, so this drops
-- only genuine overlaps and preserves all delisted-only history (COMM/NGD).
SELECT
    d."date",
    d.ticker,
    d.adj_close,
    d.processed_at,
    d.ticker_date_id,
    d.source,
    d.date_type,
    rt.query_run_time
FROM delisted d
CROSS JOIN run_time rt
WHERE NOT EXISTS (
    SELECT 1
    FROM massive_base mb
    JOIN ticker_index_summary tis
        ON mb.ticker = tis.ticker
    WHERE mb.ticker = d.ticker
      AND mb."date" = d."date"
)