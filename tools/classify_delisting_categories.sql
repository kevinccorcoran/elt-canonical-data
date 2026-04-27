-- Heuristic classifier for raw.ticker_metadata.delisting_category.
--
-- Polygon does NOT publish the reason a ticker was delisted. This SQL
-- derives a best-guess category from three signals already in the DB:
--   1. sic_code               (Phase 1.5 enrichment, /tickers detail endpoint)
--   2. name                   (Phase 1, /tickers list endpoint)
--   3. listing duration       (delisted_utc - list_date)
--   4. last close price       (raw.api_data_ingestion_massive_delisted)
--
-- Rules are evaluated top-to-bottom; first match wins.
--
-- Categories:
--   TICKER_VARIANT      — when-issued / ex-distribution / share-class
--                         clones of legit tickers (e.g. IBMw, MMMw,
--                         AEBIV, LGF.A). Brief artifacts of corporate
--                         actions; not investable as standalone equity.
--   SPAC                — sic 6770 OR name contains 'Acquisition Corp(oration)'
--                         / 'Blank Check'
--   SHELL_OR_FAILED_IPO — listed < 2 years OR fewer than 60 bars in
--                         our backfill (catches ticker renames where
--                         metadata list_date is the parent company's
--                         IPO but actual trading under the symbol was
--                         brief, e.g. VAPE post-CEAD rename)
--   BANKRUPTCY          — last close < $1.00 (worthless)
--   DISTRESSED          — last close $1.00–$4.99 (penny-ish, ambiguous —
--                         could be biotech bust, going-concern issues,
--                         reverse-split candidate, etc.)
--   M&A_OR_PRIVATE      — last close >= $5 (clean termination at decent
--                         price; cannot distinguish M&A from take-private
--                         without 8-K filings). The SHELL_OR_FAILED_IPO
--                         rule already removes < 2-year listings, so any
--                         survivor with a $5+ close is treated as a
--                         deal-driven exit rather than a slow death.
--   UNKNOWN             — none of the above (no bars, missing list_date,
--                         or doesn't fit any rule)
--
-- Idempotent: re-run anytime to refresh after new rows are added.
-- Cost: single pass over raw.ticker_metadata + one pass over the bars
-- table (aggregated). Should take a few seconds even at full scale.

WITH last_close AS (
    SELECT
        ticker,
        (array_agg(close ORDER BY date DESC))[1] AS last_close,
        MAX(date) AS last_date,
        COUNT(*) AS bar_count
    FROM raw.api_data_ingestion_massive_delisted
    GROUP BY ticker
),
classified AS (
    SELECT
        m.ticker,
        CASE
            -- 0. Ticker variants: when-issued / ex-distribution / share
            --    classes. These often share the parent company's
            --    list_date and have a normal-looking last close, so they
            --    must be detected by ticker shape and name BEFORE the
            --    price-based rules — otherwise IBMw, MMMw etc. get
            --    mis-tagged as M&A_OR_PRIVATE.
            WHEN m.ticker ~ '[a-z]$'                  -- IBMw, MMMw, ...
              OR m.ticker ~ '\.[A-Z]$'                -- LGF.A, BWL.A, ...
              OR m.ticker LIKE '%V'                   -- AEBIV, TSVTV, ...
              OR m.name ILIKE '%When Issued%'
              OR m.name ILIKE '%When-Issued%'
              OR m.name ILIKE '%Ex-distribution%'
              OR m.name ILIKE '%Ex Distribution%'
                THEN 'TICKER_VARIANT'

            -- 1. SPAC takes precedence: SIC 6770 (Blank Checks) is
            --    Polygon's authoritative tag, with name fallback for
            --    rows where SIC is missing.
            WHEN m.sic_code = '6770'
              OR m.name ILIKE '%Acquisition Corp%'
              OR m.name ILIKE '%Acquisition Corporation%'
              OR m.name ILIKE '%Blank Check%'
                THEN 'SPAC'

            -- 2. Short-lived listings (non-SPAC) — failed IPOs, shells,
            --    spin-offs that didn't survive. Also catches ticker
            --    renames where the parent company's list_date looks long
            --    but actual trading under this symbol was brief
            --    (e.g. VAPE post-CEAD rename, 34 bars).
            WHEN (m.list_date IS NOT NULL
                  AND (m.delisted_utc::date - m.list_date) < 730)
              OR (lc.bar_count IS NOT NULL AND lc.bar_count < 60)
                THEN 'SHELL_OR_FAILED_IPO'

            -- 3. Worthless / bankruptcy: ended under $1.
            WHEN lc.last_close IS NOT NULL AND lc.last_close < 1.0
                THEN 'BANKRUPTCY'

            -- 4. Clean termination at decent price ($5+). Polygon
            --    doesn't expose M&A vs take-private, so they share a
            --    label. Short-lived names already filtered out above.
            WHEN lc.last_close IS NOT NULL AND lc.last_close >= 5.0
                THEN 'M&A_OR_PRIVATE'

            -- 5. Penny-but-not-dead: distressed, biotech bust, reverse
            --    split candidate. Hard to call definitively.
            WHEN lc.last_close IS NOT NULL
              AND lc.last_close >= 1.0
              AND lc.last_close < 5.0
                THEN 'DISTRESSED'

            ELSE 'UNKNOWN'
        END AS category
    FROM raw.ticker_metadata m
    LEFT JOIN last_close lc USING (ticker)
    WHERE m.delisted_utc IS NOT NULL
)
UPDATE raw.ticker_metadata m
SET delisting_category = c.category,
    last_refreshed_at = NOW()
FROM classified c
WHERE m.ticker = c.ticker
  AND (m.delisting_category IS DISTINCT FROM c.category);

-- Show distribution after run.
SELECT delisting_category, COUNT(*) AS n
FROM raw.ticker_metadata
WHERE delisted_utc IS NOT NULL
GROUP BY delisting_category
ORDER BY n DESC;
