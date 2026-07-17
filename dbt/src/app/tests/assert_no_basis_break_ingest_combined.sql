-- Singular test: no split-adjustment basis breaks in recent active-feed bars.
-- The massive ingest appends daily bars on the provider's current adjustment
-- basis; if a split's re-adjusted history were ever discarded again (2026-07
-- incident: 27 tickers incl. CRWD 4:1), the series shows a fake one-day cliff.
-- Any returned row is a suspect unadjusted split = failure. The ingest-side
-- sentinel auto-heal (massive_to_raw_etl.heal_basis_breaks) should make this
-- never fire; this is the alerting net behind it.
WITH px AS (
    SELECT
        ticker,
        "date",
        adj_close,
        source,
        LAG(adj_close) OVER (PARTITION BY ticker ORDER BY "date") AS prev_close,
        LAG("date")    OVER (PARTITION BY ticker ORDER BY "date") AS prev_date
    FROM {{ ref('ingest_combined') }}
    WHERE "date" >= CURRENT_DATE - INTERVAL '35 days'
      AND adj_close > 0
      AND ticker NOT LIKE 'X:%'
)
SELECT
    ticker,
    "date" AS cliff_date,
    ROUND(prev_close::numeric, 2) AS prev_px,
    ROUND(adj_close::numeric, 2)  AS new_px,
    ROUND((prev_close / adj_close)::numeric, 2) AS ratio
FROM px
WHERE source = 'massive'
  AND prev_close > 0
  AND prev_date >= "date" - 7
  AND GREATEST(prev_close, adj_close) >= 5
  AND (prev_close / adj_close >= 1.8 OR prev_close / adj_close <= 0.55)
