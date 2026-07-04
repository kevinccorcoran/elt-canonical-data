-- Singular test: (ticker, date) must be unique in ingest_massive_staging.
-- Guards the active-vs-delisted dedup (the delisted branch anti-joins the
-- active universe). Any returned row is a surviving duplicate = failure.
SELECT
    ticker,
    "date",
    COUNT(*) AS n_rows
FROM {{ ref('ingest_massive_staging') }}
GROUP BY ticker, "date"
HAVING COUNT(*) > 1
