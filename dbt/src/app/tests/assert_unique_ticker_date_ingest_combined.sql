-- Singular test: (ticker, date) must be unique in ingest_combined.
-- ingest_combined's massive-vs-yfinance dedup assumes the massive staging
-- input is already unique per (ticker, date); this asserts the end result
-- holds. Any returned row is a duplicate bar = failure.
SELECT
    ticker,
    "date",
    COUNT(*) AS n_rows
FROM {{ ref('ingest_combined') }}
GROUP BY ticker, "date"
HAVING COUNT(*) > 1
