-- Singular test: (ticker, date) must be unique in ingest_massive_total_return.
-- Guards the dividend step join (each bar must land in exactly one
-- [prev_date, event_date) step). Any returned row is a duplicate = failure.
SELECT
    ticker,
    "date",
    COUNT(*) AS n_rows
FROM {{ ref('ingest_massive_total_return') }}
GROUP BY ticker, "date"
HAVING COUNT(*) > 1
