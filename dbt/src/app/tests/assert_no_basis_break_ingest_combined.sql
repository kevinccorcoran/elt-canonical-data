{{ config(severity='warn') }}
-- Singular test: no split-adjustment basis breaks in recent active-feed bars.
--
-- severity=warn (2026-08-20): as a hard error this blocked the whole daily chain
-- for 5 days on two REAL moves - MRNA +177% (Phase 3 melanoma readout) and MYGN
-- -47% (guidance cut) - i.e. 2 false positives, 0 true positives. A >=1.8x
-- one-day move is not proof of a basis break; genuine re-basings are caught and
-- healed ingest-side by massive_to_raw_etl.heal_basis_breaks (fetch-vs-stored
-- sentinel). Rows here now WARN for human adjudication (Telegram failure alerts
-- carry them) instead of freezing the model on every big real repricing.
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
