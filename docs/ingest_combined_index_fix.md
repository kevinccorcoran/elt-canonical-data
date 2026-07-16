# cdm.ingest_combined — index rebuild fix (2026-07-16)

## Symptom
`cdm.ingest_combined` (Postgres, ~18.7M rows staging / ~21.3M prod) periodically
lost its `(ticker, date)` index. Downstream `elt-inference-models` metrics do
per-row `LATERAL` nearest-prior-bar probes (`WHERE ticker = ? AND date <= ?
ORDER BY date DESC LIMIT 1`); with no index these seq-scan the whole table per
row, producing multi-hour runaways (observed 3,679s and 11,609s). On 2026-07-16
staging had **zero** indexes on the table, wedging three inference builds.

## Root cause
The model created its indexes in `post_hook`s with `CREATE INDEX IF NOT EXISTS`.

dbt's `table` materialization rebuilds by renaming the live table to
`ingest_combined__dbt_backup`, swapping in the new table, running the
`post_hook`s, then dropping the backup. Postgres index names are unique **per
schema**, and the backup still holds the old index names during the `post_hook`
window. So `CREATE INDEX IF NOT EXISTS idx_...` sees the name already exists (on
the backup), skips it, and then the backup is dropped, taking that index with it.

Net effect: every rebuild flips the index set to its **complement** — any index
that existed before the run is dropped, any that did not is created. Manually
adding an index therefore did not survive the next rebuild, and the set
oscillated between runs. That is why prod, staging, and dev each showed a
different subset on 2026-07-16.

## Fix
Each `post_hook` now does `DROP INDEX IF EXISTS <schema>.<name>` before
`CREATE INDEX <name>`, so all five indexes are rebuilt deterministically on the
live table regardless of prior state. Added `ANALYZE cdm.ingest_combined` after
load so the planner uses them immediately.

## Verification
- **dev**: two consecutive rebuilds both held all five indexes with a fresh
  `ANALYZE`. Under the old code the first rebuild would have wiped them.
- **staging** (18.7M rows): `dbt run --select fibonacci_past_offset_avg_prices`
  ran in 258s vs the historical 3,679s / 11,609s runaways.

## Prod deploy (requires box access — no auto-pull for this repo)
1. On the prod box: `cd /opt/elt-canonical-data && git pull origin main`
   (brings in the fix commit on `main`).
2. Rebuild so the new `post_hook`s run: `dbt run --select ingest_combined`
   (or trigger the `cdm_ingest_massive` DAG, or `+ingest_combined` monthly).
3. Verify: `SELECT indexname FROM pg_indexes WHERE schemaname='cdm'
   AND tablename='ingest_combined';` → five indexes present.

## WARNING — until the box runs the fixed code
Do **not** manually add the missing indexes to prod while the box is still on the
old code. Prod currently sits at `{idx_ingest_combined_ticker_date_desc}`; a
rebuild flips it to the other four, and both states contain a usable
`(ticker, date)` index, so prod is not wedged. If you manually bring prod to the
full five-index set under the old code, the next rebuild flips that to the
**empty** set and wedges prod. Deploy the fixed code first, then rebuild.
