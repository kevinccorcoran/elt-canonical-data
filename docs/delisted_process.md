# Delisted-Ticker Process

How delisted US equities are ingested, classified, and surfaced for
survivorship-bias-free backtesting. This pipeline is **separate from the
live active-ticker pipeline** on purpose: the active tables stay
survivor-only, while delisted history is captured and rebuilt
independently.

> Provider note: in this repo "massive" == **Polygon.io** (env
> `MASSIVE_API_KEY`, the `polygon` SDK / REST endpoints).

---

## 1. Why a separate pipeline

A universe built only from *currently-listed* tickers has **survivorship
bias** — every company that went bankrupt or was acquired has silently
vanished, so backtests look better than reality. To correct this we
ingest the full delisted universe and tag which names are valid to
include in a backtest.

- `ingestion_targets.py` is an **active-only** list (~1,738 tickers) and
  can *never* contain delisted names — you don't know the thousands of
  dead symbols, and a per-ticker pull only fetches what you list.
- Delisted history therefore comes from **Polygon's reference endpoint**
  (`active=false`), not from any hand-maintained list.

---

## 2. Data flow / lineage

```
Polygon /v3/reference/tickers?market=stocks&active=false
   │  Phase 1  massive_fetch_ticker_metadata.py
   ▼
raw.ticker_metadata                         ← catalog: 1 row / delisted ticker (~23k)
   │  Phase 1.5  massive_enrich_ticker_details.py   (sic_code, list_date)
   │  Phase 2    massive_backfill_delisted_aggregates.py
   ▼
raw.api_data_ingestion_massive_delisted     ← daily bars (1990 → delist date)
   │  tools/classify_delisting_categories.sql       (sets delisting_category)
   │  dbt: ingest_massive_delisted_inc              (joins metadata, adds include_for_backtest)
   ▼
cdm.ingest_massive_delisted_inc
   │  unioned (include_for_backtest = TRUE only) into …
   ▼
cdm.ingest_massive_staging  ─▶  cdm.ingest_combined  ─▶  inference / dashboards (3838, 8890)
```

Delisted bars enter the warehouse through **`ingest_massive_staging`**
(tagged `source = 'massive_delisted'`), *not* directly into
`ingest_combined`. `ingest_combined`'s `massive` CTE passes the `source`
through, so the tag survives downstream. Delisted tickers are **not** in
the index-membership allowlist (`ticker_index_summary`), so downstream
inference models that join the allowlist still ignore them — live signals
are unaffected; only allowlist-free backtest queries see them.

---

## 3. Tables & models

### `raw.ticker_metadata` — the catalog (Phase 1)
DDL: `tools/create_raw_ticker_metadata.sql`. One row per ticker (active or
delisted), keyed on `ticker`.

| column | meaning |
|---|---|
| `ticker`, `name`, `primary_exchange`, `currency_name`, `locale`, `market` | identity |
| `type` | Polygon type — `CS`, `ADRC`, `ETF`, `PFD`, `WARRANT`, … (only `CS`/`ADRC` get bars) |
| `active`, `delisted_utc` | delisting status + date |
| `cik`, `composite_figi`, `share_class_figi`, `sic_code`, `list_date` | reference/enrichment |
| `delisting_category` | heuristic reason (see §5) — NULL until classified |
| `aggregates_fetched` | work-queue flag for Phase 2 (idempotency) |
| `first_seen_at`, `last_refreshed_at` | bookkeeping |

### `raw.api_data_ingestion_massive_delisted` — the bars (Phase 2)
DDL: `tools/create_raw_api_data_ingestion_massive_delisted.sql`. Same
shape as the active table `raw.api_data_ingestion_massive`
(`ticker_date_id`, `ticker`, `date`, OHLCV, `adj_close`, `processed_at`),
with `ON CONFLICT (ticker_date_id) DO NOTHING` for row-level idempotency.

### `cdm.ingest_massive_delisted_inc` — enriched bars (dbt)
File: `dbt/src/app/models/app/raw/ingest_massive_delisted_inc.sql`
(lives in the `raw/` folder but **materializes to the `cdm` schema**).
Incremental on `processed_at`, `unique_key = ticker_date_id`. Joins bars
to `ticker_metadata` and computes:

```sql
include_for_backtest =
  delisting_category IN ('BANKRUPTCY', 'M&A_OR_PRIVATE', 'DISTRESSED')
```

SPACs and shells are excluded — not investable as normal equity.

---

## 4. Components

| Step | File | What it does |
|---|---|---|
| Phase 1 | `src/datapipeline/ingestion/massive_fetch_ticker_metadata.py` | Paginates `/v3/reference/tickers` (`active=false`, ~23k rows) → upsert `raw.ticker_metadata`. `--status delisted`, `--delisted-since YYYY-MM-DD`, `--dry-run`. |
| Phase 1.5 | `src/datapipeline/ingestion/massive_enrich_ticker_details.py` | Ticker-details endpoint → fills `sic_code` + `list_date` so the classifier can categorize. `--where "<sql>"`. |
| Phase 2 | `src/datapipeline/ingestion/massive_backfill_delisted_aggregates.py` | For `delisted_utc IS NOT NULL AND aggregates_fetched=FALSE AND type IN ('CS','ADRC')`: fetch daily bars `1990 → delisted_utc`, write bars, flip `aggregates_fetched=TRUE`. `--limit`, `--resume-from`, `--exchanges XNYS,XNAS`, `--min-delisted-since`, `--dry-run`. |
| Classify | `tools/classify_delisting_categories.sql` | Pure SQL, idempotent — derives `delisting_category` from `sic_code` + `name` + last close. |
| Transform | dbt `ingest_massive_delisted_inc` → `ingest_massive_staging` → `ingest_combined` | Adds `include_for_backtest`, folds delisted into the combined feed. |
| Orchestration | `airflow/dags/raw_ingest_delisted_monthly.py` | Monthly DAG chaining all of the above. |
| Docs | `docker/dbt-docs-canonical/` | dbt docs site on **:8890** (lineage graph). |

---

## 5. `delisting_category`

Polygon does not expose a delisting reason, so it's a heuristic best-guess
(re-runnable via the classifier). Values:

| category | in backtest? |
|---|---|
| `BANKRUPTCY` | ✅ |
| `M&A_OR_PRIVATE` | ✅ |
| `DISTRESSED` | ✅ |
| `SPAC` | ❌ |
| `SHELL_OR_FAILED_IPO` | ❌ |
| `UNKNOWN` (incl. unclassified / NULL) | ❌ |

Only ✅ categories get `include_for_backtest = TRUE` and flow into
`ingest_massive_staging` / `ingest_combined`.

---

## 6. Running it

### Monthly refresh (normal operation)
The DAG `raw_ingest_delisted_monthly` (`0 6 1 * *`) runs the full chain:
`refresh_ticker_metadata → backfill_new_delisted_bars → enrich_ticker_details
→ classify_delisting_categories → dbt_build_delisted_and_combined`.
Catches the prior month's new delistings.

### Initial / full-range backfill (manual)
The bars backfill is ~6,600 CS/ADRC tickers; run it as a one-off, not on a
schedule. From the repo root with env loaded (`ENV`, `DATABASE_URL`,
`MASSIVE_API_KEY`):

```bash
# 1. catalog (idempotent upsert of the full delisted universe)
python -m datapipeline.ingestion.massive_fetch_ticker_metadata --status=delisted

# 2. bars — NO --min-delisted-since on a full-history plan (see §7)
python -m datapipeline.ingestion.massive_backfill_delisted_aggregates

# 3. enrich + classify
python -m datapipeline.ingestion.massive_enrich_ticker_details \
    --where "delisted_utc IS NOT NULL AND aggregates_fetched = TRUE AND sic_code IS NULL"
psql "$DATABASE_URL" -f tools/classify_delisting_categories.sql

# 4. build cdm + combined
dbt run --select ingest_massive_delisted_inc ingest_massive_staging ingest_combined
```

**Re-running is safe.** Phase 2 skips `aggregates_fetched=TRUE`, so it only
processes what's missing. To force a full re-pull (e.g. to widen depth),
`UPDATE raw.ticker_metadata SET aggregates_fetched = FALSE;` first.

---

## 7. Polygon plan window (the depth gotcha)

Polygon's historical window is plan-bound:

- **Stocks Starter** ≈ 5 years. A query whose range-end is older than the
  window returns **403** — so names delisted >5y ago can't be fetched and
  were skipped via `--min-delisted-since`. This shows up as *missing
  tickers*, not truncated history.
- **Stocks Advanced** ≈ full depth (back to ~2003). Drop
  `--min-delisted-since` and re-run Phase 2 to pull the older delistings
  the Starter window had excluded.

So "I only have 5 years of delisted data" usually means *older delisted
tickers were never fetched*, not that existing tickers are shallow.

---

## 8. Troubleshooting

**Lineage graph (8890) shows the old wiring after a model change.**
The `dbt-docs-canonical` container regenerates docs **only on startup**
(`entrypoint.sh` runs `dbt docs generate --target staging`), and an
anonymous volume in `docker/dbt-docs-canonical/docker-compose.yml`
deliberately shadows `target/` so a host-side `dbt docs generate` cannot
reach it. To refresh:
```bash
docker restart dbt-docs-canonical   # then hard-reload (Cmd+Shift+R)
```

**Newly-backfilled delisted names don't appear in `ingest_combined` /
dashboards.** They have no `delisting_category` yet →
`include_for_backtest=FALSE` → filtered out. Run **classify + dbt**:
```bash
psql "$DATABASE_URL" -f tools/classify_delisting_categories.sql
dbt run --select ingest_massive_delisted_inc ingest_massive_staging ingest_combined
```

**Some tickers return no bars (`empty`).** Expected — pre-2003 delistings
(before Polygon's data) or non-equity types. They're marked
`aggregates_fetched=TRUE` so they aren't retried.

---

## 9. Where it lives

- Pipeline created on branch `feat/ingest-delisted-universe` (2026-04-25),
  restored + wired into `ingest_massive_staging` on
  `feat/delisted-full-backfill` (2026-06-10).
- Data target: by `ENV` / `DB_DATABASE` — `staging` DB for local full
  backfills, `prod` for production.
