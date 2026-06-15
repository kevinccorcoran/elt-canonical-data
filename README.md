# AlphaStream

![Architecture Diagram](tools/alphastream_system_architecture.png)

AlphaStream is an end-to-end data and analytics platform. It ingests time-series data, cleans and standardizes it, and produces statistical forecasts with an interactive dashboard on top.

## Repositories

AlphaStream is split into two repositories:

- **`elt-canonical-data`** (this repo, public) — the data layer. Ingestion, raw storage, cleaned canonical tables, and shared infrastructure and documentation.
- **`inference-models`** (private) — the forecasting and signal-generation layer that sits on top of the canonical tables.

## Data Lineage

### ELT — Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.png)

From raw ingestion to the standardized, deduplicated tables that serve as the single source of truth for every downstream model.

### ETL — Forecasting & Signals
![ETL Inference Lineage](tools/inference_lineage_graph.jpg)

Statistical forecasting and signal-generation tables that sit on top of the canonical layer.

## Interactive Analytics Dashboard

![Dashboard](tools/alpha_forecast.jpg?v=2)

Visualizes cohorts that share similar attributes across lag time horizons spanning up to 40 years.

Supports:
- Side-by-side comparison of past and future distributions across groups.
- Filtering by group, time window, and environment.
- Record counts per group to gauge reliability.

Built with R + Shiny, Plotly, and PostgreSQL. Packaged in Docker.

## Project Timeline

**2024 — Foundation**
- ELT pipeline built on Python, dbt, and Airflow
- Initial canonical data model
- Separate dev and staging environments

**2025 — Scale**
- Migrated ingestion to a new data provider after the previous one was deprecated
- Consolidated multiple sources into a single pipeline
- Added a metrics layer over the historical dataset
- Moved pipeline execution to the cloud

**2026 Q1 — Production Infrastructure**
- Codebase split into a public infrastructure repo and a private logic repo
- Stack containerized with Docker
- Hosting moved to DigitalOcean
- Database switched to managed PostgreSQL

**2026 Q2 — Forecasting & Dashboard**
- Reworked the forecasting layer for signal quality and data integrity
- Added safeguards to flag unreliable results
- Dashboard refreshed
- Known model limitations documented and ranked

**Ongoing**
- **Data quality rules** — programmatic ticker exclusion via cross-feed checks (`ticker_reuse`, `regime_shift`, `feed_discrepancy` with `no_yfinance` fallback, `stagnation`), plus a manual-override gate at `ingest_combined`
- **Delisted universe backfill** — one-shot Polygon Flat Files pull to recover historical and delisted symbols, wired into `ingest_massive_staging` and the monthly DAG
- **Inference dependency wiring** — new canonical assets (`ticker_global_action_current`, `ticker_pair_current`, `ticker_summary_current`) linked into the inference DAG
- Continued model refinement and performance tuning

---

## Documentation

See the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md)
- [**Development Guide**](docs/development_guide.md)
- [**Operations Manual**](docs/operations_manual.md)
- [**Security**](docs/security.md)
- [**Cheat Sheet**](docs/cheat_sheet.md)
