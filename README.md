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

![Dashboard](tools/alpha_forecast.jpg)

Visualizes cohorts that share similar attributes across lag time horizons spanning up to 40 years.

Supports:
- Side-by-side comparison of past and future distributions across groups.
- Filtering by group, time window, and environment.
- Record counts per group to gauge reliability.

Built with R + Shiny, Plotly, and PostgreSQL. Packaged in Docker.

## Project Timeline

- **2024 — Foundation.** ELT pipeline in Python, dbt, and Airflow. Initial canonical data model. Separate dev and staging environments.
- **2025 — Scale.** Migrated ingestion to a new data provider after the previous one was deprecated. Consolidated multiple sources into a single pipeline. Added a metrics layer over the historical dataset. Moved pipeline execution to the cloud.
- **2026 Q1 — Production Infrastructure.** Codebase split into a public infrastructure repo and a private logic repo. Stack containerized with Docker. Hosting moved to DigitalOcean. Database switched to managed PostgreSQL.
- **2026 Q2 — Forecasting & Dashboard.** Reworked the forecasting layer for signal quality and data integrity. Added safeguards to flag unreliable results. Dashboard updated. Known model limitations documented and ranked.
- **Ongoing.** Model refinement, performance tuning, new features.

---

## Documentation

See the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md)
- [**Development Guide**](docs/development_guide.md)
- [**Operations Manual**](docs/operations_manual.md)
- [**Security**](docs/security.md)
- [**Cheat Sheet**](docs/cheat_sheet.md)
