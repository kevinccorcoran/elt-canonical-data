# AlphaStream

![Architecture Diagram](tools/alphastream_system_architecture.png?v=4)

AlphaStream is an end-to-end data and machine-learning platform. It ingests time-series data, cleans and standardizes it, then applies unsupervised clustering and statistical forecasting to produce signals, with an interactive dashboard on top.

Walk-forward backtesting shows 61.2% sign agreement across 643,466 cells (June 2026).

## Repositories

AlphaStream is split into two repositories:

- **`elt-canonical-data`** (this repo, public) — the data layer. Ingestion, raw storage, cleaned canonical tables, and shared infrastructure and documentation.
- **`inference-models`** (private) — the forecasting and signal-generation layer that sits on top of the canonical tables.

## Environments

- Two pipeline setups: local and cloud (production)
- Three databases: dev and staging (local), prod (managed)

## Pipeline

![Pipeline](tools/pipeline_grouped.png?v=2)

The end-to-end flow grouped by stage, each box labeled with the database schema it lands in. Data is pulled in, screened for quality, and standardized into a canonical layer, then split into return features and clusters (unsupervised machine learning) that feed a scoring model, which walk-forward backtesting validates before the dashboard serves it. The machine-learning stages are flagged in the diagram.

## Data Lineage

### ELT — Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.jpg)

From raw ingestion to the standardized, deduplicated tables that serve as the single source of truth for every downstream model.

### ELT — Forecasting & Signals
![ELT Inference Lineage](tools/inference_lineage_graph.jpg?v=2)

Statistical forecasting and signal-generation tables that sit on top of the canonical layer.

## Interactive Analytics Dashboard

![Dashboard](tools/alpha_forecast.jpg?v=2)

Visualizes cohorts that share similar attributes across lag time horizons spanning up to 60 years.

Supports:
- Side-by-side comparison of past and future distributions across groups.
- Filtering by group, time window, and environment.
- Record counts per group to gauge reliability.

Built with R + Shiny, Plotly, and PostgreSQL. Packaged in Docker.

### Clusters

![Cluster scatter](tools/clusters_scatter.jpg)

Groups items that behave alike into clusters and plots every member in a single view.

Supports:
- One point per member, placed by age and rate of change.
- Color-coding by cluster to compare groups at a glance.
- Toggling clusters on or off and switching environments.

### Rank Stability

![Rank Stability across walk-forward cohorts](tools/rank_stability.jpg)

Small multiples of rank stability across 84 walk-forward cohorts, one heatmap per cluster id. Each cell combines two signals at a given vingtile (5% rank bin) and horizon: whether the median beat its benchmark and whether the forecast direction was correct.

Supports:
- Green means the model was right: the median beat the benchmark and the forecast direction was correct.
- Longs (id 1-12) shade green when both hold; shorts (id 13-19) shade purple when the short worked.
- Filtering by environment, cluster, vingtile depth, metric, and cutoff range.

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
- Reworked the forecasting layer with walk-forward validation and trust scoring
- Expanded the dashboard with new views for ranges, coverage, and clusters
- Introduced new automated data-quality rules
- Added 300% data coverage, removing survivorship bias

**2026 Q3 — Reliability & Data Quality**
- Restructured the database into clean, clearly named layers
- Cut out unreliable data so the models train on trustworthy inputs
- Focused forecasts on the ranges the model handles well
- Automated monthly retraining and validation to keep the system honest

---

## Documentation

See the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md)
- [**Development Guide**](docs/development_guide.md)
- [**Operations Manual**](docs/operations_manual.md)
- [**Security**](docs/security.md)
- [**Cheat Sheet**](docs/cheat_sheet.md)
