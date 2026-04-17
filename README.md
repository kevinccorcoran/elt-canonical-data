# ELT Canonical Data

![Architecture Diagram](tools/alphastream_system_architecture.png)

This repository contains the infrastructure, ingestion pipelines, and canonical data models for the AlphaStream data system.

## Data Lineage

### ELT Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.png)

*The graph above represents the data lineage for the ELT canonical data repository, showing the flow from ingestion and raw tables to the canonical data models (CDM).*

### ETL Inference
![ETL Inference Lineage](tools/inference_lineage_graph.png)

*The graph above represents the data lineage for the ETL inference repository, specifically showing the flow for the metrics and inference tables. Table names are voluntarily masked for proprietary reasons.*

## Interactive Analytics Dashboard

![Dashboard](tools/alpha_forecast.png)

An R Shiny and Plotly dashboard used for visualizing expected return distributions and statistical spreads across multiple look-ahead/look-back horizons.

- **Stack:** R, Shiny, Plotly, PostgreSQL
- **Features:** Dynamic filtering, interactive dual-axis visualizations (Past vs. Future distributions), and Alpha Z-Bucket analysis.

## Project Timeline

*   **2024 — Foundation & Prototype**
    *   **Core Pipeline:** Built the ELT pipeline using Python, dbt, and Apache Airflow.
    *   **Data Modeling:** Designed the initial Canonical Data Model (CDM).
    *   **Environments:** Established separate local development and staging environments to test changes safely.

*   **2025 — Scale, Consolidation & Metrics**
    *   **API Migration:** Migrated ingestion to a more reliable market data API following the deprecation of the previous provider.
    *   **Consolidation:** Unified data from two external APIs into a single source, consolidating Raw → CDM tables.
    *   **Metrics Layer:** Developed a comprehensive metrics layer over the full historical dataset.
    *   **Cloud Deployment:** Deployed to Heroku to run pipelines in a hosted environment instead of locally.

*   **2026 Q1 — Containerization & Production Infrastructure**
    *   **Repo Split:** Divided code into public `elt-canonical-data` (infrastructure) and private `inference-models` (proprietary logic).
    *   **Docker:** Containerized the full stack to ensure portability and environment parity across local, staging, and production.
    *   **Infrastructure:** Migrated to DigitalOcean for simpler control and lower operating costs.
    *   **Managed DB:** Transitioned to a managed PostgreSQL database to offload backups, upgrades, and maintenance.

*   **2026 Q2 — Inference Pipeline Refactor & Visualization Polish**
    *   **Pipeline simplification:** Removed a circular z-score layer from the return-cluster transition pipeline and flattened the aggregation chain; moved score formulas (Return/Improv/Risk per month) into `combined_bucket_stats` as the single source of truth.
    *   **Viability enforcement:** Added an `is_viable` flag (min-sample threshold + long-future-lag exclusion) and a `future_tail_risk_score` column; non-viable combos now emit an `INSUFFICIENT_DATA` signal instead of a trading label.
    *   **Bucket-stats accuracy:** Replaced monthly averaging with a month-end snapshot in `return_cluster_feature_set` so short-horizon signals no longer smooth over intra-month volatility.
    *   **Shiny polish:** Uniform past/future tooltip layout, true Q1/Q3 boxplots for the future distribution, smoothed bucket-share line, labels and title migrated to "Alpha" terminology, and layout tightened so annotations no longer clip.
    *   **QA documentation:** Triaged and ranked the inference-model quality backlog (multiple testing, survivorship, non-stationarity, look-ahead bias in bucket labels, SPY beta assumption, overlapping lags).

*   **Ongoing — Continuous Evolution**
    *   **Inference:** Developing and refining predictive inference models.
    *   **Quality:** Continuous performance tuning and data quality improvements.
    *   **Features:** Adding new features and extending analytical capabilities.

---

## Documentation

For technical details and setup guides, please refer to the `docs/` directory:

*   [**Architecture & Design**](docs/architecture.md): System overview, data flow, and environment strategy.
*   [**Development Guide**](docs/development_guide.md): Local setup, running tests, and Git workflows.
*   [**Operations Manual**](docs/operations_manual.md): Provisioning, deployment, and recovery procedures.
*   [**Security**](docs/security.md): Network security, SSH keys, and secret management.
*   [**Cheat Sheet**](docs/cheat_sheet.md): Quick reference for frequently used commands.
