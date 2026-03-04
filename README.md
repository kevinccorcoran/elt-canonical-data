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

![Dashboard](tools/2026-03-04_09-40-17.jpg)

An R Shiny and Plotly dashboard used for visualizing predictive return distributions and statistical spreads.

- **Stack:** R, Shiny, Plotly, PostgreSQL
- **Features:** Dynamic filtering, live database connection, interactive dual-axis visualizations (P05/P95 spreads).

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

*   **2026 — Containerization & Production Infrastructure**
    *   **Repo Split:** Divided code into public `elt-canonical-data` (infrastructure) and private `inference-models` (proprietary logic).
    *   **Docker:** Containerized the full stack to ensure portability and environment parity across local, staging, and production.
    *   **Infrastructure:** Migrated to DigitalOcean for simpler control and lower operating costs.
    *   **Managed DB:** Transitioned to a managed PostgreSQL database to offload backups, upgrades, and maintenance.

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
