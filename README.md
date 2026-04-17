# ELT Canonical Data

![Architecture Diagram](tools/alphastream_system_architecture.png)

Infrastructure, ingestion pipelines, and canonical data models for the AlphaStream data system.

## Data Lineage

### ELT Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.png)

Ingestion → raw tables → canonical data models (CDM).

### ETL Inference
![ETL Inference Lineage](tools/inference_lineage_graph.png)

Metrics and inference tables. Table names masked for proprietary reasons.

## Interactive Analytics Dashboard

![Dashboard](tools/alpha_forecast.jpg)

A one-screen view of how a metric's distribution has changed over time — designed so patterns and data gaps stand out at a glance.

**You can:**
- Compare past vs. future distributions side by side across groups.
- Filter dynamically by group, time window, and environment.
- See how many records back each group, so you know which ones to trust.

**Built with:** R + Shiny, Plotly, PostgreSQL — packaged in Docker so it runs the same locally and in production.

## Project Timeline

- **2024 — Foundation.** Built the ELT pipeline in Python, dbt, and Airflow; designed the initial canonical data model; stood up separate dev and staging environments to ship safely.
- **2025 — Scale.** Migrated ingestion to a more reliable market data API, consolidated data from multiple sources into a single pipeline, added a comprehensive metrics layer over the full historical dataset, and moved pipeline execution from local machines into the cloud.
- **2026 Q1 — Production Infrastructure.** Split the codebase into a public infrastructure repo and a private proprietary-logic repo, containerized the full stack with Docker for environment parity, migrated hosting to DigitalOcean, and switched to a managed PostgreSQL database to offload maintenance.
- **2026 Q2 — Inference & Dashboard.** Refactored the inference layer to improve signal quality and data integrity, added quality safeguards so unreliable results are flagged rather than shown as signals, polished the analytics dashboard, and formalized a ranked backlog of known model limitations.
- **Ongoing.** Continuous refinement of predictive models, performance tuning, and new analytical features.

---

## Documentation

See the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md)
- [**Development Guide**](docs/development_guide.md)
- [**Operations Manual**](docs/operations_manual.md)
- [**Security**](docs/security.md)
- [**Cheat Sheet**](docs/cheat_sheet.md)
