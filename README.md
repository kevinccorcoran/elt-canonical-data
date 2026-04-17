# ELT Canonical Data

![Architecture Diagram](tools/alphastream_system_architecture.png)

Infrastructure, ingestion pipelines, and canonical data models for the AlphaStream data system.

## Data Lineage

### ELT Canonical Data
![ELT Canonical Data Lineage](tools/elt_lineage_graph.png)

End-to-end flow from market data ingestion, through raw tables, into the canonical data models (CDM) that the rest of the platform builds on.

### ETL Inference
![ETL Inference Lineage](tools/inference_lineage_graph.png)

Downstream metrics and inference models built on top of the CDM. Table names are masked for proprietary reasons.

## Interactive Analytics Dashboard

![Dashboard](tools/alpha_forecast.png)

An R Shiny + Plotly dashboard for exploring expected-return distributions and risk across multiple look-back and look-ahead horizons.

- **Stack:** R, Shiny, Plotly, PostgreSQL.
- **Features:** dynamic filters, dual-axis past-vs-future distribution plots, and per-bucket signal summaries.

## Project Timeline

- **2024 — Foundation.** Built the ELT pipeline in Python, dbt, and Airflow; designed the initial canonical data model; stood up separate dev and staging environments to ship safely.
- **2025 — Scale.** Migrated ingestion to a more reliable market data API, consolidated data from multiple sources into a single pipeline, added a comprehensive metrics layer over the full historical dataset, and moved pipeline execution from local machines into the cloud.
- **2026 Q1 — Production Infrastructure.** Split the codebase into a public infrastructure repo and a private proprietary-logic repo, containerized the full stack with Docker for environment parity, migrated hosting to DigitalOcean, and switched to a managed PostgreSQL database to offload maintenance.
- **2026 Q2 — Inference & Dashboard.** Refactored the inference layer to improve signal quality and data integrity, added quality safeguards so unreliable results are flagged rather than shown as signals, polished the analytics dashboard, and formalized a ranked backlog of known model limitations.
- **Ongoing.** Continuous refinement of predictive models, performance tuning, and new analytical features.

---

## Documentation

Deeper technical details live in the `docs/` directory:

- [**Architecture & Design**](docs/architecture.md) — system overview, data flow, and environment strategy.
- [**Development Guide**](docs/development_guide.md) — local setup, tests, and Git workflows.
- [**Operations Manual**](docs/operations_manual.md) — provisioning, deployment, and recovery.
- [**Security**](docs/security.md) — network security, SSH, and secret management.
- [**Cheat Sheet**](docs/cheat_sheet.md) — quick reference for common commands.
