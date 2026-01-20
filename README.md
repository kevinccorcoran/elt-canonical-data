```md
# ELT – Technical Setup

## Architecture

This setup uses two local repositories with isolated Python environments, while orchestration runs centrally in Docker. Airflow runs as a runtime service inside a Docker container, and dbt is installed and executed from local Python virtual environments (.venv).

Mac (macOS)
├── Git repos
│   ├── elt-canonical-data/
│   │   ├── .venv        (local Python env: dbt, scripts)
│   │   └── source code
│   └── elt-inference-models/
│       ├── .venv        (local Python env: dbt, scripts)
│       └── source code
│
└── Docker Desktop
    └── Linux Virtual Machine
        └── Docker containers
            ├── Airflow
            └── Postgres (Airflow metadata database)

Key points:
- Airflow runs entirely in Docker.
- Local .venv environments are for development only.
- Airflow does not use local Python environments.

---

## Requirements

- macOS
- Docker Desktop
- Python 3.11
- dbt
- direnv (recommended)

---

## Local Python Environment (Development)

Each repository that uses Python should have its own virtual environment.

    python3.11 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Used for:
- dbt
- ingestion scripts
- helpers and experimentation

---

## Airflow (Docker)

Airflow is not installed locally.

    docker compose up -d

Check status:

    docker ps

Airflow UI:
http://localhost:8080

Stop Airflow:

    docker compose down

---

## What Not to Do

- Do not run airflow webserver locally
- Do not run airflow scheduler locally
- Do not set AIRFLOW_HOME in your shell
- Do not install Apache Airflow into .venv

---

## Summary

- Local repos → source code + isolated Python environments
- Docker Desktop → Airflow + Airflow Postgres
- One Airflow instance orchestrates across repos
- Clear separation between development tools and orchestration
```

## Data Sources

This project ingests data from external APIs that provide large volumes of financial and market data.

API ingestion is handled via Python scripts in the local repositories and orchestrated by Airflow.  
Authentication details and API credentials are managed outside the repository and are not committed to source control.
