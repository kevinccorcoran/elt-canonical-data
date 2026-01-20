# ELT – Technical Setup

## Architecture

This setup uses two local repositories with isolated Python environments, while orchestration runs centrally in Docker. Airflow runs as a runtime service inside a Docker container, and dbt is installed and executed from local Python virtual environments (`.venv`).

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
- Local `.venv` environments are for development only.
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

- Do not run `airflow webserver` locally
- Do not run `airflow scheduler` locally
- Do not set `AIRFLOW_HOME` in your shell
- Do not install Apache Airflow into `.venv`

---

## Summary

- Local repositories → source code + isolated Python environments
- Docker Desktop → Airflow + Airflow metadata database
- One Airflow instance orchestrates across repositories
- Clear separation between development tools and orchestration

---

## Data Sources

This project ingests data from external APIs that provide large volumes of financial and market data.

API ingestion is handled via Python scripts in the local repositories and orchestrated by Airflow.  
Authentication details and API credentials are managed outside the repository and are not committed to source control.

---

## Infrastructure Provisioning (DigitalOcean)

Runtime infrastructure is provisioned programmatically using the DigitalOcean API.

Droplets are created via authenticated API calls (e.g. `curl` or scripts) and are **not** managed manually through the UI. This enables reproducible, scriptable environment setup.

---

## Environments & Databases

The project operates across three environments:

- **Local development** – two local repositories, each with its own Python virtual environment
- **Staging** – a non-production environment used for integration and validation
- **Production** – hosted on DigitalOcean

Each environment uses its own database:
- Local development database
- Staging database
- Production database (DigitalOcean)

Local development runs entirely on macOS; staging and production databases are hosted remotely.

---

## Authentication

DigitalOcean authentication uses a personal access token stored as an environment variable:

    export do_token="***"

The token is:
- Stored locally in the shell environment (e.g. `.zshrc` / `.zshenv`)
- Never committed to source control
- Required only for infrastructure provisioning and management

---

## Notes

- Infrastructure credentials are intentionally kept outside the repository
- Droplet creation and lifecycle management are decoupled from Airflow and dbt
- Airflow itself runs in Docker; provisioned hosts are used for runtime workloads only
