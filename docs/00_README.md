# ELT – Technical Setup

## One-Minute Overview

This project implements a production-grade data pipeline with a strict separation
between **development**, **local testing**, and **runtime execution**.

Code is written and tested locally on macOS.  
Airflow can be run locally (via Docker) for **dev / staging testing only**.  
Authoritative scheduling and execution run on a remote Linux server using Airflow in Docker.  
All persistent data is stored in a managed PostgreSQL database.

The runtime server is disposable — it can be destroyed and rebuilt without data loss
because all important data and code live in managed PostgreSQL and GitHub.

---

## Architecture

### Mac (Development + Local Testing)

Application layer:
└── elt-canonical-data (Git repo)
    ├── airflow/dags/        # DAG source code
    ├── dbt models
    ├── ingestion scripts
    ├── src/                # shared Python code
    └── docker/airflow/     # Local Airflow runtime (Docker Compose)
        └── docker-compose.yml
    (.venv)

Infrastructure layer (local):
└── Docker Desktop
    └── Airflow (local dev / staging)
        ├── webserver
        ├── scheduler
        └── metadata Postgres (container)

Data layer (local):
└── Local PostgreSQL
    ├── dev database
    └── staging database

        ↓ deploy (git / rsync)

---

### Linux Server (Runtime)

Application checkout:
└── /opt/elt-canonical-data
    ├── code (git pull)
    └── docker/airflow/dags (rsync target)

Infrastructure layer:
└── Docker
    └── Airflow (authoritative runtime)
        ├── webserver
        ├── scheduler
        └── workers

        ↓ SQL

Managed PostgreSQL
└── business data
└── airflow metadata

---

This separation ensures:
- local machines are used for authoring and testing
- local Airflow is safe and non-authoritative
- all real execution happens in a controlled runtime environment

---

## Requirements (Development)

- macOS  
- Python 3.11  
- dbt  
- Docker Desktop (required for local Airflow)  
- direnv (recommended)

---

## Local Python Environment (Development Only)

Each repository that uses Python has its own virtual environment.

    python3.11 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Used for:
- dbt
- ingestion scripts
- helpers and experimentation

Not used for:
- Airflow
- scheduling
- orchestration

---

## Airflow (Local Dev / Staging)

Airflow can be run locally via Docker for testing DAGs
against local dev or staging databases.

Location:

    docker/airflow/

Start:

    cd docker/airflow
    docker compose up -d

Stop:

    docker compose down

Status:

    docker compose ps

UI:

    http://localhost:8080

Local Airflow is **never** used for production workloads.

See `06_airflow_local.md` for full details.

---

## Airflow (Runtime Server)

The runtime Airflow:
- runs on a Linux server
- is authoritative
- executes real workloads only

Start services:

    cd /opt/elt-canonical-data
    docker compose up -d

Stop services:

    docker compose down

Check status:

    docker compose ps

Airflow UI (private):

    http://<RUNTIME_SERVER_IP>:8080

---

## Database

- PostgreSQL is provided by **DigitalOcean Managed Database**
- Airflow metadata is stored in managed Postgres
- The database is not containerized
- Credentials are provided via `.env` on the runtime server

The database persists even if the runtime server is destroyed.

---

## Configuration & Secrets

Runtime configuration is provided via a `.env` file on the runtime server.

Required variables include:

- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN  
- AIRFLOW__CORE__EXECUTOR  
- AIRFLOW__CORE__FERNET_KEY  
- AIRFLOW__WEBSERVER__SECRET_KEY  

Secrets are:
- stored only on the runtime server
- never committed to Git
- retrieved from the DigitalOcean DB console when rebuilding

---

## DAG Deployment Model

There is a strict separation between DAG source code and the Airflow runtime.

Source of truth:
- airflow/dags/

Runtime mount:
- docker/airflow/dags/

Airflow reads DAGs from:
- /opt/airflow/dags (inside container)

DAGs must be explicitly deployed.

### Supported deployment command (Mac → Runtime Server)

    rsync -av --delete airflow/dags/ \
      $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/

No DAGs are ever edited directly on the runtime server.

---

## Infrastructure Provisioning

Runtime infrastructure is created manually or via scripts.

Rebuild is the default recovery strategy.

See:
- docs/11_recovery.md
- docs/05_runtime_provisioning.md

---

## Server Access

Runtime access uses environment variables defined locally:

- ELT_SERVER_IP
- ELT_SERVER_USER
- ELT_SERVER_PATH

Example:

    ssh $ELT_SERVER_USER@$ELT_SERVER_IP
    cd $ELT_SERVER_PATH

No development work is performed directly on the runtime server.

---

## What Not To Do

- Do not install Airflow into `.venv`
- Do not run `airflow webserver` manually
- Do not run schedulers locally for production
- Do not edit DAGs inside runtime directories
- Do not debug broken servers for hours — rebuild instead

---

## Design Philosophy

- Local = development and testing only
- Local Airflow = dev / staging safety net
- Runtime server = authoritative execution
- Docker = execution boundary
- Managed Postgres = durable state
- Runtime server = disposable

If something breaks badly, rebuild.

That is the system design.
