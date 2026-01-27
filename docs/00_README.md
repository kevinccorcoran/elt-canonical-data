# ELT – Technical Setup

## One-Minute Overview

This project implements a production-grade data pipeline with a strict separation
between development and runtime.

Code is written and tested locally on macOS.  
All scheduling and execution runs on a remote Linux server using Airflow in Docker.  
All persistent data is stored in a managed PostgreSQL database.

The runtime server is disposable — it can be destroyed and rebuilt without data loss
because all important data and code live in managed PostgreSQL and GitHub.

---

## Architecture

Mac (Development)

Application layer:
└── elt-canonical-data (Git repo)
    ├── dbt models
    ├── ingestion scripts
    └── DAG definitions
    (.venv)

Infrastructure layer:
└── infra/docker/airflow (Git repo)
    └── Docker Compose
        └── Airflow (local)
            ├── webserver
            ├── scheduler
            └── workers

Data layer:
└── Local PostgreSQL
    ├── dev database
    └── staging database

        ↓ deploy (rsync)

Linux Server (Runtime)

Infrastructure layer:
└── /opt/elt-canonical-data (infra copy)
    └── Docker Compose
        └── Airflow (production)
            ├── webserver
            ├── scheduler
            └── workers

        ↓ SQL

Managed PostgreSQL (Production)
└── business data
└── airflow metadata


This separation ensures local machines are used only for authoring and testing,
while all scheduling and execution happens in a controlled production environment.

Key points:
- Airflow runs entirely in Docker on the runtime server.
- Local `.venv` environments are for development only.

---

## Requirements (Development)

- macOS  
- Python 3.11  
- dbt  
- Docker Desktop (optional)  
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

## Airflow (Runtime Only)

Airflow runs **only** on the runtime server to avoid coupling development
machines with production scheduling and state.

Start services:

    docker compose up -d

Stop services:

    docker compose down

Check status:

    docker compose ps

Airflow UI:

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
- Stored only on the runtime server  
- Never committed to Git  
- Retrieved from DigitalOcean DB console when rebuilding  

---

## DAG Deployment Model

There is a strict separation between DAG source code and the Airflow runtime.

Source of truth:
- airflow/dags/ (development repository)

Runtime mount:
- docker/airflow/dags/ (mounted into Airflow containers)

Airflow only reads DAGs from:
- /opt/airflow/dags (inside container)

Therefore DAGs must be explicitly deployed from source to runtime.

This ensures production only runs code that has been intentionally shipped,
not whatever happens to exist on a developer’s laptop.

The runtime DAG folder is treated as a deployment output, not a working directory.  
It can be deleted and rebuilt at any time.

### Real Deployment Command (Mac → Runtime Server)

This is the only supported way to deploy DAGs:

    rsync -av --delete airflow/dags/ $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/

No DAGs are ever edited directly on the runtime server.

This prevents:
- accidental execution of unfinished DAGs  
- unintended dependency between development and runtime  
- hidden state inside containers  

---

## Infrastructure Provisioning

Runtime infrastructure is created manually or via scripts.

Rebuild is the default recovery strategy.

See:
- docs/05_recovery.md  
- docs/runtime-provisioning.md  

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
- Do not run schedulers locally
- Do not edit DAGs inside docker/airflow/dags directly
- Do not debug broken servers for hours — rebuild instead

---

## Design Philosophy

- Local = development only  
- Runtime server = production only  
- Docker = execution boundary  
- Managed Postgres = durable data  
- Runtime server = disposable  

If something breaks badly, rebuild.

That is the system design.
