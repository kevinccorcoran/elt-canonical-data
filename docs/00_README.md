# ELT – Technical Setup

## Architecture

This project has a **strict separation between development and runtime**.

Mac (macOS) — Development  
├── Git repositories (source code)  
└── Docker Desktop (optional, local Airflow testing / debugging)  

Runtime Host (Linux VM) — Production  
└── Docker containers  
    ├── Airflow  
    └── Postgres (Airflow metadata database)  

Key points:
- Airflow runs entirely in Docker on the runtime host.
- Local `.venv` environments are for development only.
- The runtime host is disposable; the database and GitHub repo are the real assets.

Local (macOS)
  Git repositories
    - ingestion scripts
    - dbt models
    - DAG definitions
  deploy / sync
      ↓
Runtime Host (Linux VM)
  Docker
    Airflow Webserver ↔ Scheduler
        |
        SQLAlchemy
        |
Managed PostgreSQL (durable state)

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

Airflow runs **only** on the runtime host.

Start services:

    docker compose up -d

Stop services:

    docker compose down

Check status:

    docker compose ps

Airflow UI:

    http://<RUNTIME_HOST_IP>:8080

---

## Database

- PostgreSQL is provided by **DigitalOcean Managed Database**
- Airflow metadata is stored in managed Postgres
- The database is not containerized
- Credentials are provided via `.env` on the runtime host

The database persists even if the runtime host is destroyed.

---

## Configuration & Secrets

Runtime configuration is provided via a `.env` file on the runtime host.

Required variables include:

- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN  
- AIRFLOW__CORE__EXECUTOR  
- AIRFLOW__CORE__FERNET_KEY  
- AIRFLOW__WEBSERVER__SECRET_KEY  

Secrets are:
- Stored only on the runtime host  
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

The runtime DAG folder is treated as a deploy artifact, not a working directory.  
It can be deleted and rebuilt at any time.

### Real Deployment Command (Mac → Runtime Host)

This is the only supported way to deploy DAGs:

    rsync -av --delete airflow/dags/ $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/

No DAGs are ever edited directly on the runtime host.

This prevents:
- accidental execution of unfinished DAGs  
- implicit coupling between development and runtime  
- hidden state inside containers  

---

## Filesystem Invariant (Critical)

Airflow runs inside Docker as user **UID 50000**.

All mounted directories must be writable by UID 50000 or Airflow will crash.

Required ownership on the runtime host:

    chown -R 50000:0 /opt/elt-canonical-data/docker/airflow/logs
    chmod -R 775 /opt/elt-canonical-data/docker/airflow/logs

If Airflow cannot write to `/opt/airflow/logs`, the webserver will start and immediately crash.

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

No development work is performed directly on the runtime host.

---

## What Not To Do

- Do not install Airflow into `.venv`
- Do not run `airflow webserver` manually
- Do not run schedulers locally
- Do not debug broken servers for hours
- Do not edit DAGs inside docker/airflow/dags directly

---

## Design Philosophy

- Local = development only  
- Runtime host = production only  
- Docker = orchestration boundary  
- Managed Postgres = durable state  
- Runtime host = disposable  

If something breaks badly, rebuild.

That is the system design.
