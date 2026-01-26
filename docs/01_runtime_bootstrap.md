# 01 – Runtime Bootstrap

This document describes the one-time setup steps required when provisioning a new runtime host.

These steps are operational and environment-specific. They are not part of the core system architecture.

---

## Bootstrap Flow (fresh machine)

┌────────────────────┐
│ New Runtime Host   │
│ (Ubuntu)           │
└─────────┬──────────┘
          │ SSH
          ▼
┌────────────────────────┐
│ Clone Repo             │
│ /opt/elt-canonical-data│
└─────────┬──────────────┘
          │ create .env
          ▼
┌────────────────────────┐
│ Configure Secrets      │
│ - DB connection        │
│ - Fernet key           │
│ - Web secret           │
└─────────┬──────────────┘
          │ docker compose up
          ▼
┌────────────────────────┐
│ Start Containers       │
│ - Airflow Webserver    │
│ - Airflow Scheduler    │
└─────────┬──────────────┘
          │ create admin
          ▼
┌────────────────────────┐
│ Ready                  │
│ http://<IP>:8080       │
└────────────────────────┘

---

## Runtime Access

Runtime administration is performed directly on the runtime host.

Access requires SSH:

    ssh root@<RUNTIME_IP>

All commands in this document are executed on the runtime host.

---

## Clone Repository

    mkdir -p /opt
    cd /opt
    git clone https://github.com/kevinccorcoran/elt-canonical-data.git
    cd elt-canonical-data

---

## Restore Environment Variables

Create `.env`:

    nano .env

Paste:

    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://doadmin:<PASSWORD>@<HOST>:25060/defaultdb?sslmode=require
    AIRFLOW__CORE__EXECUTOR=LocalExecutor
    AIRFLOW__CORE__FERNET_KEY=<FERNET_KEY>
    AIRFLOW__WEBSERVER__SECRET_KEY=<ANY_RANDOM_STRING>

Values come from the DigitalOcean database console.

---

## Start Runtime

    docker compose up -d

Verify:

    docker compose ps

---

## Verify Volume Mounts (Critical)

Ensure DAG and logs folders are mounted into the containers:

    docker inspect elt-canonical-data-airflow-webserver-1 | grep -A5 Mounts

Must show:

- Source: /opt/elt-canonical-data/docker/airflow/dags  
  Destination: /opt/airflow/dags  

- Source: /opt/elt-canonical-data/docker/airflow/logs  
  Destination: /opt/airflow/logs  

If these mounts do not exist, Airflow will never see DAGs or logs.

---

## Fix Filesystem Ownership (Critical)

Airflow runs inside Docker as user **UID 50000**.

Run once on fresh host:

    chown -R 50000:0 /opt/elt-canonical-data/docker/airflow/logs
    chmod -R 775 /opt/elt-canonical-data/docker/airflow/logs

If this is not done, Airflow will start and immediately crash.

---

## Admin User Creation

Create admin user:

    docker compose run --rm airflow-webserver airflow users create \
      --username admin \
      --firstname Kevin \
      --lastname Corcoran \
      --role Admin \
      --email kevin.corcoran@hotmail.com \
      --password <PASSWORD>

This only needs to be done once per database.

---

## Validation

Open:

    http://<RUNTIME_IP>:8080

Check:
- UI loads
- Scheduler is running
- DAGs visible

---

## Important Notes

- No `airflow db init` is required  
- Database is managed by DigitalOcean  
- Metadata persists even if the runtime host is destroyed  
- This document is run only on fresh machines  

---

## If This Fails

Do not debug deeply.

Destroy the runtime host.  
Re-run from `docs/05_recovery.md`.  

The runtime host is disposable.  
The database is the system.
