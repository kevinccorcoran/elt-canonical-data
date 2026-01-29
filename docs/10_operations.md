# 04 – Operations

This document describes the **authoritative operational workflow**
for synchronizing local changes to the runtime server.

Rule: All `docker compose` commands target the **current host**.  
Commands are assumed to run on the **runtime host** unless explicitly marked as **[LOCAL]**.

---

## Docker & Airflow Control (Cheat Sheet)

There are three layers of control:

1. Docker engine (machine)
2. ELT system (containers)
3. Airflow (inside containers)

---

## Docker Engine

[LOCAL] macOS  
Start: open Docker Desktop  
Stop: quit Docker Desktop  

[SERVER] Linux  
Start:
    systemctl start docker  
Stop:
    systemctl stop docker  

---

## Local Airflow (Dev / Staging)

Local Airflow is used **only for testing** against local dev or staging databases.  
It never runs production workloads.

Location:
    ~/repos/elt-canonical-data/docker/airflow

Start:
    cd ~/repos/elt-canonical-data/docker/airflow
    docker compose up -d

Stop:
    docker compose down

Status:
    docker compose ps

Logs:
    docker compose logs -f airflow

See `airflow.md` for full local Airflow setup, users, and variables.

---

## Runtime Production Overview

This document describes how to run the system and deploy changes to the **runtime host**.

Normal day-to-day flow:

[LOCAL]  
Local machine  
   │  
   │ (git push / rsync / deploy)  
   ▼  
[SERVER]  
Runtime Host  
   │  
   │ docker compose restart  
   ▼  
Airflow reloads DAGs and code  

---

## Connecting to the Runtime Host

[LOCAL]

    ssh $ELT_SERVER_USER@$ELT_SERVER_IP

All commands below are executed on the runtime host unless stated otherwise.

---

## Starting the Runtime

[SERVER]

    cd /opt/elt-canonical-data
    docker compose up -d

---

## Stopping the Runtime

[SERVER]

    docker compose down

---

## Checking Status

[SERVER]

    docker compose ps

Containers should show:
- airflow-webserver
- airflow-scheduler

---

## Viewing Logs

[SERVER]

    docker compose logs -f airflow-webserver
    docker compose logs -f airflow-scheduler

---

## Deploying Changes (Code)

GitHub is the source of truth.

Flow:

1. Edit locally
2. Commit & push to GitHub
3. Pull on runtime host
4. Restart containers

[SERVER]

    cd /opt/elt-canonical-data
    git pull
    docker compose restart

---

## Deploying Changes (DAGs)

DAGs are deployed explicitly via `rsync`.

[LOCAL]

    rsync -av --delete airflow/dags/ \
      $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/

Then restart on the runtime host:

[SERVER]

    docker compose restart

No hot-reload is assumed. Restart is the contract.

---

## Opening the Airflow UI (Private Access)

The Airflow UI is never publicly exposed.

Access is via SSH tunnel or firewall allowlist.

[LOCAL]

    ssh -L 8081:localhost:8080 $ELT_SERVER_USER@$ELT_SERVER_IP

Then open:

    http://localhost:8081

---

## Golden Rules

- Never run Airflow locally for production work
- Never edit code directly on the runtime host
- Always deploy code via Git
- Always deploy DAGs via rsync
- Always restart containers after changes

If something behaves strangely:  
**restart first, debug second**.
