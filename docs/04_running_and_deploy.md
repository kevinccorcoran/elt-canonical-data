# 04 – Running & Deployment

This document describes how to run the system and deploy changes to the runtime host.

This is the normal day-to-day operational workflow.

Local machine  
   │  
   │ (git push / rsync / deploy)  
   ▼  
Runtime Host  
   │  
   │ docker compose restart  
   ▼  
Airflow reloads DAGs and code  

---

## Starting the Runtime

On the runtime host:

    cd /opt/elt-canonical-data
    docker compose up -d

---

## Stopping the Runtime

    docker compose down

---

## Checking Status

    docker compose ps

Containers should show:
- airflow-webserver  
- airflow-scheduler  

---

## Viewing Logs

    docker compose logs -f airflow-webserver
    docker compose logs -f airflow-scheduler

---

## Deploying Changes (Code)

Deployment flow:

1. Make changes locally  
2. Commit & push to GitHub  
3. Pull changes on the runtime host  
4. Restart containers  

On runtime host:

    ssh $ELT_SERVER_USER@$ELT_SERVER_IP
    cd /opt/elt-canonical-data
    git pull
    docker compose restart

---

## Deploying Changes (DAGs)

DAGs are deployed explicitly via rsync.

From local machine:

    rsync -av --delete airflow/dags/ $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/

Then restart:

    docker compose restart

No hot-reload is assumed. Restart is the contract.

---

## Opening the UI (Private Access)

The Airflow UI is never exposed publicly.

Access is done via direct firewall allowlist or SSH tunnel.

SSH tunnel option:

    ssh -L 8081:localhost:8080 root@<RUNTIME_IP>

Then open in browser:

    http://localhost:8081

---

## Golden Rules

- Never run Airflow locally for production work  
- Never edit code directly on the runtime host  
- Always deploy code via Git  
- Always deploy DAGs via rsync  
- Always restart containers after changes  

If something behaves strangely:  
restart first, debug second.
