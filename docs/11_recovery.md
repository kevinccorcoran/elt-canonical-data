# 05 – Recovery / Rebuild (Full Runbook)

This document describes how to fully recover the system if the runtime host is lost, broken, or corrupted.

Goal: redeploy everything in **< 10 minutes**.

GitHub Repo        Managed Postgres  
     │                   │  
     │                   │  
     └──────┬────────────┘  
            │  
            ▼  
     Destroy Runtime Host  
            │  
            ▼  
     Create New Runtime Host  
            │  
            ▼  
     Install Docker  
            │  
            ▼  
     Clone Repo + .env  
            │  
            ▼  
     docker compose up -d  
            │  
            ▼  
       System Restored  

---

## When to Use This

Use this if:

- Runtime host is unreachable  
- Docker is broken beyond fixing  
- Disk is full / corrupted  
- You want a clean slate  

Do **not** debug for hours. Rebuild is cheaper and faster.

---

## Step 1 – Destroy Runtime Host

In cloud provider UI:

- Go to servers  
- Select `elt-runtime-prod`  
- Destroy → Confirm  

**Do NOT delete the managed Postgres cluster.**

---

## Step 2 – Create New Runtime Host

Create new server with:

- Ubuntu 24.04 LTS  
- Size: 4GB / 2 vCPU  
- Region: AMS3 (or equivalent)  
- Add your SSH key  
- Name: `elt-runtime-prod`  

Wait until it boots.

---

## Step 3 – SSH Into New Machine

    ssh root@<NEW_IP>

---

## Step 4 – Install Docker

    apt update
    apt install -y ca-certificates curl gnupg
    curl -fsSL https://get.docker.com | sh

    systemctl enable docker
    systemctl start docker

Verify:

    docker --version
    docker compose version

---

## Step 5 – Clone Repo

    mkdir -p /opt
    cd /opt
    git clone https://github.com/kevinccorcoran/elt-canonical-data.git
    cd elt-canonical-data

---

## Step 6 – Restore .env

Create file:

    nano .env

Paste:

    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://doadmin:<PASSWORD>@<HOST>:25060/defaultdb?sslmode=require
    AIRFLOW__CORE__EXECUTOR=LocalExecutor
    AIRFLOW__WEBSERVER__SECRET_KEY=<ANY_RANDOM_STRING>
    AIRFLOW__CORE__FERNET_KEY=<FERNET_KEY>

Values come from the managed database console.

---

## Step 7 – Start System

    docker compose up -d
    docker compose ps

---

## Step 8 – Open UI

In browser:

    http://<NEW_IP>:8080

Login with admin user.

---

## Step 9 – Sanity Checks

Confirm:

- Web UI loads  
- Scheduler is running  
- DAGs visible  
- No red errors in logs  

---

## If This Fails

Do not debug deeply.

- Destroy runtime host again  
- Re-run this document exactly  

If it still fails:
the problem is in **GitHub or the database**, not infrastructure.

---

## Golden Rule

Never treat the runtime host as precious.

The only real assets are:

- GitHub repository  
- Managed PostgreSQL  

Everything else is disposable.
