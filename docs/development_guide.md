# Development Guide

This guide covers local environment setup, testing, and contribution workflows.

---

## 1. Prerequisites (macOS)

*   **Python 3.11** (Core scripts & dbt)
*   **Docker Desktop** (Airflow runtime)
*   **direnv** (Environment variable management)
*   **dbt** (Data transformation)

---

## 2. Local Environment Setup

### A. Python (dbt & Scripts)
Create a virtual environment for tools that run on your host machine:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
*Note: Do not install Airflow here. Airflow runs in Docker.*

### B. Environment Variables (direnv)
We use `direnv` to switch between `dev` and `staging` contexts automatically.

| Context | Database | Command |
| :--- | :--- | :--- |
| **Dev** | Local Dev DB | `ln -sf .envrc.dev .envrc && direnv allow` |
| **Staging** | Local Staging DB | `ln -sf .envrc.staging .envrc && direnv allow` |

**Verify:** Run `echo $ENV` to check current context.

### C. Airflow (Docker)
Airflow runs in containers to mimic production.

```bash
cd docker/airflow
docker compose up -d    # Start
docker compose down     # Stop
```
**UI:** [http://localhost:8080](http://localhost:8080)

### D. Switching Airflow Environment (dev / staging / prod)

The Airflow container's environment is controlled by **`docker/airflow/.env`**.
The key variable is `DB_DATABASE`, which determines:

| `DB_DATABASE` | `ENV` | Database | Tickers |
| :--- | :--- | :--- | :--- |
| `dev` | dev | Local dev DB | `TICKERS_SUB` (3 tickers) |
| `staging` | staging | Local staging DB | `TICKERS_FULL` (1738 tickers) |
| `prod` | prod | Production DB | `TICKERS_FULL` (1738 tickers) |

**To switch environments:**

```bash
cd docker/airflow

# 1. Edit .env → change DB_DATABASE to dev, staging, or prod
#    Also update DATABASE_URL to match the target database

# 2. Recreate the container to pick up the new values
docker compose up -d airflow
```

> ⚠️ **Important:** Shell environment variables (e.g. from `direnv`) take priority
> over `.env` file values. If switching doesn't work, run `unset DB_DATABASE`
> and `unset DATABASE_URL` in your shell before `docker compose up`.

---

## 3. Workflow

### Feature Development
1.  **Branch**: `git checkout -b feature/my-feature`
2.  **Code**: Edit scripts in `src/` or models in `dbt/`.
3.  **Test**:
    *   `dbt run`: Build models against local DB.
    *   `pytest`: Run Python unit tests.
4.  **Commit**: `git commit -m "feat: description"`

### Deployment
1.  **Merge**: Pull request or merge to `main`.
2.  **Sync DAGs**: `rsync` DAG files to server (if changed).
3.  **Deploy Code**: `git pull` on server and `docker compose restart`.
