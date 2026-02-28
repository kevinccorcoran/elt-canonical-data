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

| Environment | Database | Tickers | Use case |
| :--- | :--- | :--- | :--- |
| **dev** | Local `dev` DB | `TICKERS_SUB` (3) | Fast dev/test cycles |
| **staging** | Local `staging` DB | `TICKERS_FULL` (1738) | Full backfill locally |
| **prod** | Remote DigitalOcean DB | `TICKERS_FULL` (1738) | Production (server only) |

**File to edit:** `docker/airflow/.env`

**Step 1 — Disable direnv** (it overrides `.env` values):

```bash
cd docker/airflow
direnv deny .
unset DB_HOST DB_PORT DB_USER DB_PASSWORD DB_DATABASE DATABASE_URL ENV
```

**Step 2 — Edit `.env`**, change these two lines:

To switch to **dev** (3 tickers, fast):
```
DB_DATABASE=dev
DATABASE_URL=postgresql://postgres:@host.docker.internal:5432/dev
```

To switch to **staging** (1738 tickers, full backfill):
```
DB_DATABASE=staging
DATABASE_URL=postgresql://postgres:@host.docker.internal:5432/staging
```

**Step 3 — Recreate the container:**

```bash
docker compose up -d airflow
```

**Step 4 — Verify:**

```bash
docker exec airflow bash -c 'echo DB_HOST=$DB_HOST DB_DATABASE=$DB_DATABASE ENV=$ENV'
# Should show: DB_HOST=host.docker.internal DB_DATABASE=dev ENV=dev
```

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
