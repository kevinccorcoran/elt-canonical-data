# Docker Setup

## What's running

### `airflow` container — port 8080 (DAGs), 3838 + 3839 (Shiny)
**What it does:** runs your scheduled data pipelines (DAGs) and serves two Shiny dashboards on the same container.
**Why one container:** the dashboards read the same data the DAGs produce. Putting Python (Airflow) and R (Shiny) in one image avoids volume juggling and inter-container networking just to share files.
**Built from:** `elt-canonical-data/Dockerfile` — apache/airflow base + R + Shiny libraries + Python `requirements.txt`.

### `airflow-postgres` container — port 5432
**What it does:** a Postgres database that Airflow uses **only for itself** — DAG run history, task states, scheduling metadata.
**Why separate from your Mac Postgres:** Airflow has churn (millions of metadata rows over time). Keeping its DB inside docker means you can wipe it without touching your real trading data, and your real DB never ages from Airflow internals.

### `dbt-docs` container — port 8889
**What it does:** serves the dbt lineage graph for the **inference-models** repo only.
**Why a container at all:** dbt docs needs Python + dbt installed. Containerizing means you don't pollute your laptop's Python with project-specific dbt versions.
**Why slim and pip-installed at startup:** image is `python:3.11-slim` and runs `pip install dbt-postgres` on boot. That keeps the image tiny and always uses the latest dbt — at the cost of ~30 sec startup.

### `dbt-docs-canonical` container — port 8890
**What it does:** same as above but for the **canonical** repo.
**Why two separate containers:** each repo has its own dbt project. Mixing them produces a lineage graph that shows models from both, which is misleading. Separation keeps each graph honest.
**Special trick:** this one runs a Python script at startup that **prunes the dbt manifest** to remove anything not in the canonical project. Reason: canonical historically imported the inference repo as a dbt package, leaving leftover model files that still get picked up by `dbt docs generate`.
**Why `target/` is on an anonymous volume:** the source repo is bind-mounted into the container, but `target/` is intentionally shadowed by a private docker volume. Without that, anything on the host (or another container) that runs `dbt docs generate` would overwrite the pruned manifest mid-life and silently un-prune the lineage graph.

### `moltbot-postgres-1` container
**Unrelated project.** Ignore.

---

## How env vars get into the airflow container

There are two ways docker-compose feeds environment variables, and **the order matters**:

1. **`env_file: .env`** — every line in the file becomes an env var inside the container. This is where secrets and DB credentials live.
2. **`environment:` block in compose** — these *override* env_file values. Useful for hardcoded settings (e.g. `AIRFLOW__CORE__PARALLELISM: 4`) or for renaming a var (`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: ${AIRFLOW_DATABASE_URL}`).

**Rule:** never list the same variable name in both. The `environment:` line wins, even if it's empty — that's how `MASSIVE_API_KEY` got silently blanked.

**Active env file:** `docker/airflow/.env`. Templates exist alongside (`.env.dev`, `.env.staging`, `.env.prod`) — to switch envs, copy a template over `.env`.

---

## How the airflow container reaches your data

- **Inside docker:** services find each other by container name. Airflow → `airflow-postgres` (its own metadata DB).
- **Reaching the host (your Mac):** docker provides a magic hostname `host.docker.internal` that resolves to your laptop. Airflow uses this to reach your **Mac Postgres** (where the real trading data lives).

So inside the container, `DB_HOST=host.docker.internal DB_PORT=5432` means *"connect to whatever Postgres is on my Mac at 5432."*

---

## Why source code is mounted, not baked into the image

The compose file mounts `src/`, `dbt/`, `airflow/dags/` as volumes. That means:
- You edit code on your Mac → it's instantly visible inside the container, no rebuild.
- Only system-level stuff (R packages, system libs, Python deps) lives in the image. That changes rarely, so rebuilds are rare.

Trade-off: the container is tied to your Mac filesystem. Not portable, but convenient for solo dev.

---

## File map

| What | Where |
|---|---|
| Airflow image build | `elt-canonical-data/Dockerfile` |
| Airflow compose | `elt-canonical-data/docker/airflow/docker-compose.yml` |
| Airflow active env | `elt-canonical-data/docker/airflow/.env` |
| Env templates | `.env.dev`, `.env.staging`, `.env.prod` (same dir) |
| inference dbt-docs compose | `elt-inference-models/docker-compose.docs.yml` |
| canonical dbt-docs compose | `elt-canonical-data/docker/dbt-docs-canonical/docker-compose.yml` |

---

## Known issues

1. **Edit `.env`, not the compose file.** Secrets live in `.env`. Compose only references variable names.
2. **Shiny dashboard (3838 or 3839) shows nothing** → R package missing. `docker exec airflow tail /opt/airflow/scripts/shiny.log` for `there is no package called 'X'`. Add to [Dockerfile:8](../Dockerfile#L8) and rebuild.

---

## Useful commands

```bash
# See what's up
docker ps

# Tail airflow logs
docker logs airflow --tail 50 -f

# Tail shiny logs
docker exec airflow tail -f /opt/airflow/scripts/shiny.log

# Shell inside airflow
docker exec -it airflow bash

# Restart airflow (keeps image)
cd elt-canonical-data/docker/airflow && docker compose up -d --force-recreate airflow

# Rebuild airflow image (slow — only when Dockerfile changes)
cd elt-canonical-data/docker/airflow && docker compose build airflow
```
