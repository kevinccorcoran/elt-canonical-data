# Docker Setup

## What's running

The old single `airflow` container was split into one service per concern. All of
it is defined in `docker/airflow/docker-compose.yml`, built from the repo-root
`Dockerfile` (apache/airflow base + R + Shiny libs + Python `requirements.txt`).

### Port registry (single source of truth)

Services defined in `docker/airflow/docker-compose.yml`:

| Container | Published | Bind | Purpose |
|---|---|---|---|
| `airflow-init` | — | — | one-shot `airflow db upgrade`, then exits |
| `airflow-scheduler` | — | — | runs DAGs (no port) |
| `airflow-shiny` | 3838 -> 3838 | 127.0.0.1 | the Shiny dashboard (`scripts/app.R`) |
| `airflow-postgres` | 5435 -> 5432 | 127.0.0.1 | Airflow **metadata** DB (dbname `airflow`) |
| `dbt-docs` (inference repo) | 8889 -> 8080 | 127.0.0.1 | dbt lineage for inference-models |
| `dbt-docs-canonical` | 8890 -> 8080 | 127.0.0.1 | dbt lineage for canonical |

The **Airflow UI (8080)** is served by a separate long-running webserver container,
not a service in this compose (the `airflow-webserver` service was removed — see the
comment in the compose — because a combined container already owns host 8080).

Notes:
- Everything in this compose is loopback-only; reach the dashboard remotely via SSH
  tunnel (it is unauthenticated).
- **The dashboard port is env-overridable:** `SHINY_PORT` (default 3838) is honored
  by both `scripts/app.R` (`runApp`) and `scripts/shiny_entrypoint.sh`.
- `app_violin.R` / port **3839 is retired** — it is no longer launched or published
  (it only survives in `docker-compose.yml.bak`).
- `airflow-postgres` is the metadata DB only (DAG run history, task states). It is
  deliberately NOT any trading database and must not be reachable off-host.

### The Shiny launcher (`airflow-shiny`)
`scripts/shiny_entrypoint.sh` runs `app.R` in a restart loop plus a watchdog that
restarts it if the port stops answering — a crash, OOM-kill, or hang self-heals. A
preflight warns loudly if a stale `httpuv/later` ABI (the known segfault stack) ever
ships in the image.

### dbt-docs containers
`python:3.11-slim` that `pip install dbt-postgres` on boot and serve the pruned
lineage as static HTML. `dbt-docs-canonical` prunes the manifest to canonical-only
models and shadows `target/` with an anonymous volume so a host `dbt docs generate`
can't silently un-prune it.

---

## How env vars get into the containers

Two mechanisms, and **order matters**:

1. **`env_file: .env`** — every line becomes an env var in the container. Secrets and
   DB credentials live here.
2. **`environment:` block in compose** — these *override* env_file values (e.g.
   `AIRFLOW__CORE__PARALLELISM`, or renaming via `${AIRFLOW_DATABASE_URL}`).

**Rule:** never list the same variable in both — the `environment:` line wins even if
empty (that's how `MASSIVE_API_KEY` once got silently blanked).

**Active env file:** `docker/airflow/.env` (gitignored). A tracked, secret-free
template `docker/airflow/.env.example` documents every required key —
`cp .env.example .env` and fill in. The `.env.dev/.staging/.prod` files are
switchable presets you copy over `.env`. The dbt-docs container has its own
`.env.docs` (template: `.env.example` in the same dir).

---

## How the containers reach your data

- **Inside docker:** services find each other by container name — Airflow reaches
  `airflow-postgres` (its metadata DB) directly.
- **Reaching the host Mac:** the hostname `host.docker.internal` resolves to your
  laptop; the DAGs and the dashboard's Staging/Dev presets use it to reach the
  **Mac-native Postgres** (real trading data). `DB_HOST=host.docker.internal
  DB_PORT=5432` means "the Postgres on my Mac at 5432."
- **Explicit mapping:** `host.docker.internal:host-gateway` is declared on the
  `x-airflow-common` anchor (so every airflow service inherits it) and on both
  dbt-docs composes. Docker Desktop resolves this name implicitly, but engines like
  **OrbStack do not** — without the explicit map, connections fail with "could not
  translate host name."

---

## Why source is mounted, not baked

Compose mounts `src/`, `dbt/`, `airflow/dags/`, `scripts/` (and the sibling
`elt-inference-models` / `qualstream` repos) as volumes — edit on the Mac, it's live
in the container, no rebuild. Only system-level deps (R packages, libs, Python) live
in the image, so rebuilds are rare. `app.R` in particular is read at container start,
so a change needs `docker restart airflow-shiny`, not a rebuild.

---

## File map

| What | Where |
|---|---|
| Airflow image build | `Dockerfile` |
| Airflow compose | `docker/airflow/docker-compose.yml` |
| Airflow active env / template | `docker/airflow/.env` / `.env.example` |
| Env presets | `.env.dev`, `.env.staging`, `.env.prod` (same dir) |
| inference dbt-docs compose | `elt-inference-models/docker-compose.docs.yml` |
| canonical dbt-docs compose | `docker/dbt-docs-canonical/docker-compose.yml` |
| Shiny launcher | `scripts/shiny_entrypoint.sh` |

---

## Known issues

1. **Edit `.env`, not the compose file** for secrets. Compose only references names.
2. **Dashboard shows nothing / an R package is missing** →
   `docker exec airflow-shiny tail /opt/airflow/scripts/shiny.log` and look for
   `there is no package called 'X'`; add it to the `Dockerfile` CRAN layer and rebuild.
3. **"could not translate host name host.docker.internal"** → the service is missing
   the `extra_hosts` map (see above); it lives on the anchor + both dbt-docs composes.

---

## Useful commands

```bash
docker ps                                              # what's up

# Tail the dashboard log
docker exec airflow-shiny tail -f /opt/airflow/scripts/shiny.log

# Reload the dashboard after an app.R edit (no rebuild)
docker restart airflow-shiny

# Recreate a service after a compose change (picks up extra_hosts etc.)
cd docker/airflow && docker compose up -d --force-recreate airflow-webserver airflow-scheduler

# Rebuild the image (slow — only when Dockerfile changes)
cd docker/airflow && docker compose build && docker compose up -d --no-deps --force-recreate airflow-shiny
```
