# Shiny Returns Analyzer Dashboard

## What is it?
An interactive web dashboard for visualizing return data.

## Where is it installed/hosted?
Inside the Airflow Docker stack, in the **`airflow-shiny`** container, served on port **3838**.

The app is **auto-started and supervised** by the compose service (`command: bash /opt/airflow/scripts/shiny_entrypoint.sh`):
- `app.R` runs in a restart loop, so a crash or OOM-kill self-heals.
- a watchdog restarts it if port 3838 stops responding, so a hang self-heals.

You normally do **not** start `app.R` by hand — bringing the stack up is enough.

---

## How to Launch

```bash
cd ~/repos/elt-canonical-data/docker/airflow
docker compose up -d
```

Then open: [http://localhost:3838](http://localhost:3838)

Confirm the container name if a command says "No such container":
```bash
docker ps --format '{{.Names}}' | grep shiny      # expect: airflow-shiny
```

---

## Common Commands

> All commands target the **`airflow-shiny`** container. (The old runbook said
> `airflow`, which no longer exists — that mismatch is why exec commands failed.)

**View app logs:**
```bash
docker exec airflow-shiny cat /opt/airflow/scripts/shiny.log
```

**Restart the app (after editing `app.R`):**
The supervisor relaunches automatically — just kill the running R process:
```bash
docker exec airflow-shiny pkill -f "/opt/airflow/scripts/app.R"
```
Or restart the whole service:
```bash
docker compose restart airflow-shiny
```

**Shut everything down (end of day):**
```bash
cd ~/repos/elt-canonical-data/docker/airflow
docker compose down
```

---

## Troubleshooting: "could not translate host name … Temporary failure in name resolution"

This is a **DNS** failure (`EAI_AGAIN`), not a dead database. The dashboard dials
the managed-DB **public** hostname on every connect, so each connect needs a DNS
lookup; when the container's resolver blips, you get this error. It is invisible
to the self-healing launcher (the app is still up on 3838), so it does **not**
auto-restart away.

**Durable mitigations now in place:**
1. `docker-compose.yml` pins reliable resolvers (`dns: 8.8.8.8, 1.1.1.1`) with
   fail-fast options (`dns_opt: timeout:2, attempts:3, rotate`).
2. `app.R`'s `get_con()` retries transient DNS/connect failures up to 3× before
   surfacing an error, so a single blip self-recovers.

**To apply the resolver fix you must recreate the container** (a plain restart
keeps the old DNS config):
```bash
cd ~/repos/elt-canonical-data/docker/airflow
docker compose up -d --force-recreate airflow-shiny
```

**Diagnose a live occurrence:**
```bash
# What resolvers is the container using?
docker exec airflow-shiny cat /etc/resolv.conf

# Does it resolve right now? Loop to catch the intermittent failure.
docker exec airflow-shiny sh -c \
  'for i in 1 2 3 4 5; do getent hosts dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com || echo FAIL; sleep 1; done'

# Compare against the droplet host itself:
getent hosts dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com
```
If the host resolves fine but the container intermittently FAILs, the container
resolver was the problem and the pinned `dns:` above fixes it.
