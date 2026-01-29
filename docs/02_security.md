# 03 – Security Model (ELT Runtime)

## One-Minute Overview

The ELT runtime is secured primarily at the **network level**, not through
application-level authentication.

The runtime server is treated as **untrusted and disposable**.
All durable assets live outside the runtime, primarily in managed PostgreSQL and GitHub.

No admin interfaces are publicly exposed.
If the runtime is compromised, it is destroyed and rebuilt.

---

## Network Security Model

The runtime server is **not publicly accessible**.

Security is enforced using a DigitalOcean firewall.
Only explicitly whitelisted traffic is allowed.

### Firewall Rules (DigitalOcean)

Inbound traffic is restricted to:

| Port | Source            | Purpose      |
|------|-------------------|--------------|
| 22   | Kevin’s IP only   | SSH access   |
| 8080 | Kevin’s IP only   | Airflow UI  |

All other inbound traffic is **blocked by default**.

From the public internet’s perspective, the runtime server does not exist.

---

## Airflow UI Access

Airflow is an **internal control plane**, not a public application.

Two supported access models exist.

### Model A — IP Whitelist (default)

Airflow UI is reachable only from Kevin’s current IP address.

    http://<runtime-ip>:8080

If the IP changes, firewall rules must be updated.

This model relies entirely on network-level access control.

---

### Model B — SSH Tunnel (gold standard)

Port 8080 is **not exposed at all**.

Access is established via an SSH tunnel:

    ssh -N -L 8080:localhost:8080 $ELT_SERVER_USER@$ELT_SERVER_IP

Then open locally:

    http://localhost:8080

From the internet’s perspective, Airflow does not exist.

---

## Application Authentication Model

Airflow UI authentication exists only as a **secondary safeguard**.

It is not relied upon as a primary security boundary.

- Credentials are local and environment-specific
- No shared or reused passwords
- No public exposure regardless of credentials

Network access is the real control.

---

## Threat Model

Assumptions:

- The public internet is hostile
- Bots continuously scan all open ports
- Any exposed service will eventually be discovered

Therefore:

- No admin interfaces are publicly reachable
- No secrets are baked into container images
- No business or financial data is stored on the runtime server

---

## Compromise Model

If the runtime server is compromised:

- The attacker gains no durable data
- No secrets are recoverable from Git
- The server is destroyed and rebuilt
- Credentials are rotated

Recovery is a **rebuild**, not an investigation.

---

## Database Security

- PostgreSQL is provided by DigitalOcean Managed Database
- The database is not containerized
- Only the runtime server is allowed to connect
- Database credentials are stored only on the runtime server

The database is the only critical asset.

---

## Configuration & Secrets

Runtime configuration is provided via a `.env` file on the runtime server.

Required variables include:

- AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
- AIRFLOW__CORE__EXECUTOR
- AIRFLOW__CORE__FERNET_KEY
- AIRFLOW__WEBSERVER__SECRET_KEY

Secrets are:

- Stored only on the runtime server
- Never committed to Git
- Recreated manually when rebuilding infrastructure

---

## What Not To Do

- Do not expose port 8080 to the public internet
- Do not store secrets in Git
- Do not rely on application auth as a security boundary
- Do not debug compromised servers — rebuild instead

---

## Design Principle

Security is enforced at the **network layer**.

The runtime server is disposable.
The database is the only asset that matters.

If something breaks badly, destroy and rebuild.
