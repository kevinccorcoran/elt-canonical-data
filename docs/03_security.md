# 03 – Security Model (ELT Runtime)

## One-Minute Overview

The Airflow runtime is protected by a network firewall that allows access only from my whitelisted IP address.

Security for the ELT runtime is enforced primarily at the **network level**, not via
application-level authentication.


The runtime server is treated as **untrusted and disposable**.
All durable assets live in managed PostgreSQL and GitHub.

No admin interfaces are publicly exposed.

---

## Network Security Model

The runtime server is **not publicly accessible**.

Security is enforced using a DigitalOcean firewall.

### Firewall Rules (DigitalOcean)

Inbound traffic is restricted to:

| Port | Source            | Purpose      |
|------|-------------------|--------------|
| 22   | Kevin’s IP only   | SSH access   |
| 8080 | Kevin’s IP only   | Airflow UI   |

All other inbound traffic is **blocked by default**.

No services are exposed to the public internet.

---

## Airflow UI Access

Airflow is never meant to be publicly accessible.

Two supported access models:

### Model A — IP Whitelist (default)

Airflow UI is reachable only from Kevin’s current IP.

    http://<runtime-ip>:8080

If the IP changes, firewall rules must be updated.

---

### Model B — SSH Tunnel (gold standard)

Port 8080 is not exposed at all.

Access is done via:

    ssh -N -L 8080:localhost:8080 $ELT_SERVER_USER@$ELT_SERVER_IP

Then open:

    http://localhost:8080

From the internet’s perspective, Airflow does not exist.

---

## Threat Model

Assumptions:

- The public internet is hostile
- Bots scan all open ports
- Anything exposed will be discovered

Therefore:

- No admin interfaces are public
- No secrets are stored in containers
- No business data is stored on the runtime server

---

## Compromise Model

If the runtime server is compromised:

- The attacker gains no durable data
- No secrets are recoverable from Git
- The server is destroyed and rebuilt
- Credentials are rotated

The managed PostgreSQL database is the only critical asset.

---

## Database Security

- PostgreSQL is provided by DigitalOcean Managed Database
- The database is not containerized
- Only the runtime server is allowed to connect
- Credentials are stored only in `.env` on the runtime server

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
- Do not run Airflow locally
- Do not debug compromised servers — rebuild instead

---

## Design Principle

Security is enforced at the **network layer**.

The runtime server is disposable.
The database is the only asset that matters.

If something breaks badly, destroy and rebuild.
