# 06 – Security Hardening (Runtime)

This document describes how the ELT runtime is secured and how access to the system is restricted.

## Goal

- Airflow UI is NOT publicly accessible  
- Access is restricted to a single trusted client  
- Runtime infrastructure is disposable, data is protected  

---

## Threat Model

Assumptions:

- The public internet is hostile  
- Automated scanners will probe open ports  
- Any service exposed on port 8080 will be discovered  

Therefore:

- The Airflow UI must never be exposed to the public internet  

---

## 1. Firewall (DigitalOcean)

A DigitalOcean Firewall is attached to the runtime host.

Name:  
airflow-private

Inbound rules:

| Port | Source             | Purpose    |
|------|--------------------|------------|
| 22   | Trusted client IP  | SSH access |
| 8080 | Trusted client IP  | Airflow UI |

Everything else: BLOCKED

Outbound rules:

- All allowed (default)

---

## 2. IP-Based Access Control

Current public IP:

    curl ifconfig.me

Example:

    62.166.182.227

This IP is explicitly whitelisted in the firewall rules.

If the IP changes:

- Firewall rules must be updated  
- Otherwise access will be blocked  

---

## 3. No Public Exposure

Airflow may log warnings such as:

Recent requests have been made to /robots.txt

This indicates automated scanning attempts.

These requests are blocked at the network layer and can be safely ignored once the firewall is active.

Application-level security is not relied upon.  
Protection is enforced at the infrastructure level.

---

## 4. Docker Network Exposure

Airflow runs inside Docker.

Only exposed port:

8080

Verified via:

    docker compose ps

Expected output:

    0.0.0.0:8080->8080/tcp

The port is open on the host, but reachable only from whitelisted IPs.

---

## 5. SSH Security

Each environment uses unique SSH keys.

Rules:

- No shared keys across machines  
- Private keys are never copied  
- Compromised keys are rotated immediately  

Verification:

    ssh -T git@github.com

---

## 6. Secrets Management

Secrets are stored only in:

    /opt/elt-canonical-data/.env

Never committed to version control.

Includes:

- Database credentials  
- Fernet key  
- Webserver secret  

In case of leakage:

- Secrets are rotated  
- Runtime host is destroyed  
- System is rebuilt  

---

## 7. Recovery Model

If exposure is suspected:

- Destroy runtime host  
- Rebuild from recovery documentation  
- Reapply firewall rules  

Recovery time is under 10 minutes.

---

## Security Model Summary

- Runtime is disposable  
- Database is the persistent system of record  
- Firewall is the primary security control  
- SSH keys define identity  

Long-lived infrastructure is not trusted.  
System integrity relies on fast rebuild and strict network isolation.
