# 03 – Runtime Provisioning

This document describes how to prepare a new Linux machine for use as a runtime host.

It converts an empty server into a machine capable of running the ELT system.

Your laptop ──(SSH)──▶ Fresh Runtime Host  
                          │  
                          ▼  
                     Install Docker  
                          │  
                          ▼  
                     Runtime host ready  

---

## Host Requirements

- Ubuntu 22.04+ (or equivalent)
- Public IP address
- SSH access

The runtime host is a generic Linux server (VM).  
Cloud provider is irrelevant to the system design.

---

## Initial Access

Connect from your laptop:

    ssh root@<RUNTIME_IP>

All commands in this document are executed on the runtime host.

---

## Docker Installation

Install Docker:

    apt update
    apt install -y ca-certificates curl gnupg
    curl -fsSL https://get.docker.com | sh

---

## Enable Docker

Enable Docker on boot:

    systemctl enable docker
    systemctl start docker

Verify daemon is running:

    systemctl status docker

---

## Docker Compose

Docker Compose is included with modern Docker.

Verify:

    docker version
    docker compose version

---

## Networking

The runtime host must expose:

- 22 – SSH  
- 8080 – Airflow UI  

All other ports should be blocked.

Access should be restricted by firewall to the laptop IP only.

Mental model:

- Public server  
- Private service  
- Explicit allowlist  

The runtime host is not a public web application.

---

## Result

At this point the machine is:

- Reachable via SSH  
- Running Docker  
- Protected by firewall  
- Ready to run the ELT runtime stack  

Next step:  
Proceed to `docs/04_runtime_bootstrap.md`
