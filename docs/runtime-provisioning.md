# Runtime Provisioning

This document describes how to prepare a new DigitalOcean droplet for use as a runtime host.

---

## Host Requirements

- Ubuntu 22.04+ (or equivalent)
- Public IP address
- SSH access

---

## Docker Installation

Install Docker:

    apt update
    apt install -y ca-certificates curl gnupg
    curl -fsSL https://get.docker.com | sh

Install Docker Compose:

    apt install -y docker-compose-plugin

Verify:

    docker version
    docker compose version
