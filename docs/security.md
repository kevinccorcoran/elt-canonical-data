# Security Design

**Purpose**: To explain the threat model, network security, and access controls for the platform.

---

## 1. Network Security Model

The runtime server is **not publicly accessible**.

### Firewall Rules (DigitalOcean)
*   **Allow**: Port `22` (SSH) - Whitelisted IPs only.
*   **Allow**: Port `8080` (Airflow UI) - Whitelisted IPs only (or via SSH tunnel).
*   **Deny**: All other inbound traffic.

**Threat Model**: The public internet is hostile. Bots scan all ports. Network restrictions are the primary defense.

---

## 2. Authentication (SSH Keys)

We use **SSH keys** for machine identity. Passwords are insecure and are not used.

### Key Management
*   **Private Key**: `~/.ssh/id_ed25519` (Must never leave the machine).
*   **Public Key**: `~/.ssh/id_ed25519.pub` (Registered with GitHub/DigitalOcean).

### Access Flow
1.  **Laptop** connects to **Server**.
2.  **Server** challenges **Laptop** with cryptographic proof.
3.  **Laptop** signs proof with private key.
4.  **Access Granted**.

No passwords are ever transmitted or stored.

---

## 3. Secret Management

Secrets (API keys, DB passwords) are never committed to Git.

### Storage
*   Provided via `.env` file on the runtime host only.
*   Retrieved from DigitalOcean console during deployment.

### Critical Secrets
*   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
*   `AIRFLOW__CORE__FERNET_KEY`
*   `AIRFLOW__WEBSERVER__SECRET_KEY`

---

## 4. Threat Response

### If Runtime Host is Compromised:
1.  **Destroy** the server immediately.
2.  **Rotate** affected credentials (DB password, API keys).
3.  **Rebuild** a fresh server from `operations_manual.md`.

**Recovery is a rebuild, not an investigation.**
