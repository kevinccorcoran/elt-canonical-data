# Operations Manual

This document covers provisioning, maintenance, and recovery of the production runtime.

---

## 1. Provisioning

**Host Specs** (DigitalOcean):
*   **OS**: Ubuntu 24.04 LTS
*   **Region**: AMS3
*   **Size**: 4GB / 2 vCPU
*   **Firewall**: Allow SSH (22) from whitelist only.

**Initialization Script:**
Run these commands on a fresh server to install the stack.

```bash
# 1. Install Docker
apt update && apt install -y ca-certificates curl gnupg
curl -fsSL https://get.docker.com | sh
systemctl enable docker && systemctl start docker

# 2. Clone Repo
mkdir -p /opt
cd /opt
git clone https://github.com/kevinccorcoran/elt-canonical-data.git
cd elt-canonical-data

# 3. Configure Secrets (Create .env)
# Paste AIRFLOW__DATABASE__SQL_ALCHEMY_CONN and other keys here
nano .env

# 4. Start
docker compose up -d
```

---

## 2. Deployment

**Code Update**
```bash
ssh $ELT_SERVER_USER@$ELT_SERVER_IP
cd /opt/elt-canonical-data
git pull origin main
docker compose restart
```

**DAG Sync (from Mac)**
```bash
rsync -av --delete airflow/dags/ $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/
```

---

## 3. Maintenance

**Status Check**
```bash
docker compose ps
docker compose logs -f airflow-scheduler
```

**Access UI**
SSH Tunnel required (Port 8080 is blocked publicly).
`ssh -L 8081:localhost:8080 $ELT_SERVER_USER@$ELT_SERVER_IP`
*   Open: [http://localhost:8081](http://localhost:8081)

---

## 4. Disaster Recovery

If the runtime server is compromised or broken:

1.  **Destroy** the Droplet (Server).
    *   *Note: Database is managed externally and is safe.*
2.  **Create** a new Droplet (see Section 1).
3.  **Run** Initialization Script.
4.  **Restore** `.env` file from secure backup.

**Recovery Time Objective**: < 10 minutes.
