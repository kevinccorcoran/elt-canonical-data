# Operational Cheat Sheet

Common commands for the AlphaStream pipeline.

---

## 1. Feature Workflow (End-to-End)

**Step 1: Local Development**
```bash
# Branch
git checkout -b feature/my-feature

# Commit
git add .
git commit -m "feat: description"
git push origin feature/my-feature
```

**Step 2: Merge**
```bash
git checkout main
git pull origin main
git merge feature/my-feature
git push origin main
```

**Step 3: Deploy to Production**
```bash
# 1. Sync DAGs (if changed)
rsync -av --delete airflow/dags/ $ELT_SERVER_USER@$ELT_SERVER_IP:/opt/elt-canonical-data/docker/airflow/dags/

# 2. Update Code
ssh $ELT_SERVER_USER@$ELT_SERVER_IP
cd /opt/elt-canonical-data
git pull origin main
docker compose restart
```

---

## 2. Local Environment

**Switching (direnv)**
```bash
ln -sf .envrc.dev .envrc && direnv allow       # Dev
ln -sf .envrc.staging .envrc && direnv allow   # Staging
```

**Airflow (Docker)**
```bash
docker compose up -d    # Start
docker compose down     # Stop
docker compose ps       # Status
```

**Testing**
```bash
dbt run                 # Run models
dbt test                # Run data tests
pytest                  # Run python tests
```

---

## 3. Server Operations

**Management**
```bash
docker compose ps       # Status
docker compose restart  # Restart all
docker compose logs -f  # Tail logs
```

**Debugging**
```bash
# Access UI via Tunnel
ssh -L 8081:localhost:8080 $ELT_SERVER_USER@$ELT_SERVER_IP
# Open http://localhost:8081
```

---

## 4. Data Sync

**Push Local to Staging**
```bash
./tools/refresh_staging_from_local.sh
```

**Upload Specific Data to Prod**
```bash
./tools/upload_api_data_ingestion_massive_to_prod.sh
./tools/upload_ticker_index_summary_to_prod.sh
```
