# Shiny Returns Analyzer Dashboard

## What is it?
An interactive web dashboard for visualizing return data.

## Where is it installed/hosted?
Locally inside the Airflow Docker container.

---

## How to Launch

1. **Start the Docker environment** (if it isn't already running):
   ```bash
   cd ~/repos/elt-canonical-data/docker/airflow
   docker compose up -d
   ```

2. **Start the Shiny App inside the container:**
   ```bash
   docker exec -u 0 -d airflow bash -c "Rscript /opt/airflow/scripts/app.R > /opt/airflow/scripts/shiny.log 2>&1"
   ```

3. **Open the Dashboard:**
   Navigate via web browser to: [http://localhost:3838](http://localhost:3838)

---

## Common Commands

**View App Logs (if the app crashes or won't load):**
```bash
docker exec airflow cat /opt/airflow/scripts/shiny.log
```

**Restart the App (if code is modified):**
```bash
# 1. Kill the running R process
docker exec airflow pkill -f "Rscript /opt/airflow/scripts/app.R"

# 2. Start it again
docker exec -u 0 -d airflow bash -c "Rscript /opt/airflow/scripts/app.R > /opt/airflow/scripts/shiny.log 2>&1"
```

**Shut everything down safely (End of day):**
```bash
cd ~/repos/elt-canonical-data/docker/airflow
docker compose down
```
