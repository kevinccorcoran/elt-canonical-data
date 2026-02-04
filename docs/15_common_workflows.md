# Common Operational Workflows

Detailed procedures for frequent tasks in the ELT system.

---

## 1. Deploying Local Changes to Server (Production)

Use this when you have modified code on your Mac and want to see it live.

**[LOCAL] - Push local work**
```bash
git add .
git commit -m "Describe your changes"
git push origin <your-branch-name>
```

**[SERVER] - Pull and Refresh**
```bash
ssh <your-server-user>@<your-server-ip>
cd /opt/elt-canonical-data

# 1. Update the code
git pull origin <your-branch-name>

# 2. Re-load environment variables (Source of Truth)
source .envrc.prod

# 3. Restart Docker containers with new code/env
cd docker/airflow
docker compose up -d --force-recreate
```

---

## 2. Initializing a New Production Database

Use this when you create a new PostgreSQL instance or database (e.g., switching from `defaultdb` to `prod`).

1. Open **DBeaver** and connect to your database.
2. Run the initialization script in a SQL Editor:

```sql
-- 1. Create the schema folder
CREATE SCHEMA IF NOT EXISTS raw;

-- 2. Create the target table
CREATE TABLE raw.api_data_ingestion_massive (
    ticker_date_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    adj_close DOUBLE PRECISION,
    dividends DOUBLE PRECISION,
    stock_splits DOUBLE PRECISION,
    capital_gains DOUBLE PRECISION,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    date_type TEXT
);

-- 3. Add constraint for UPSERT (Avoid duplicates)
ALTER TABLE raw.api_data_ingestion_massive
ADD CONSTRAINT unique_ticker_date_id UNIQUE (ticker_date_id);
```

---

## 3. Managing Environment Variable "Truth"

The system follows a strict hierarchy for configuration:
1. **OS Environment (`.envrc.prod`)** - **ABSOLUTE TRUTH**. Overrides everything.
2. **Airflow UI Variables** - **FALLBACK**. Used only if OS variable is missing.

### How to change the Database Connection
1. Edit `/opt/elt-canonical-data/.envrc.prod` on the server.
2. Update `DB_DATABASE` or other settings.
3. Run the **[SERVER] - Pull and Refresh** steps above.

---

## 4. Running a Quick Test in Production

If you want to test the full ETL flow but only for a small set of tickers:

1. Open **Airflow UI** -> **Admin** -> **Variables**.
2. TEMPORARILY set `DB_DATABASE` to `dev`.
3. Save and Run the DAG.
4. **IMPORTANT**: Delete the variable from the UI when finished. The system will fall back to using your `.envrc.prod` (which is set to `prod`).

---

## 5. Monitoring Logs

**Live Airflow Logs:**
```bash
cd /opt/elt-canonical-data/docker/airflow
docker compose logs -f airflow
```

**Checking Ingestion Progress:**
Run this in DBeaver:
```sql
SELECT count(*), source, date_type FROM raw.api_data_ingestion_massive GROUP BY 2, 3;
```
