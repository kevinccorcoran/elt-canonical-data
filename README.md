# ELT – Technical Setup

## Requirements
- Python 3.11.6
- PostgreSQL
- Apache Airflow 2.9.3
- dbt 1.7

## Environment
- Repo-scoped virtual environment (`.venv`)
- Repo-scoped Airflow instance
  - `AIRFLOW_HOME=$PWD/airflow`
  - DAGs: `$AIRFLOW_HOME/dags`
  - Logs: `$AIRFLOW_HOME/logs`
- No global `~/airflow`
- No global `AIRFLOW_CONFIG`
- Environment loaded via `direnv`

## Local Setup

```bash
git clone https://github.com/kevinccorcoran/ELT.git
cd ELT

python3.11 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

mkdir -p airflow
airflow db init

# Run Airflow
airflow scheduler
airflow webserver --port 8080

# Airflow Commands
# Verify setup
echo $AIRFLOW_HOME
airflow info
airflow config get-value core dags_folder

# Open Airflow config
code $AIRFLOW_HOME/airflow.cfg
# or
nano $AIRFLOW_HOME/airflow.cfg

# Reset Airflow (local only)
rm -rf $AIRFLOW_HOME
mkdir -p $AIRFLOW_HOME
airflow db init

