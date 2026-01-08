# Procfile used by Heroku to define how to run each process type (web UI, scheduler, worker, release script)

# Commands to run during a release phase, before starting the app
release: ./release.sh

# Starts the Airflow webserver using Gunicorn, binding to the port set by the $PORT environment variable
web: gunicorn "airflow.www.app:cached_app()" --bind 0.0.0.0:$PORT

# Starts the Airflow scheduler, which triggers DAGs based on time and other conditions
scheduler: airflow scheduler

# Starts the Airflow Celery worker to execute DAG tasks
worker: airflow worker
