# datapipeline/config/env.py
import os
import logging

# ──────────────────────────────────────────────
# Unified accessor: Airflow Variables → env vars
# ──────────────────────────────────────────────
try:
    from airflow.models import Variable

    def get_var(key: str, default=None):
        val = os.getenv(key)
        if val is not None:
            logging.info("Resolved var '%s' from OS ENV", key)
            return val
        try:
            val = Variable.get(key)
            logging.info("Resolved var '%s' from Airflow Variable", key)
            return val
        except Exception:
            return default

except ModuleNotFoundError:
    def get_var(key: str, default=None):
        return os.getenv(key, default)


# ──────────────────────────────────────────────
# Resolve environment (dev / staging / prod)
# Source of truth = DB_DATABASE
# ──────────────────────────────────────────────

RAW_ENV = (get_var("DB_DATABASE", "dev") or "dev").lower()

if RAW_ENV.startswith("dev"):
    ENV = "dev"
elif RAW_ENV.startswith("stag"):
    ENV = "staging"
elif RAW_ENV in {"prod", "production", "heroku_postgres", "defaultdb"}:
    ENV = "prod"
else:
    ENV = "dev"

logging.info("Resolved RAW_ENV='%s' → ENV='%s'", RAW_ENV, ENV)


# ──────────────────────────────────────────────
# Resolve database connection
# Works for:
#  - Docker
#  - Airflow
#  - Local dev (.envrc)
# ──────────────────────────────────────────────

DATABASE_URL = (
    get_var("DATABASE_URL")
    or get_var("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
)

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. "
        "Check Docker env, Airflow Variables, or direnv (.envrc)."
    )

logging.info("Resolved DATABASE_URL for ENV='%s'", ENV)
