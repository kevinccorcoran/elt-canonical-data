
import os
import logging

# Airflow Variables → env vars
try:
    from airflow.models import Variable

    def get_var(key: str, default=None):
        """
        Prioritize OS environment variables, then Airflow variables.
        Filters out 'REPLACEME' placeholders.
        """
        val = os.getenv(key)

        # If OS env exists but is a placeholder, treat as None
        if val and "REPLACEME" in val:
            logging.debug("Ignoring placeholder OS ENV for '%s'", key)
            val = None

        if val is not None:
            logging.info("Resolved var '%s' from OS ENV", key)
            return val

        try:
            val = Variable.get(key)
            if val and "REPLACEME" in val:
                logging.info("Ignoring placeholder Airflow Variable for '%s'", key)
                return default
            logging.info("Resolved var '%s' from Airflow Variable", key)
            return val
        except Exception:
            return default

except ModuleNotFoundError:
    def get_var(key: str, default=None):
        val = os.getenv(key)
        if val and "REPLACEME" in val:
            return default
        return val or default


# Resolve environment. Prefer an explicit ENV (OS env / Airflow Variable) so a
# local dev instance pointed at a non-dev database (e.g. DB_DATABASE=staging)
# still runs as dev and picks up the ticker subset. SAFETY: a production database
# ALWAYS forces prod, so an accidental ENV=dev can never ingest a tiny ticker
# subset into prod. With ENV unset, behaviour is unchanged (derives from the db).

def _explicit_env() -> str:
    """ENV flag with the Airflow Variable taking PRECEDENCE over OS env, so
    dev/staging can be switched from the Airflow UI without editing .env or
    recreating the container. (Deliberately the reverse of get_var's OS-first
    order.) Falls back to OS env when no Airflow Variable is set (e.g. prod,
    or running outside Airflow)."""
    try:
        from airflow.models import Variable
        v = Variable.get("ENV", default_var=None)
        if v and "REPLACEME" not in v:
            return v.strip().lower()
    except Exception:
        pass
    return (os.getenv("ENV") or "").lower()


DB_ENV = (get_var("DB_DATABASE", "") or "").lower()
EXPLICIT_ENV = _explicit_env()
RAW_ENV = EXPLICIT_ENV or DB_ENV

if DB_ENV in {"prod", "production", "heroku_postgres", "defaultdb"} or DB_ENV.startswith("prod"):
    ENV = "prod"                       # prod database → always prod (hard guard)
elif EXPLICIT_ENV.startswith("dev"):
    ENV = "dev"                        # explicit ENV=dev wins (local dev on any non-prod db)
elif EXPLICIT_ENV.startswith("stag"):
    ENV = "staging"
elif EXPLICIT_ENV in {"prod", "production"}:
    ENV = "prod"
elif DB_ENV.startswith("dev"):
    ENV = "dev"                        # fall back to db-name derivation
elif DB_ENV.startswith("stag"):
    ENV = "staging"
else:
    ENV = "dev"

logging.info(
    "Resolved ENV='%s' (explicit ENV=%r, DB_DATABASE-env=%r)", ENV, EXPLICIT_ENV, DB_ENV
)


# Resolve database connection — the DB FOLLOWS the resolved ENV. Pick the
# per-env URL Variable (DATABASE_URL_DEV / _STAGING / _PROD, matching the
# Airflow Variables) so switching ENV moves the database too. Fall back to a
# generic DATABASE_URL (OS env) or the Airflow SQLAlchemy conn.

DATABASE_URL = (
    get_var(f"DATABASE_URL_{ENV.upper()}")
    or get_var("DATABASE_URL")
    or get_var("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
)

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. "
        "Check Docker env, Airflow Variables, or direnv (.envrc)."
    )

# Derive the db name from the resolved URL and inject ENV/DB_DATABASE/DATABASE_URL
# back into the process env, so every downstream reader (os.environ, dbt env_var)
# is consistent with what we just resolved instead of a stale .env value.
DB_DATABASE = DATABASE_URL.rsplit("/", 1)[-1].split("?")[0]
os.environ["ENV"] = ENV
os.environ["DB_DATABASE"] = DB_DATABASE
os.environ["DATABASE_URL"] = DATABASE_URL

logging.info("Resolved DATABASE_URL for ENV='%s' (DB_DATABASE=%r)", ENV, DB_DATABASE)
