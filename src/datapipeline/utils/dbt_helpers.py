from typing import Tuple, Dict
from airflow.models import Variable

def get_dbt_bash_command(runtime_env: str, model: str) -> Tuple[str, Dict[str, str]]:
    env_vars = {
        "DB_HOST": Variable.get("DB_HOST"),
        "DB_PORT": Variable.get("DB_PORT"),
        "DB_USER": Variable.get("DB_USER"),
        "DB_PASSWORD": Variable.get("DB_PASSWORD"),
        "DB_DATABASE": Variable.get("DB_DATABASE"),
        "DBT_NO_VERSION_CHECK": "1",
    }

    bash_command = f"""
        set -e
        echo "DB_USER: $DB_USER"
        echo "DB_HOST: $DB_HOST"
        echo "DB_PORT: $DB_PORT"
        echo "DB_DATABASE: $DB_DATABASE"

        cd /Users/kevin/repos/ELT_private/dbt/src/app

        /Users/kevin/.pyenv/shims/dbt run \
          --profiles-dir /Users/kevin/.dbt \
          --target {runtime_env} \
          --models {model} \
          --threads 4 \
          --quiet \
          --no-use-colors \
          --no-version-check
    """
    return bash_command, env_vars
