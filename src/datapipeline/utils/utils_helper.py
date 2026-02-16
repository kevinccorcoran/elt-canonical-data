
from airflow.models import Variable

def get_db_connection_string(env: str) -> str:
    """Return the DATABASE_URL for the given environment (dev/staging/heroku_postgres)."""
    env_to_var_map = {
        "dev": "DATABASE_URL_DEV",
        "staging": "DATABASE_URL_STAGING",
        "heroku_postgres": "DATABASE_URL",
    }


    if env not in env_to_var_map:
        raise ValueError(f"Invalid environment specified: {env}. Please set a valid ENV variable.")
    

    connection_string = Variable.get(env_to_var_map[env], default_var=None)
    if not connection_string:
        raise ValueError(f"Environment variable {env_to_var_map[env]} is not set. Ensure it is defined in Airflow Variables.")
    
    return connection_string
