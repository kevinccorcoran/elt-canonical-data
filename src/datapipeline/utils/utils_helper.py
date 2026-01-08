# Airflow-specific imports
from airflow.models import Variable

def get_db_connection_string(env: str) -> str:
    """
    Retrieve the database connection string based on the environment.

    Args:
        env (str): The environment name ("dev", "staging", "heroku_postgres").

    Returns:
        str: The database connection string for the specified environment.

    Raises:
        ValueError: If the environment is invalid or the corresponding Variable is not set.
    """
    # Mapping of environments to their respective Airflow Variable names
    env_to_var_map = {
        "dev": "DATABASE_URL_DEV",
        "staging": "DATABASE_URL_STAGING",
        "heroku_postgres": "DATABASE_URL",
    }

    # Validate the environment and retrieve the corresponding variable
    if env not in env_to_var_map:
        raise ValueError(f"Invalid environment specified: {env}. Please set a valid ENV variable.")
    
    # Retrieve the connection string from Airflow Variables
    connection_string = Variable.get(env_to_var_map[env], default_var=None)
    if not connection_string:
        raise ValueError(f"Environment variable {env_to_var_map[env]} is not set. Ensure it is defined in Airflow Variables.")
    
    return connection_string
