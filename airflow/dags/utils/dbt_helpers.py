from __future__ import annotations

from typing import Tuple, Dict


def get_dbt_bash_command(
    env: str,
    selector: str,
) -> Tuple[str, Dict[str, str]]:
    """
    Build a dbt CLI bash command and environment variables
    for use with an Airflow BashOperator.
    """

def _get_dbt_project_dir() -> Path:
    import os
    from pathlib import Path

    if "PROJECT_ROOT" in os.environ:
        project_root = Path(os.environ["PROJECT_ROOT"])
    else:
        # Fallback for local execution: calculate from this file
        # this file is in airflow/dags/utils
        # repo root is 3 levels up
        project_root = Path(__file__).resolve().parents[3]
    
    return project_root / "dbt" / "src" / "app"


def _get_inference_dbt_project_dir() -> Path:
    """Return the dbt project directory for elt-inference-models."""
    import os
    from pathlib import Path

    if "PROJECT_ROOT" in os.environ:
        # Production: inference repo is at /opt/elt-inference-models
        return Path("/opt/elt-inference-models") / "dbt" / "src" / "app"
    else:
        # Local: assume sibling repo
        repo_root = Path(__file__).resolve().parents[3]
        return repo_root.parent / "elt-inference-models" / "dbt" / "src" / "app"


def get_dbt_deps_command(
    env: str,
) -> Tuple[str, Dict[str, str]]:
    """
    Build a dbt deps bash command and environment variables
    for use with an Airflow BashOperator.
    """
    import logging
    dbt_project_dir = _get_dbt_project_dir()

    bash_command = (
        "set -euo pipefail && "
        f"cd {dbt_project_dir} && "
        "dbt deps"
    )

    logging.info(f"dbt_helpers: Generated dbt deps command: {bash_command}")

    env_vars = {
        "ENV": env,
        "DB_DATABASE": env,
    }

    return bash_command, env_vars


def get_dbt_bash_command(
    env: str,
    selector: str,
) -> Tuple[str, Dict[str, str]]:
    """
    Build a dbt CLI bash command and environment variables
    for use with an Airflow BashOperator.
    """

    import logging
    dbt_project_dir = _get_dbt_project_dir()

    logging.info(f"dbt_helpers: Using dbt_project_dir at {dbt_project_dir}")

    # Fail fast on any error, unset variable, or pipeline failure,
    # then run dbt from the correct project directory.
    bash_command = (
        "set -euo pipefail && "
        f"cd {dbt_project_dir} && "
        f"dbt run --select {selector}"
    )

    logging.info(f"dbt_helpers: Generated bash_command: {bash_command}")

    # Minimal environment overrides:
    # - ENV controls application-level environment awareness
    # - DB_DATABASE determines the target database for dbt
    
    # Securely fetch credentials (handling Airflow Variable fallback)
    import os
    try:
        from airflow.models import Variable
        password = Variable.get("DB_PASSWORD_PROD", default_var=os.getenv("DB_PASSWORD"))
    except ImportError:
        password = os.getenv("DB_PASSWORD")

    env_vars = {
        "ENV": env,
        "DB_DATABASE": env,
        "DB_PASSWORD": password,
    }
    
    if env == "prod":
        env_vars["DBT_TARGET"] = "prod"
        env_vars["DB_SSLMODE"] = "require"
    elif env == "staging":
        env_vars["DBT_TARGET"] = "staging"
        env_vars["DB_SSLMODE"] = "require"
    else:
        env_vars["DBT_TARGET"] = "dev"

    return bash_command, env_vars


def get_inference_dbt_bash_command(
    env: str,
    selector: str,
) -> Tuple[str, Dict[str, str]]:
    """
    Build a dbt CLI bash command for elt-inference-models project.
    """
    import logging
    import os

    dbt_project_dir = _get_inference_dbt_project_dir()

    logging.info(f"dbt_helpers: Using inference dbt_project_dir at {dbt_project_dir}")

    bash_command = (
        "set -euo pipefail && "
        f"cd {dbt_project_dir} && "
        f"dbt run --select {selector}"
    )

    logging.info(f"dbt_helpers: Generated inference bash_command: {bash_command}")

    try:
        from airflow.models import Variable
        password = Variable.get("DB_PASSWORD_PROD", default_var=os.getenv("DB_PASSWORD"))
    except ImportError:
        password = os.getenv("DB_PASSWORD")

    env_vars = {
        "ENV": env,
        "DB_DATABASE": env,
        "DB_PASSWORD": password,
    }
    
    if env == "prod":
        env_vars["DBT_TARGET"] = "prod"
        env_vars["DB_SSLMODE"] = "require"
    elif env == "staging":
        env_vars["DBT_TARGET"] = "staging"
        env_vars["DB_SSLMODE"] = "require"
    else:
        env_vars["DBT_TARGET"] = "dev"

    return bash_command, env_vars
