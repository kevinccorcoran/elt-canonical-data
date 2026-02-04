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
    # All other connection details are expected to be inherited
    # from the Airflow/direnv environment.
    env_vars = {
        "ENV": env,
        "DB_DATABASE": env,
    }

    return bash_command, env_vars
