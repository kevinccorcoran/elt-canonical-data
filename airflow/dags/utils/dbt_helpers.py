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

    import os
    from pathlib import Path

    # Absolute path to the dbt project directory.
    # Kept explicit to avoid ambiguity when Airflow runs in different contexts.
    
    if "PROJECT_ROOT" in os.environ:
        project_root = Path(os.environ["PROJECT_ROOT"])
    else:
        # Fallback for local execution: calculate from this file
        # this file is in airflow/dags/utils
        # repo root is 3 levels up
        project_root = Path(__file__).resolve().parents[3]

    dbt_project_dir = project_root / "dbt" / "src" / "app"

    # Fail fast on any error, unset variable, or pipeline failure,
    # then run dbt from the correct project directory.
    bash_command = (
        "set -euo pipefail && "
        f"cd {dbt_project_dir} && "
        f"dbt run --select {selector}"
    )

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
