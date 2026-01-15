from __future__ import annotations

from typing import Tuple, Dict


def get_dbt_bash_command(
    env: str,
    selector: str,
) -> Tuple[str, Dict[str, str]]:
    """
    Build a dbt CLI bash command and env vars for Airflow BashOperator.
    """

    dbt_project_dir = "/Users/kevin/repos/elt-canonical-data/dbt/src/app"

    bash_command = (
        "set -euo pipefail && "
        f"cd {dbt_project_dir} && "
        f"dbt run --select {selector}"
    )

    # Only override what must differ per environment
    env_vars = {
        "ENV": env,
        "DB_DATABASE": env,
    }

    return bash_command, env_vars
