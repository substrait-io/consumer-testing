from collections.abc import Iterable
from pathlib import Path

CUR_DIR = Path(__file__).parent


def get_substrait_plan(filename: str) -> str:
    """
    Get the substrait plan.

    Parameters:
        filename:
            The file to retrieve the substrait plan from.

    Returns:
        String representation of the json formatted substrait plan.
    """
    plan_path = CUR_DIR / "tests" / "integration" / "queries" / "tpch_substrait_plans" / filename

    with open(plan_path, "r") as f:
        return f.read()


def get_sql(filename: str) -> str:
    """
    Get the SQL query

    Parameters:
        filename:
            The file to retrieve the SQL from.

    Returns:
        String representation of the SQL query.
    """
    plan_path = CUR_DIR / "tests" / "integration" / "queries" / "tpch_sql" / filename

    with open(plan_path, "r") as f:
        return f.read()


class SubstraitUtils:
    """
    Common utility for substrait integration tests.
    """

    @staticmethod
    def compute_full_paths(local_files: dict[str, str]) -> dict[str, str]:
        """
        Get the full paths for the given local files.

        Parameters:
            local_files:
                A `dict` mapping format argument names to local files paths.

        Returns:
            A `dict` where the paths are expanded to absolute paths.
        """
        data_dir = CUR_DIR / "data" / "tpch_parquet"
        return {k: f"{data_dir}/{v}" for k, v in local_files.items()}
