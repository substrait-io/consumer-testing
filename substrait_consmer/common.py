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
    plan_path = CUR_DIR / "integration" / "queries" / "tpch_substrait_plans" / filename

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
    plan_path = CUR_DIR / "integration" / "queries" / "tpch_sql" / filename

    with open(plan_path, "r") as f:
        return f.read()


class SubstraitUtils:
    """
    Common utility for substrait integration tests.
    """

    @staticmethod
    def get_full_path(file_names: Iterable[str]) -> list[str]:
        """
        Get full paths for the TPCH parquet data.

        Parameters:
            file_names:
                List of TPCH parquet data file names provided by the test case.

        Returns:
            List of full paths.
        """
        data_dir = CUR_DIR / "data" / "tpch_parquet"
        full_paths_list = [f"{data_dir}/{dataset}" for dataset in file_names]

        return full_paths_list

    def format_sql_query(self, sql_query: str, file_names: list[str]) -> str:
        """
        Replace the 'Table' Parameters from the SQL query with the relative
        file paths of the parquet data.

        Parameters:
            sql_query:
                SQL query.
            file_names:
                List of file names.

        Returns:
            SQL Query with file paths.
        """
        sql_commands_list = [line.strip() for line in sql_query.strip().split("\n")]
        sql_query = " ".join(sql_commands_list)
        # Get full path for all datasets used in the query
        parquet_file_paths = self.get_full_path(file_names)

        return sql_query.format(*parquet_file_paths)

    def format_substrait_query(
        self, substrait_query: str, file_names: list[str]
    ) -> str:
        """
        Replace the 'local_files' path in the substrait query plan with
        the full path of the parquet data.

        Parameters:
            substrait_query:
                Substrait query.
            file_names:
                List of file names.

        Returns:
            Substrait query plan in byte format.
        """
        # Get full path for all datasets used in the query
        parquet_file_paths = self.get_full_path(file_names)

        # Replace the filename placeholder in the substrait query plan with
        # the proper parquet data file paths.
        for count, file_path in enumerate(parquet_file_paths):
            substrait_query = substrait_query.replace(
                f"FILENAME_PLACEHOLDER_{count}", file_path
            )

        return substrait_query
