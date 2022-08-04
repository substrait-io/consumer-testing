import logging
import os
from collections.abc import Iterable

import pyarrow as pa

# create and configure main logger
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# create console handler with a higher log level
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setLevel(logging.DEBUG)

# create formatter and add it to the handler
FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_HANDLER.setFormatter(FORMATTER)

# add the handler to the logger
logging.getLogger("").addHandler(LOG_HANDLER)

REALPATH_DIRECTORY = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__))
)


def get_full_path(file_names: Iterable[str]) -> list[str]:
    """
    Get full paths for the TPCH parquet data.

    Parameters:
        file_names:
            List of TPCH parquet data file names provided by the test case.

    Returns:
        List of full paths.
    """
    data_dir = os.path.join(REALPATH_DIRECTORY, "data/tpch_parquet")
    full_paths_list = [os.path.join(data_dir, dataset) for dataset in file_names]

    return full_paths_list


def get_substrait_plan(filename: str) -> str:
    """
    Get the substrait plan.

    Parameters:
        filename:
            The file to retrieve the substrait plan from.

    Returns:
        String representation of the json formatted substrait plan.
    """
    plan_path = os.path.join(
        REALPATH_DIRECTORY, "integration/queries/tpch_substrait_plans", filename
    )

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
    plan_path = os.path.join(
        REALPATH_DIRECTORY, "integration/queries/tpch_sql", filename
    )

    with open(plan_path, "r") as f:
        return f.read()


class SubstraitUtils:
    """
    Common utility for substrait integration tests.
    """

    def __init__(self):
        """Initializes SubstraitUtils class with a logger"""
        self.logger = LOGGER

    @staticmethod
    def arrow_sort_tb_values(table: pa.Table, sortby: Iterable[str]) -> pa.Table:
        """
        Sort the pyarrow table by the given list of columns.

        Parameters:
            table:
                Original pyarrow Table.
            sortby:
                Columns to sort the results by.

        Returns:
            Pyarrow Table sorted by given columns.

        """
        table_sorted_indexes = pa.compute.bottom_k_unstable(
            table, sort_keys=sortby, k=len(table)
        )
        table_sorted = table.take(table_sorted_indexes)
        return table_sorted

    @staticmethod
    def format_sql_query(sql_query: str, file_names: list[str]) -> str:
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
        parquet_file_paths = get_full_path(file_names)

        return sql_query.format(*parquet_file_paths)

    @staticmethod
    def format_substrait_query(substrait_query: str, file_names: list[str]) -> bytes:
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
        parquet_file_paths = get_full_path(file_names)

        # Replace the filename placeholder in the substrait query plan with
        # the proper parquet data file paths.
        for count, file_path in enumerate(parquet_file_paths):
            substrait_query = substrait_query.replace(
                f"FILENAME_PLACEHOLDER_{count}", file_path
            )

        return pa.lib.tobytes(substrait_query)
