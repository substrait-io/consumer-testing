import logging
import os

import pyarrow as pa
from pyarrow.lib import tobytes
from pyarrow.lib import Table

# create and configure main logger
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# create console handler with a higher log level
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setLevel(logging.DEBUG)

# create formatter and add it to the handler
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG_HANDLER.setFormatter(FORMATTER)

# add the handler to the logger
logging.getLogger('').addHandler(LOG_HANDLER)


def get_full_path(file_names: list[str]) -> list[str]:
    realpath_directory = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
    data_dir = os.path.join(realpath_directory, 'data/tpch_parquet')
    full_paths_list = (
        [os.path.join(data_dir, dataset) for dataset in file_names]
    )

    return full_paths_list


class SubstraitUtils(object):
    """
    Common utility for substrait integration tests.
    """
    def __init__(self, name="SubstraitUtils"):
        """

        """
        self.logger = LOGGER
        self.logger.info(name)

    @staticmethod
    def arrow_sort_tb_values(table: type[Table],
                             sortby: list[str]) -> type[Table]:
        """
        Sort the pyarrow table by the given list of columns.

        Args:
            table: Original pyarrow Table
            sortby: Columns to sort the results by

        Returns:
            Pyarrow Table sorted by given columns

        """
        table_sorted_indexes = pa.compute.bottom_k_unstable(table,
                                                            sort_keys=sortby,
                                                            k=len(table))
        table_sorted = table.take(table_sorted_indexes)
        return table_sorted

    @staticmethod
    def format_sql_query(sql_query: str, file_names: list[str]) -> str:
        """
        Replace the 'Table' Parameters from the SQL query with the relative
        file paths of the parquet data.

        Args:
            sql_query: SQL Query
            file_names: List of file names

        Returns:
            SQL Query with file paths
        """
        sql_commands_list = (
            [line.strip() for line in sql_query.strip().split("\n")]
        )
        sql_query = " ".join(sql_commands_list)
        # Get full path for all datasets used in the query
        parquet_file_paths = get_full_path(file_names)

        return sql_query.format(*parquet_file_paths)

    @staticmethod
    def format_substrait_query(substrait_query: str,
                               file_names: list[str]) -> bytes:
        """
        Replace the 'local_files' path in the substrait query plan with
        the full path of the parquet data.

        Args:
            substrait_query: Substrait Query
            file_names: List of file names

        Returns:
            Substrait query plan in byte format
        """
        # Get full path for all datasets used in the query
        parquet_file_paths = get_full_path(file_names)

        # Replace the filename placeholder in the substrait query plan with
        # the proper parquet data file paths.
        for count, file_path in enumerate(parquet_file_paths):
            substrait_query = substrait_query.replace(
                f"FILENAME_PLACEHOLDER_{count}", file_path)

        return tobytes(substrait_query)
