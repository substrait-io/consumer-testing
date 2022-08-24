import string
from pathlib import Path
from typing import Iterable

import pyarrow as pa

from ..common import SubstraitUtils


class DuckDBConsumer:
    """
    Implementation of the DuckDB substrait consumer Class for testing
    """

    def __init__(self, db_connection):
        """
        Initialize the DuckDBConsumer Class with a duckdb connection.
        """
        self.db_connection = db_connection

    def run_substrait_query(self, sql_query: str) -> pa.Table:
        """
        Convert the SQL query into a substrait query plan and run the plan against DuckDB.

        Parameters:
            sql_query:
                A SQL query string.

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        duckdb_substrait_plan = self.db_connection.get_substrait(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        return self.db_connection.from_substrait(proto_bytes).arrow()

    def load_tables_from_parquet(
        self,
        created_tables: set,
        file_names: Iterable[str],
    ) -> list:
        """
        Load all the parquet files into separate tables in DuckDB.

        Parameters:
            created_tables:
                The set of tables that have already been created.
            file_names:
                Name of parquet files.

        Returns:
            A list of the table names.
        """
        parquet_file_paths = SubstraitUtils.get_full_path(file_names)
        table_names = []
        for file_name, file_path in zip(file_names, parquet_file_paths):
            table_name = Path(file_name).stem
            table_name = table_name.translate(str.maketrans("", "", string.punctuation))
            if table_name not in created_tables:
                create_table_sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
                self.db_connection.execute(create_table_sql)
                created_tables.add(table_name)
            table_names.append(table_name)

        return table_names
