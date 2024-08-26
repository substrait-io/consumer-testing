import string
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable

from substrait_consumer.common import SubstraitUtils


class Producer(ABC):
    @abstractmethod
    def set_db_connection(self, db_connection):
        pass

    @abstractmethod
    def produce_substrait(self, sql_query: str, ibis_expr: str = None) -> str:
        pass

    @abstractmethod
    def format_sql(self, created_tables, sql_query, file_names):
        pass


def load_tables_from_parquet(
    db_connection,
    created_tables: set,
    file_names: Iterable[str],
) -> list:
    """
    Load all the parquet files into separate tables in DuckDB.

    Parameters:
        db_connection:
            DuckDB Connection.
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
        if table_name not in created_tables:
            create_table_sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
            db_connection.execute(create_table_sql)
            created_tables.add(table_name)
        table_names.append(table_name)

    return table_names
