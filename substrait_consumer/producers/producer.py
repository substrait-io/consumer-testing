from abc import ABC, abstractmethod
from typing import Optional

from duckdb import DuckDBPyConnection

from substrait_consumer.common import SubstraitUtils


class Producer(ABC):
    def __init__(
            self,
            db_connection: Optional[DuckDBPyConnection] = None,
            local_files: Optional[dict[str, str]] = None,
            named_tables: Optional[dict[str, str]] = None,
            ):
        if db_connection is None:
            db_connection = DuckDBPyConnection()
        if local_files is None:
            local_files = {}
        if named_tables is None:
            named_tables = {}
        self.setup(db_connection, local_files, named_tables)

    def setup(
            self,
            db_connection: DuckDBPyConnection,
            local_files: dict[str, str],
            named_tables: dict[str, str],
            ):
        """
        Initializes this `Producer` instance.

        In particular, expands the paths in `local_files` and `named_tables` to
        absolute paths and forwards the arguments to `self._setup` implemented
        by classes inheriting from `Producer`.

        Parameters:
            db_connection:
                DuckDB connection for this `Producer`.
            local_files:
                A `dict` mapping format argument names to local files paths.
            named_tables:
                A `dict` mapping table names to local file paths.
        """
        self._db_connection = db_connection
        self._local_files = SubstraitUtils.compute_full_paths(local_files)
        self._named_tables = SubstraitUtils.compute_full_paths(named_tables)
        self._setup(db_connection, self._local_files, self._named_tables)

    def produce_substrait(self, sql_query: str, validate = False, ibis_expr: str = None) -> str:
        """
        Produces a Substrait plan of the given query in JSON format.

        The query can be given either as sql_query or as Ibis expression. In
        the first case, the function first formats the query using
        `self.format_sql`. In either case, the function lets the concrete
        class produce the substrait plan using `self._produce_substrait`.

        Parameters:
            sql_query:
                SQL query.
            validate:
                Whether the Substrait plan should be validated.
            ibis_expr:
                Ibis expression.
        Returns:
            Substrait query plan in JSON format.
        """
        sql_query = self.format_sql(sql_query)
        return self._produce_substrait(sql_query, validate, ibis_expr)

    def format_sql(self, sql_query: str) -> str:
        """
        Formats the given SQL query.

        formatting consist of calling `self._format_sql` that is implemented by
        concrete classes with producer-specific formatting logic as well as
        substituting format arguments for named tables and local files.

        Parameters:
            sql_query:
                SQL query.
        Returns:
            Formatted SQL query.
        """
        sql_query = self._format_sql(sql_query)
        named_tables = {k: k for k in self._named_tables.keys()}
        return sql_query.format(**self._local_files, **named_tables)

    @abstractmethod
    def _setup(self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]):
        """
        Initializes this `Producer` instance with base-class-specific logic.

        This typically consists of loading the named tables into the producer
        back-end such that they are available during subsequent calls to
        `produce_substrait`.

        Parameters:
            db_connection:
                DuckDB connection for this `Producer`.
            local_files:
                A `dict` mapping format argument names to local files paths.
            named_tables:
                A `dict` mapping table names to local file paths.
        """
        pass

    @abstractmethod
    def _produce_substrait(self, sql_query: str, validate = False, ibis_expr: str = None) -> str:
        """
        Produces a Substrait plan of the given SQL query in JSON format.

        At this point, the SQL query has already been formatted by the base
        class.

        Parameters:
            sql_query:
                SQL query.
            validate:
                Whether the Substrait plan should be validated.
            ibis_expr:
                Ibis expression.
        Returns:
            Substrait query plan in JSON format.
        """
        pass

    def _format_sql(self, sql_query: str) -> str:
        """
        Executes producer-specific reformatting of the given SQL query.

        This function may be overridden by concrete classes in order to change
        (i.e., "reformat") the given SQL query such that it fits the syntax of
        the producer.

        Parameters:
            sql_query:
                SQL query.
        Returns:
            Formatted SQL query.
        """
        return sql_query


def load_named_tables(
    db_connection,
    named_tables: dict[str, str],
) -> None:
    """
    Load all the parquet files into separate named_tables in DuckDB.

    Parameters:
        db_connection:
            DuckDB Connection.
        named_tables:
            A `dict` mapping table names to local file paths.
    Returns:
        A list of the table names.
    """
    for table_name, file_path in named_tables.items():
        try:
            db_connection.execute(f"DROP TABLE {table_name}")
        except:
            pass
        create_table_sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
        db_connection.execute(create_table_sql)
