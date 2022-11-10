import string
from pathlib import Path
from typing import Iterable

import duckdb
import pytest
from google.protobuf import json_format
from ibis_substrait.compiler.core import SubstraitCompiler

from tests.common import SubstraitUtils


class DuckDBProducer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")

    def set_db_connection(self, db_connection):
        self.db_connection = db_connection

    def produce_substrait(
        self, sql_query: str, consumer, ibis_expr: str = None
    ) -> bytes:
        """
        Produce the DuckDB substrait plan using the given SQL query.

        Parameters:
            sql_query:
                SQL query.
            consumer:
                Name of substrait consumer.
        Returns:
            Substrait query plan in byte format.
        """
        if type(consumer).__name__ == "AceroConsumer":
            duckdb_substrait_plan = self.db_connection.get_substrait_json(sql_query)
        else:
            duckdb_substrait_plan = self.db_connection.get_substrait(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        return proto_bytes

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

    def format_sql(self, created_tables, sql_query, file_names):
        if len(file_names) > 0:
            table_names = self.load_tables_from_parquet(created_tables, file_names)
            sql_query = sql_query.format(*table_names)
        return sql_query


class IbisProducer:
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()

        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")
        self.compiler = SubstraitCompiler()

    def set_db_connection(self, db_connection):
        self.db_connection = db_connection

    def produce_substrait(
        self, sql_query: str, consumer, ibis_expr: str = None
    ) -> bytes:
        """
        Produce the Ibis substrait plan using the given Ibis expression

        Parameters:
            consumer:
                Name of substrait consumer.
            ibis_expr:
                Ibis expression.
        Returns:
            Substrait query plan in byte format.
        """
        if ibis_expr is None:
            pytest.skip("ibis expression currently undefined")
        tpch_proto_bytes = self.compiler.compile(ibis_expr)
        if type(consumer).__name__ == "DuckDBConsumer":
            substrait_plan = tpch_proto_bytes.SerializeToString()
        else:
            substrait_plan = json_format.MessageToJson(tpch_proto_bytes)
        return substrait_plan

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

    def format_sql(self, created_tables, sql_query, file_names):
        if len(file_names) > 0:
            table_names = self.load_tables_from_parquet(created_tables, file_names)
            sql_query = sql_query.format(*table_names)
        return sql_query
