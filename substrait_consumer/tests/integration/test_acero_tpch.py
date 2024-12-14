from typing import Iterable

import duckdb
import pyarrow as pa
import pytest

from pytest_snapshot.plugin import Snapshot

from substrait_consumer.common import SubstraitUtils
from substrait_consumer.consumers.acero_consumer import AceroConsumer
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.tests.integration.queries.tpch_test_cases import TPCH_QUERY_TESTS


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestAceroConsumer:
    """
    Test Class for testing Substrait using Acero as a consumer.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls
        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.duckdb_producer = DuckDBProducer(cls.db_connection)
        cls.acero_consumer = AceroConsumer()
        cls.utils = SubstraitUtils()

        yield

        cls.db_connection.close()

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_isthmus_substrait_plan(
        self,
        test_name: str,
        snapshot: Snapshot,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        substrait_query: str,
        sort_results: bool = False,
    ) -> None:
        """
        1.  Format the substrait_query by replacing the 'local_files' 'uri_file'
            path with the full path to the parquet data.
        2.  Format the SQL query to work with DuckDB by setting the 'Table'
            Parameters to be the relative files paths for parquet data.
        3.  Run the substrait query plan.
        4.  Execute the SQL on DuckDB.
        5.  Compare substrait query plan results against the results of
            running the SQL on DuckDB.

        Parameters:
            test_name:
                Name of test.
            local_files:
                A `dict` mapping format argument names to local files paths.
            named_tables:
                A `dict` mapping table names to local file paths.
            sql_query:
                SQL query.
            substrait_query:
                Substrait query.
            sort_results:
                Whether to sort the results before comparison.
        """
        tpch_num = test_name.split("_")[-1].zfill(2)

        # Format the substrait query to include the parquet file paths.
        # Calculate the result of running the substrait query plan.
        consumer = AceroConsumer()
        consumer.setup(self.db_connection, local_files, named_tables)

        try:
            subtrait_query_result_tb = consumer.run_substrait_query(substrait_query)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
            return

        # Reformat the sql query to be used by duck db by inserting all the
        # parquet filepaths where the table names should be.
        # Calculate results to verify against by running the SQL query on DuckDB
        sql_query = self.duckdb_producer.format_sql(sql_query)
        try:
            duckdb_query_result_tb = self.duckdb_producer.run_sql_query(sql_query)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
            return

        col_names = [x.lower() for x in subtrait_query_result_tb.column_names]
        exp_col_names = [x.lower() for x in duckdb_query_result_tb.column_names]

        # Sort results by specified column names
        if sort_results:
            subtrait_sort_col = subtrait_query_result_tb.column_names[0]
            subtrait_query_result_tb = arrow_sort_tb_values(
                subtrait_query_result_tb, sortby=[subtrait_sort_col]
            )
            duckdb_sort_col = duckdb_query_result_tb.column_names[0]
            duckdb_query_result_tb = arrow_sort_tb_values(
                duckdb_query_result_tb, sortby=[duckdb_sort_col]
            )

        # Verify results between substrait plan query and sql running against
        # duckdb are equal.
        outcome = {
            "column_names": col_names == exp_col_names,
            "table": subtrait_query_result_tb == duckdb_query_result_tb,
        }
        snapshot.assert_match(str(outcome), f"query_{tpch_num}_outcome.txt")

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_duckdb_substrait_plan(
        self,
        test_name: str,
        snapshot: Snapshot,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        substrait_query: str,
        sort_results: bool = False,
    ) -> None:
        """
        1.  Load all the parquet files into DuckDB as separate named_tables.
        2.  Format the SQL query to work with DuckDB by inserting all the table names.
        3.  Execute the SQL on DuckDB.
        4.  Produce the substrait plan with duckdb
        5.  Run the duckdb substrait plan against Acero
        5.  Compare the results of running the duckdb plan on Acero against the results of
            running the SQL on DuckDB.

        Parameters:
            test_name:
                Name of test.
            local_files:
                A `dict` mapping format argument names to local files paths.
            named_tables:
                A `dict` mapping table names to local file paths.
            sql_query:
                SQL query.
        """
        tpch_num = test_name.split("_")[-1].zfill(2)

        self.duckdb_producer.setup(self.db_connection, local_files, named_tables)
        self.acero_consumer.setup(self.db_connection, local_files, named_tables)

        # Convert the SQL into a substrait query plan
        try:
            proto_bytes = self.duckdb_producer.produce_substrait(sql_query)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
            return

        # Run the duckdb produced substrait plan against Acero
        try:
            subtrait_query_result_tb = self.acero_consumer.run_substrait_query(
                proto_bytes
            )
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
            return

        # Calculate results to verify against by running the SQL query on DuckDB
        try:
            duckdb_sql_result_tb = self.duckdb_producer.run_sql_query(sql_query)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
            return

        col_names = [x.lower() for x in subtrait_query_result_tb.column_names]
        exp_col_names = [x.lower() for x in duckdb_sql_result_tb.column_names]

        # Verify results between substrait plan query and sql running against
        # duckdb are equal.
        outcome = {
            "column_names": col_names == exp_col_names,
            "table": subtrait_query_result_tb == duckdb_sql_result_tb,
        }
        snapshot.assert_match(str(outcome), f"query_{tpch_num}_outcome.txt")


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
