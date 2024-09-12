from typing import Iterable

import duckdb
import pyarrow as pa
import pytest
from pyarrow import compute

from substrait_consumer.common import SubstraitUtils
from substrait_consumer.consumers.acero_consumer import AceroConsumer
from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.verification import verify_equals
from substrait_consumer.tests.integration.queries.tpch_test_cases import TPCH_QUERY_TESTS


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestAceroConsumer:
    """
    Test Class for testing Substrait using Acero as a consumer.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls
        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.duckdb_consumer = DuckDBConsumer(cls.db_connection)
        cls.acero_consumer = AceroConsumer()
        cls.utils = SubstraitUtils()
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_isthmus_substrait_plan(
        self,
        test_name: str,
        file_names: list,
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
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
            substrait_query:
                Substrait query.
            sort_results:
                Whether to sort the results before comparison.
        """
        # Format the substrait query to include the parquet file paths.
        # Calculate the result of running the substrait query plan.
        consumer = AceroConsumer()
        consumer.setup(self.db_connection, self.created_tables, file_names)

        subtrait_query_result_tb = consumer.run_substrait_query(
            substrait_query
        )

        # Reformat the sql query to be used by duck db by inserting all the
        # parquet filepaths where the table names should be.
        # Calculate results to verify against by running the SQL query on DuckDB
        sql_query = self.utils.format_sql_query(sql_query, file_names)
        duckdb_query_result_tb = self.db_connection.query(f"{sql_query}").arrow()

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
        verify_equals(
            col_names,
            exp_col_names,
            message=f"Actual column names: \n{col_names} \n"
            f"are not equal to the expected"
            f"column names: \n{exp_col_names}",
        )
        verify_equals(
            subtrait_query_result_tb,
            duckdb_query_result_tb,
            message=f"Result table: \n{subtrait_query_result_tb} \n"
            f"is not equal to the expected "
            f"table: \n{duckdb_query_result_tb}",
        )

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_duckdb_substrait_plan(
        self,
        test_name: str,
        file_names: list,
        sql_query: str,
        substrait_query: str,
        sort_results: bool = False,
    ) -> None:
        """
        1.  Load all the parquet files into DuckDB as separate tables.
        2.  Format the SQL query to work with DuckDB by inserting all the table names.
        3.  Execute the SQL on DuckDB.
        4.  Produce the substrait plan with duckdb
        5.  Run the duckdb substrait plan against Acero
        5.  Compare the results of running the duckdb plan on Acero against the results of
            running the SQL on DuckDB.

        Parameters:
            test_name:
                Name of test.
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
        """
        # Load the parquet files into DuckDB and return all the table names as a list
        table_names = self.duckdb_consumer.load_tables_from_parquet(
            self.created_tables, file_names
        )

        # Format the sql query by inserting all the table names
        sql_query = sql_query.format(*table_names)

        # Convert the SQL into a substrait query plan
        duckdb_substrait_plan = self.db_connection.get_substrait_json(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]

        # Run the duckdb produced substrait plan against Acero
        subtrait_query_result_tb = self.acero_consumer.run_substrait_query(proto_bytes)

        # Calculate results to verify against by running the SQL query on DuckDB
        duckdb_sql_result_tb = self.db_connection.query(f"{sql_query}").arrow()

        col_names = [x.lower() for x in subtrait_query_result_tb.column_names]
        exp_col_names = [x.lower() for x in duckdb_sql_result_tb.column_names]

        # Verify results between substrait plan query and sql running against
        # duckdb are equal.
        verify_equals(
            col_names,
            exp_col_names,
            message=f"Actual column names: \n{col_names} \n"
            f"are not equal to the expected"
            f"column names: \n{exp_col_names}",
        )
        verify_equals(
            subtrait_query_result_tb,
            duckdb_sql_result_tb,
            message=f"Result table: \n{subtrait_query_result_tb} \n"
            f"is not equal to the expected "
            f"table: \n{duckdb_sql_result_tb}",
        )


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
