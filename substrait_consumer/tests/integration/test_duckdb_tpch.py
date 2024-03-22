import duckdb
import pytest

from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.verification import verify_equals
from .queries.tpch_test_cases import TPCH_QUERY_TESTS


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestDuckDBConsumer:
    """
    Test Class for testing Substrait using DuckDB as a consumer.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.consumer = DuckDBConsumer(cls.db_connection)
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_substrait_query(
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
        4.  Run the substrait query plan.
        5.  Compare substrait query plan results against the results of
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
        table_names = self.consumer.load_tables_from_parquet(
            self.created_tables, file_names
        )

        # Format the sql query by inserting all the table names
        sql_query = sql_query.format(*table_names)

        # Convert the SQL into a substrait query plan and run the plan.
        substrait_plan = self.db_connection.get_substrait_json(sql_query)
        proto_bytes = substrait_plan.fetchone()[0]

        subtrait_query_result_tb = self.consumer.run_substrait_query(proto_bytes)

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
