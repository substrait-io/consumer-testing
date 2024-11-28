import duckdb
import pytest

from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.verification import verify_equals
from .queries.tpch_test_cases import TPCH_QUERY_TESTS


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestDuckDBConsumer:
    """
    Test Class for testing Substrait using DuckDB as a consumer.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.consumer = DuckDBConsumer(cls.db_connection)
        cls.producer = DuckDBProducer(cls.db_connection)

        yield

        cls.db_connection.close()

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_substrait_query(
        self,
        test_name: str,
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
        4.  Run the substrait query plan.
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
        """
        self.consumer.setup(self.db_connection, local_files, named_tables)
        self.producer.setup(self.db_connection, local_files, named_tables)

        # Convert the SQL into a substrait query plan and run the plan.
        proto_bytes = self.producer.produce_substrait(sql_query)

        subtrait_query_result_tb = self.consumer.run_substrait_query(proto_bytes)

        # Calculate results to verify against by running the SQL query on DuckDB
        duckdb_sql_result_tb = self.producer.run_sql_query(sql_query)

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
