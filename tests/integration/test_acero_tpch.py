import duckdb
import pytest
import substrait_validator as sv

from ..common import SubstraitUtils
from ..consumers.acero_consumer import AceroConsumer
from ..parametrization import custom_parametrization
from ..verification import verify_equals
from .queries.tpch_test_cases import TPCH_QUERY_TESTS


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
        cls.consumer = AceroConsumer()
        cls.utils = SubstraitUtils()

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

        # self.logger.info(f"Start to run test: {test_name}")
        # sv.check_plan_valid(substrait_query)

        # Format the substrait query to include the parquet file paths.
        # Calculate the result of running the substrait query plan.
        substrait_query = self.utils.format_substrait_query(substrait_query, file_names)
        subtrait_query_result_tb = self.consumer.run_substrait_query(substrait_query)

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
            subtrait_query_result_tb = self.utils.arrow_sort_tb_values(
                subtrait_query_result_tb, sortby=[subtrait_sort_col]
            )
            duckdb_sort_col = duckdb_query_result_tb.column_names[0]
            duckdb_query_result_tb = self.utils.arrow_sort_tb_values(
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
