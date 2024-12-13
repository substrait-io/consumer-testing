import duckdb
import pytest

from substrait_consumer.functional.common import generate_snapshot_results
from substrait_consumer.functional.datetime_configs import SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestDatetimeFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait datetime functions.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_datetime_functions_results(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
    ) -> None:
        test_name = f"function:datetime:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
