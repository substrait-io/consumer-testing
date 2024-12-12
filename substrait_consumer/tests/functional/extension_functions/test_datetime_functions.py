import duckdb
import pytest

from substrait_consumer.functional.common import (
    generate_snapshot_results,
    substrait_consumer_sql_test,
)
from substrait_consumer.functional.datetime_configs import SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.consumers.datafusion_consumer import DataFusionConsumer


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue("producer")
    consumer = request.getfixturevalue("consumer")
    func_name = request.node.callspec.id.split("-")[-1]
    if isinstance(consumer, DataFusionConsumer) and func_name in [
        "lt",
        "lte",
        "gt",
        "gte",
    ]:
        pytest.skip(reason="Flaky results")


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
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_datetime_functions(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        producer,
        consumer,
    ) -> None:
        test_name = f"function:datetime:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
            producer,
            consumer,
        )

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
