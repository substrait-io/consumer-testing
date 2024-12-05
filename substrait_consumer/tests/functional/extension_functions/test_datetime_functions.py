from pathlib import Path
from typing import Callable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.common import (
    generate_snapshot_results, substrait_consumer_sql_test,
    substrait_producer_sql_test)
from substrait_consumer.functional.datetime_configs import SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.consumers.datafusion_consumer import DataFusionConsumer
from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"

@pytest.fixture
def mark_producer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    func_name = request.node.callspec.id.split('-')[1]
    if isinstance(producer, DuckDBProducer):
        if func_name == "add_intervals":
            pytest.skip(reason='INTERNAL Error: DUMMY_SCAN')


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    consumer = request.getfixturevalue('consumer')
    func_name = request.node.callspec.id.split('-')[-1]
    if isinstance(consumer, DuckDBConsumer):
        if not isinstance(producer, DuckDBProducer):
            pytest.skip(
                reason=f"Unsupported Integration: duckdb consumer with {producer.name()} producer"
            )
    elif isinstance(consumer, DataFusionConsumer):
        if not isinstance(producer, DataFusionProducer):
            pytest.skip(
                reason=f"Unsupported Integration: datafusion consumer with {producer.name()} producer"
            )
        elif func_name in ["extract"]:
            pytest.skip(reason='Results mismatch. Rounding Error')
        elif func_name in ["lt", "lte", "gt", "gte"]:
            pytest.skip(reason='Results mismatch')


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
    @pytest.mark.produce_substrait_snapshot
    @pytest.mark.usefixtures('mark_producer_tests_as_xfail')
    def test_producer_datetime_functions(
        self,
        snapshot,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        snapshot.snapshot_dir = SNAPSHOT_DIR / "producer" / "datetime"
        substrait_producer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
        )

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_datetime_functions(
        self,
        snapshot,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
    ) -> None:
        test_name = f"{test_name}-{producer.name()}"
        plan_path = SNAPSHOT_DIR / "producer" / "datetime" / f"{test_name}_plan.json"
        snapshot.snapshot_dir = SNAPSHOT_DIR / "consumer" / "datetime"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            plan_path,
            consumer,
        )

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_datetime_functions_results(
        self,
        snapshot,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        snapshot.snapshot_dir = SNAPSHOT_DIR / "consumer" / "datetime"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
