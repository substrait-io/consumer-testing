from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.common import (
    generate_snapshot_results, substrait_consumer_sql_test,
    substrait_producer_sql_test)
from substrait_consumer.functional.string_configs import (
    AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS)
from substrait_consumer.parametrization import custom_parametrization


@pytest.fixture
def mark_producer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    func_name = request.node.callspec.id.split('-')[1]
    if producer.__class__.__name__ == 'DataFusionProducer':
        if func_name == "trim":
            pytest.skip(reason='DataFusion error: SQL(ParserError("Expected ), found: ,")')
    elif producer.__class__.__name__ == 'IsthmusProducer':
        if func_name == "concat":
            pytest.skip(reason='No match found for function signature CONCAT(<CHARACTER>, <CHARACTER>)')


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    consumer = request.getfixturevalue('consumer')
    if consumer.__class__.__name__ == 'DuckDBConsumer':
        if producer.__class__.__name__ != 'DuckDBProducer':
            pytest.skip(reason=f'Unsupported Integration: DuckDBConsumer with non {producer.__class__.__name__}')
    elif consumer.__class__.__name__ == 'DataFusionConsumer':
        if producer.__class__.__name__ != 'DataFusionProducer':
            pytest.skip(reason=f'Unsupported Integration: DataFusionConsumer with non {producer.__class__.__name__}')


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestStringFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait string functions.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.produce_substrait_snapshot
    @pytest.mark.usefixtures('mark_producer_tests_as_xfail')
    def test_producer_string_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        nation,
        orders,
    ) -> None:
        test_name = f"string_snapshots:{test_name}"
        substrait_producer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            nation,
            orders,
        )

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_string_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        nation,
        orders,
    ) -> None:
        test_name = f"string_snapshots:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            consumer,
        )

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_string_functions_results(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"string_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
        )
