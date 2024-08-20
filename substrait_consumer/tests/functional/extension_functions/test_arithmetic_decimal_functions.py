from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.arithmetic_decimal_configs import (
    AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS)
from substrait_consumer.functional.common import (
    generate_snapshot_results, load_custom_duckdb_table,
    substrait_consumer_sql_test, substrait_producer_sql_test)
from substrait_consumer.parametrization import custom_parametrization


@pytest.fixture
def mark_producer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    func_name = request.node.callspec.id.split('-')[1]
    if producer.__class__.__name__ == 'DataFusionProducer':
        if func_name in ["add", "subtract", "multiply", "divide", "modulus"]:
            pytest.skip(reason='DataFusion error: Invalid function')


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    consumer = request.getfixturevalue('consumer')
    func_name = request.node.callspec.id.split('-')[-1]
    if consumer.__class__.__name__ == 'DuckDBConsumer':
        if producer.__class__.__name__ != 'DuckDBProducer':
            pytest.skip(reason=f'Unsupported Integration: DuckDBConsumer with non {producer.__class__.__name__}')
    elif consumer.__class__.__name__ == 'DataFusionConsumer':
        if producer.__class__.__name__ != 'DataFusionProducer':
            pytest.skip(reason=f'Unsupported Integration: DataFusionConsumer with non {producer.__class__.__name__}')
        elif func_name in ["min", "max"]:
            pytest.skip(reason='pyarrow.lib.ArrowInvalid: Schema at index 0 was different')


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestArithmeticDecimalFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait arithmetic decimal functions.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        load_custom_duckdb_table(cls.db_connection)

        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.produce_substrait_snapshot
    @pytest.mark.usefixtures('mark_producer_tests_as_xfail')
    def test_producer_arithmetic_decimal_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        test_name = f"arithmetic_decimal_snapshots:{test_name}"
        substrait_producer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
        )

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_arithmetic_decimal_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        partsupp,
    ) -> None:
        test_name = f"arithmetic_decimal_snapshots:{test_name}"
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
    def test_generate_arithmetic_decimal_functions_results(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"arithmetic_decimal_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
        )
