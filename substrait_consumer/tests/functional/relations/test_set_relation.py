from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.set_relation_configs import (
    SET_RELATION_TESTS)
from substrait_consumer.functional.common import (
    generate_snapshot_results,
    substrait_consumer_sql_test, substrait_producer_sql_test)
from substrait_consumer.parametrization import custom_parametrization


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


@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
class TestSetRelation:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait sort relations.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")

        yield

        cls.db_connection.close()

    @custom_parametrization(SET_RELATION_TESTS)
    @pytest.mark.produce_substrait_snapshot
    def test_producer_set_relations(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp
    ) -> None:
        test_name = f"set_relation_snapshots:{test_name}"
        substrait_producer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
            validate=True
        )

    @custom_parametrization(SET_RELATION_TESTS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_set_relations(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
    ) -> None:
        test_name = f"set_relation_snapshots:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            consumer,
        )

    @custom_parametrization(SET_RELATION_TESTS)
    @pytest.mark.generate_function_snapshots
    def test_generate_set_relation_results(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"set_relation_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            file_names,
            sql_query,
        )
