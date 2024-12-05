from typing import Callable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.join_relation_configs import (
    JOIN_RELATION_TESTS)
from substrait_consumer.functional.common import (
    generate_snapshot_results,
    substrait_consumer_sql_test, substrait_producer_sql_test)
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
from substrait_consumer.consumers.datafusion_consumer import DataFusionConsumer
from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer


@pytest.fixture
def mark_producer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    test_case_name = request.node.callspec.id.split('-')[-1]
    if isinstance(producer, DuckDBProducer):
        if test_case_name in ["left_anti_join", "right_anti_join"]:
            pytest.skip(reason='INTERNAL Error: Unsupported join type ANTI')
        elif test_case_name in ["left_single_join", "right_single_join"]:
            pytest.skip(reason='INTERNAL Error: Unsupported join comparison: !=')
        elif test_case_name in ["left_mark_join", "right_mark_join"]:
            pytest.skip(reason='INTERNAL Error: Unsupported join type MARK')
    elif isinstance(producer, DataFusionProducer):
        if test_case_name in ["left_mark_join", "right_mark_join", "left_single_join", "right_single_join"]:
            pytest.skip(reason='Unsuppported join type')
    elif isinstance(producer, IsthmusProducer):
        if test_case_name in ["left_mark_join", "right_mark_join", "left_single_join", "right_single_join"]:
            pytest.skip(reason='Unsuppported join type')


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue('producer')
    consumer = request.getfixturevalue('consumer')
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


@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
class TestJoinRelation:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait join relations.
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

    @custom_parametrization(JOIN_RELATION_TESTS)
    @pytest.mark.produce_substrait_snapshot
    @pytest.mark.usefixtures('mark_producer_tests_as_xfail')
    def test_producer_join_relations(
        self,
        snapshot,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp
    ) -> None:
        test_name = f"join_relation_snapshots:{test_name}"
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
            validate=True
        )

    @custom_parametrization(JOIN_RELATION_TESTS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_join_relations(
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
        test_name = f"join_relation_snapshots:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            consumer,
        )

    @custom_parametrization(JOIN_RELATION_TESTS)
    @pytest.mark.generate_function_snapshots
    def test_generate_join_relation_results(
        self,
        snapshot,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"join_relation_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
