from pathlib import Path
from typing import Callable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.aggregate_relation_configs import (
    AGGREGATE_RELATION_TESTS)
from substrait_consumer.functional.common import (
    generate_snapshot_results,
    substrait_consumer_sql_test, substrait_producer_sql_test)
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
from substrait_consumer.consumers.acero_consumer import AceroConsumer
from substrait_consumer.consumers.datafusion_consumer import DataFusionConsumer
from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"

@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue("producer")
    consumer = request.getfixturevalue("consumer")
    test_name = request.getfixturevalue("test_name")
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
    elif isinstance(consumer, AceroConsumer):
        if (
            isinstance(producer, IsthmusProducer)
        ) and test_name == "compute_within_aggregate":
            pytest.skip(
                reason="'isthmus-acero-compute_within_aggregate' currently crashes"
            )


@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
class TestAggregatetRelation:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait sort relations.
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

    @custom_parametrization(AGGREGATE_RELATION_TESTS)
    @pytest.mark.produce_substrait_snapshot
    def test_producer_aggregate_relations(
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
        snapshot.snapshot_dir = SNAPSHOT_DIR / "producer" / "aggregate"
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

    @custom_parametrization(AGGREGATE_RELATION_TESTS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_aggregate_relations(
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
        plan_path = SNAPSHOT_DIR / "producer" / "aggregate" / f"{test_name}_plan.json"
        snapshot.snapshot_dir = SNAPSHOT_DIR / "consumer" / "aggregate"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            plan_path,
            consumer,
        )

    @custom_parametrization(AGGREGATE_RELATION_TESTS)
    @pytest.mark.generate_function_snapshots
    def test_generate_aggregate_relation_results(
        self,
        snapshot,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        snapshot.snapshot_dir = SNAPSHOT_DIR / "consumer" / "aggregate"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
