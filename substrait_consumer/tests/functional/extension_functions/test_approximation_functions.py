from typing import Callable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.approximation_configs import AGGREGATE_FUNCTIONS
from substrait_consumer.functional.common import (
    generate_snapshot_results, substrait_consumer_sql_test,
    substrait_producer_sql_test)
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestApproximationFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait approximation functions.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")

        yield

        cls.db_connection.close()

    @custom_parametrization(AGGREGATE_FUNCTIONS)
    @pytest.mark.produce_substrait_snapshot
    def test_producer_approximation_functions(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        test_name = f"approximation_snapshots:{test_name}"
        substrait_producer_sql_test(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
        )

    @custom_parametrization(AGGREGATE_FUNCTIONS)
    @pytest.mark.consume_substrait_snapshot
    def test_consumer_approximation_functions(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        partsupp,
    ) -> None:
        test_name = f"approximation_snapshots:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            consumer,
        )

    @custom_parametrization(AGGREGATE_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_approximation_functions_results(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"approximation_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
