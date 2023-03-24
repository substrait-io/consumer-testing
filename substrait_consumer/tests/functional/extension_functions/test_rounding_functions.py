from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.common import (
    generate_snapshot_results, substrait_consumer_function_test,
    substrait_producer_function_test)
from substrait_consumer.functional.rounding_configs import SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestRoundingFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait rounding functions.
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

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.produce_substrait_snapshot
    def test_producer_rounding_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
        lineitem
    ) -> None:
        test_name = f"rounding_snapshots:{test_name}"
        substrait_producer_function_test(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
            lineitem,
        )

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.consume_substrait_snapshot
    def test_consumer_rounding_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        partsupp,
        lineitem,
    ) -> None:
        test_name = f"rounding_snapshots:{test_name}"
        substrait_consumer_function_test(
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

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_rounding_functions_results(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"rounding_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
        )
