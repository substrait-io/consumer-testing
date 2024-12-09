from typing import Callable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.common import (
    generate_snapshot_results, load_custom_duckdb_table,
    substrait_consumer_sql_test, substrait_producer_sql_test)
from substrait_consumer.functional.comparison_configs import SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestComparisonFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait comparison functions.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        load_custom_duckdb_table(cls.db_connection)

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.produce_substrait_snapshot
    def test_producer_comparison_functions(
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
        nation,
    ) -> None:
        test_name = f"comparison_snapshots:{test_name}"
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
            nation,
        )

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.consume_substrait_snapshot
    def test_consumer_comparison_functions(
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
        nation,
    ) -> None:
        test_name = f"comparison_snapshots:{test_name}"
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

    @custom_parametrization(SCALAR_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_comparison_functions_results(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"comparison_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
