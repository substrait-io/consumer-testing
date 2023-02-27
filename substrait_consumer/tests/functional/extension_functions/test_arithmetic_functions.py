from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.arithmetic_configs import (
    AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS)
from substrait_consumer.functional.common import (
    load_custom_duckdb_table, substrait_consumer_function_test,
    substrait_producer_function_test, generate_snapshot_results)
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestArithmeticFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait arithmetic functions.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        load_custom_duckdb_table(cls.db_connection)
        cls.table_t = ibis.table(
            [("a", dt.int32), ("b", dt.int32), ("c", dt.boolean), ("d", dt.boolean)],
            name="t",
        )

        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.producer
    def test_producer_arithmetic_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
        lineitem,
    ) -> None:
        test_name = f"arithmetic_snapshots:{test_name}"
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
            self.table_t,
        )

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.consumer
    def test_consumer_arithmetic_functions(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer
    ) -> None:
        test_name = f"arithmetic_snapshots:{test_name}"
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

    @custom_parametrization(SCALAR_FUNCTIONS + AGGREGATE_FUNCTIONS)
    @pytest.mark.generate_function_snapshots
    def test_generate_arithmetic_functions_results(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        test_name = f"arithmetic_snapshots:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
        )
