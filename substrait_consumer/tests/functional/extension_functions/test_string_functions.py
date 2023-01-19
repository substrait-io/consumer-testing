from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.string_configs import AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.functional.common import substrait_function_test


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
    def test_string_functions(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        nation,
        orders,
    ) -> None:
        substrait_function_test(
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            consumer,
            nation,
            orders
        )
