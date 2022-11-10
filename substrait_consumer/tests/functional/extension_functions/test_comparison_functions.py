from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.common import load_custom_duckdb_table, substrait_function_test
from substrait_consumer.functional.comparison_configs import SCALAR_FUNCTIONS
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestComparisonFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait comparison functions.
    """

    @staticmethod
    @pytest.fixture(scope="function", autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        load_custom_duckdb_table(cls.db_connection)

        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(SCALAR_FUNCTIONS)
    def test_comparison_functions(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        partsupp,
        nation,
    ) -> None:
        substrait_function_test(
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            consumer,
            partsupp,
            nation,
        )
