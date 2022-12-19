from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from tests.functional.approximation_tests import AGGREGATE_FUNCTIONS
from tests.parametrization import custom_parametrization
from tests.functional.common import substrait_function_test


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestApproximationFunctions:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait approximation functions.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(AGGREGATE_FUNCTIONS)
    def test_approximation_functions(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        partsupp,
    ) -> None:
        substrait_function_test(
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            consumer,
            partsupp
        )
