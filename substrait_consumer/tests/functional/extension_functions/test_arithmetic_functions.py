from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.arithmetic_configs import AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS
from substrait_consumer.functional.common import load_custom_duckdb_table, substrait_function_test
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
    def test_arithmetic_functions(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        partsupp,
        lineitem,
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
            lineitem,
            self.table_t,
        )
