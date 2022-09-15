import json
from typing import Callable, Iterable

import duckdb
import pytest
from ibis.expr.types.relations import Table

from tests.consumers import DuckDBConsumer
from tests.functional.common import check_subtrait_function_names
from tests.functional import arithmetic_tests, rounding_tests, approximation_tests, \
    arithmetic_decimal_tests, comparison_tests, datetime_tests, boolean_tests, \
    logarithmic_tests, string_tests
from tests.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestSubstraitFunctionNames:
    """
    Test Class for testing arithmetic functions in substrait plans created by different
    producers and consumed by different consumers.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        cls.db_connection.execute("create table t (a int, b int, c boolean, d boolean)")
        cls.db_connection.execute(
            "INSERT INTO t VALUES "
            "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
            "(4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (6, 2, TRUE, TRUE), "
            "(7, 2, FALSE, TRUE), (8, 2, True, TRUE), (9, 2, FALSE, TRUE), "
            "(NULL, 2, FALSE, TRUE);"
        )
        cls.consumer = DuckDBConsumer(cls.db_connection)
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(
        approximation_tests.AGGREGATE_FUNCTIONS
        + arithmetic_decimal_tests.SCALAR_FUNCTIONS
        + arithmetic_decimal_tests.AGGREGATE_FUNCTIONS
        + arithmetic_tests.SCALAR_FUNCTIONS
        + arithmetic_tests.AGGREGATE_FUNCTIONS
        + boolean_tests.SCALAR_FUNCTIONS
        + boolean_tests.AGGREGATE_FUNCTIONS
        + comparison_tests.SCALAR_FUNCTIONS
        + datetime_tests.SCALAR_FUNCTIONS
        + logarithmic_tests.SCALAR_FUNCTIONS
        + rounding_tests.SCALAR_FUNCTIONS
        + string_tests.SCALAR_FUNCTIONS
        + string_tests.AGGREGATE_FUNCTIONS
    )
    def test_duckdb_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
    ) -> None:
        """
        Verify the substrait function names that appear in the produced plan match up
        with the function names as defined in Substrait.

        Parameters:
            test_name:
                Expected function name as defined by the substrait spec.
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
        """
        # DuckDB Produced function name verification
        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = self.consumer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the DuckDB producer substrait plan to verify
        # the proper substrait function name.
        substrait_json = self.db_connection.get_substrait_json(sql_query)
        proto = substrait_json.fetchone()[0]
        substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)
