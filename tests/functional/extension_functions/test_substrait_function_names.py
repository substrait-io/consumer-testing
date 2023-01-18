import json
from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from tests.consumers import DuckDBConsumer
from tests.functional import (
    arithmetic_tests, boolean_tests, comparison_tests, datetime_tests, logarithmic_tests,
    rounding_tests)
from tests.functional.common import check_subtrait_function_names, load_custom_duckdb_table
from tests.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestSubstraitFunctionNames:
    """
    Test Class for testing functions names in substrait plans created by different
    producers.
    """

    @staticmethod
    @pytest.fixture(scope="function", autouse=True)
    def setup_teardown_function(request):
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

    @custom_parametrization(
        arithmetic_tests.SCALAR_FUNCTIONS + arithmetic_tests.AGGREGATE_FUNCTIONS
    )
    def test_arithmetic_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
        lineitem,
    ) -> None:
        """
        Verify the substrait function names for arithmetic functions.
        """
        self.run_function_name_test(
            test_name,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
            lineitem,
            self.table_t,
        )

    @custom_parametrization(
        boolean_tests.SCALAR_FUNCTIONS + boolean_tests.AGGREGATE_FUNCTIONS
    )
    def test_boolean_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
    ) -> None:
        """
        Verify the substrait function names for boolean functions.
        """
        self.run_function_name_test(
            test_name, file_names, sql_query, ibis_expr, producer, self.table_t
        )

    @custom_parametrization(comparison_tests.SCALAR_FUNCTIONS)
    def test_comparison_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
        nation,
    ) -> None:
        """
        Verify the substrait function names for comparison functions.
        """
        self.run_function_name_test(
            test_name, file_names, sql_query, ibis_expr, producer, partsupp, nation
        )

    @custom_parametrization(datetime_tests.SCALAR_FUNCTIONS)
    def test_datetime_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        """
        Verify the substrait function names for datetime functions.
        """
        self.run_function_name_test(
            test_name, file_names, sql_query, ibis_expr, producer, partsupp
        )

    @custom_parametrization(logarithmic_tests.SCALAR_FUNCTIONS)
    def test_logarithmic_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        """
        Verify the substrait function names for logarithmic functions.
        """
        self.run_function_name_test(
            test_name, file_names, sql_query, ibis_expr, producer, partsupp
        )

    @custom_parametrization(rounding_tests.SCALAR_FUNCTIONS)
    def test_rounding_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        """
        Verify the substrait function names for rounding functions.
        """
        self.run_function_name_test(
            test_name, file_names, sql_query, ibis_expr, producer, partsupp
        )

    def run_function_name_test(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        *args
    ):
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
            ibis_expr:
                Ibis expression.
            producer:
                Substrait producer class.
            *args:
                The data tables to be passed to the ibis expression.
        """

        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        sql_query = producer.format_sql(self.created_tables, sql_query[0], file_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(*args)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)
