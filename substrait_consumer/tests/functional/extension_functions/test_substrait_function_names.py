import json
from typing import Callable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional import (
    arithmetic_configs, boolean_configs, comparison_configs, datetime_configs, logarithmic_configs, rounding_configs)
from substrait_consumer.functional.common import check_subtrait_function_names, load_custom_duckdb_table
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.duckdb_producer import DuckDBProducer


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestSubstraitFunctionNames:
    """
    Test Class for testing functions names in substrait plans created by different
    producers.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
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

        yield

        cls.db_connection.close()

    @custom_parametrization(
        arithmetic_configs.SCALAR_FUNCTIONS + arithmetic_configs.AGGREGATE_FUNCTIONS
    )
    def test_arithmetic_function_names(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
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
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
            lineitem,
            self.table_t,
        )

    @custom_parametrization(
        boolean_configs.SCALAR_FUNCTIONS + boolean_configs.AGGREGATE_FUNCTIONS
    )
    def test_boolean_function_names(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
    ) -> None:
        """
        Verify the substrait function names for boolean functions.
        """
        self.run_function_name_test(
            test_name,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            self.table_t,
        )

    @custom_parametrization(comparison_configs.SCALAR_FUNCTIONS)
    def test_comparison_function_names(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
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
            test_name,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
            nation,
        )

    @custom_parametrization(datetime_configs.SCALAR_FUNCTIONS)
    def test_datetime_function_names(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        """
        Verify the substrait function names for datetime functions.
        """
        self.run_function_name_test(
            test_name,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
        )

    @custom_parametrization(logarithmic_configs.SCALAR_FUNCTIONS)
    def test_logarithmic_function_names(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        """
        Verify the substrait function names for logarithmic functions.
        """
        self.run_function_name_test(
            test_name,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
        )

    @custom_parametrization(rounding_configs.SCALAR_FUNCTIONS)
    def test_rounding_function_names(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
    ) -> None:
        """
        Verify the substrait function names for rounding functions.
        """
        self.run_function_name_test(
            test_name,
            local_files,
            named_tables,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
        )

    def run_function_name_test(
        self,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
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
            local_files:
                A `dict` mapping format argument names to local files paths.
            named_tables:
                A `dict` mapping table names to local file paths.
            sql_query:
                SQL query.
            ibis_expr:
                Ibis expression.
            producer:
                Substrait producer class.
            *args:
                The data named_tables to be passed to the ibis expression.
        """

        producer.setup(self.db_connection, local_files, named_tables)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan_json = producer.produce_substrait(
                    sql_query[0], validate=False, ibis_expr=ibis_expr(*args)
                )
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            duckdb_producer = DuckDBProducer(self.db_connection)
            duckdb_producer.setup(self.db_connection, local_files, named_tables)
            substrait_plan_json = duckdb_producer.produce_substrait(sql_query[0])

        substrait_plan = json.loads(substrait_plan_json)
        check_subtrait_function_names(substrait_plan, test_name)
