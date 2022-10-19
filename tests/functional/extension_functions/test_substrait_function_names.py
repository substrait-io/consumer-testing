import json
from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from tests.consumers import DuckDBConsumer
from tests.functional import (
    arithmetic_tests, boolean_tests, comparison_tests, datetime_tests, logarithmic_tests,
    rounding_tests)
from tests.functional.common import check_subtrait_function_names
from tests.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestSubstraitFunctionNames:
    """
    Test Class for testing arithmetic functions in substrait plans created by different
    producers and consumed by different consumers.
    """

    @staticmethod
    @pytest.fixture(scope="function", autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        cls.db_connection.execute("create table t (a int, b int, c boolean, d boolean)")
        cls.db_connection.execute(
            "INSERT INTO t VALUES "
            "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
            "(-4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (-6, 2, TRUE, TRUE), "
            "(7, 2, FALSE, TRUE), (8, 2, True, TRUE), (9, 2, FALSE, TRUE), "
            "(NULL, 2, FALSE, TRUE);"
        )
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
        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = producer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(partsupp, nation)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)

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
        partsupp,
        nation,
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
        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = producer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(partsupp, nation)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)

    @custom_parametrization(comparison_tests.SCALAR_FUNCTIONS)
    def test_comparison_function_names(
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
        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = producer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(partsupp, nation)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)

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
        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = producer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(partsupp, nation)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)

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
        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = producer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(partsupp, nation)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)

    @custom_parametrization(rounding_tests.SCALAR_FUNCTIONS)
    def test_rounding_function_names(
        self,
        test_name: str,
        file_names: Iterable[str],
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp,
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
        producer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = producer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Grab the json representation of the produced substrait plan to verify
        # the proper substrait function name.
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, DuckDBConsumer, ibis_expr(partsupp, nation)
                )
                substrait_plan = json.loads(substrait_plan)
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_json = self.db_connection.get_substrait_json(sql_query)
            proto = substrait_json.fetchone()[0]
            substrait_plan = json.loads(proto)

        check_subtrait_function_names(substrait_plan, test_name)
