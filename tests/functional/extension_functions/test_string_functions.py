from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from tests.functional.string_tests import AGGREGATE_FUNCTIONS, SCALAR_FUNCTIONS
from tests.parametrization import custom_parametrization
from tests.verification import verify_equals


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestStringFunctions:
    """
    Test Class for testing Substrait comparison functions
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
        sql_query: str,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
        nation,
        orders,
    ) -> None:
        """
        Test for verifying duckdb is able to run substrait plans that include
        string functions produced by different producers.

        Parameters:
            test_name:
                Name of substrait function.
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
            ibis_expr:
                Ibis expression.
        """
        producer.set_db_connection(self.db_connection)
        consumer.set_db_connection(self.db_connection)

        # Load the parquet files into DuckDB and return all the table names as a list
        if len(file_names) > 0:
            table_names = consumer.load_tables_from_parquet(
                self.created_tables, file_names
            )
            # Format the sql_queries query by inserting all the table names
            sql_query = sql_query.format(*table_names)

        # Convert the SQL/Ibis expression to a substrait query plan
        if type(producer).__name__ == "IbisProducer":
            if ibis_expr:
                substrait_plan = producer.produce_substrait(
                    sql_query, consumer, ibis_expr(nation, orders)
                )
            else:
                pytest.skip("ibis expression currently undefined")
        else:
            substrait_plan = producer.produce_substrait(sql_query, consumer)

        actual_result = consumer.run_substrait_query(substrait_plan)
        expected_result = self.db_connection.query(f"{sql_query}").arrow()

        verify_equals(
            actual_result.columns,
            expected_result.columns,
            message=f"Result: {actual_result.columns} "
            f"is not equal to the expected: "
            f"{expected_result.columns}",
        )
