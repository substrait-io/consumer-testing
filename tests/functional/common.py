from typing import Callable, Iterable

import pytest
from duckdb import DuckDBPyConnection
from ibis.expr.types.relations import Table

from tests.verification import verify_equals


def check_subtrait_function_names(
    substrait_plans: dict,
    expected_function_name,
):
    """
    Validate that the produced substrait plans are using the correct names for the
    substrait functions.

    Parameters:
        substrait_plans:
            Substrait Plan
        expected_function_name:
            Name of function as defined in substrait
    """
    functions_list = set()

    for function in substrait_plans["extensions"]:
        function_name = function["extensionFunction"]["name"]
        functions_list.add(function_name)

    # Verify that the proper substrait function name appears in the list of
    # extensionFunctions of the substrait plan
    assert (
        expected_function_name in functions_list
    ), f"Error in function: {expected_function_name}.  Does not appear in {functions_list}."


def substrait_function_test(
    db_con: DuckDBPyConnection,
    created_tables: set,
    file_names: Iterable[str],
    sql_query: str,
    ibis_expr: Callable[[Table], Table],
    producer,
    consumer,
    *args,
):
    """
    Verify the substrait plan produced for the specified function can run and return
    results equal to the results of running the SQL/Ibis expression against the data.

    Parameters:
        db_con:
            DuckDB connection for creating in memory tables.
        created_tables:
            Tables names that have already been created.
        file_names:
            List of parquet files.
        sql_query:
            SQL query.
        ibis_expr:
            Ibis expression.
        producer:
            Substrait producer class.
        consumer:
            Substrait consumer class.
        *args:
            The data tables to be passed to the ibis expression.
    """
    producer.set_db_connection(db_con)
    consumer.set_db_connection(db_con)

    # Load the parquet files into DuckDB and return all the table names as a list
    if len(file_names) > 0:
        table_names = consumer.load_tables_from_parquet(created_tables, file_names)
        # Format the sql_queries query by inserting all the table names
        sql_query = sql_query.format(*table_names)

    # Convert the SQL/Ibis expression to a substrait query plan
    if type(producer).__name__ == "IbisProducer":
        if ibis_expr:
            substrait_plan = producer.produce_substrait(
                sql_query, consumer, ibis_expr(*args)
            )
        else:
            pytest.skip("ibis expression currently undefined")
    else:
        substrait_plan = producer.produce_substrait(sql_query, consumer)

    actual_result = consumer.run_substrait_query(substrait_plan)
    expected_result = db_con.query(f"{sql_query}").arrow()

    verify_equals(
        actual_result.columns,
        expected_result.columns,
        message=f"Result: {actual_result.columns} "
        f"is not equal to the expected: "
        f"{expected_result.columns}",
    )
