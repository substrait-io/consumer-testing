from typing import Callable, Iterable

import pytest
from duckdb import DuckDBPyConnection
from ibis.expr.types.relations import Table

from tests.verification import verify_equals


def check_subtrait_function_names(
    substrait_plan: dict,
    expected_function_name,
):
    """
    Validate that the produced substrait plans are using the correct names for the
    substrait functions.

    Parameters:
        substrait_plan:
            Substrait Plan
        expected_function_name:
            Name of function as defined in substrait
    """
    functions_list = set()

    for function in substrait_plan["extensions"]:
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
    sql_query: tuple,
    ibis_expr: Callable[[Table], Table],
    producer,
    consumer,
    *args,
):
    """
    Verify the substrait plan produced for the specified function can run and return
    results equal to the results of running the SQL/Ibis expression against the data
    with a trusted SQL consumer.

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
    consumer.setup(db_con, file_names)
    supported_producers = sql_query[1]

    # Load the parquet files into DuckDB and return all the table names as a list
    sql_query = producer.format_sql(created_tables, sql_query[0], file_names)

    # Convert the SQL/Ibis expression to a substrait query plan
    if type(producer).__name__ == "IbisProducer":
        if ibis_expr:
            substrait_plan = producer.produce_substrait(
                sql_query, consumer, ibis_expr(*args)
            )
        else:
            pytest.xfail("ibis expression currently undefined")
    else:
        if type(producer) in supported_producers:
            substrait_plan = producer.produce_substrait(sql_query, consumer)
        else:
            pytest.xfail(f"{type(producer).__name__} does not support the following SQL: "
                        f"{sql_query}")

    actual_result = consumer.run_substrait_query(substrait_plan)
    expected_result = db_con.query(f"{sql_query}").arrow()

    verify_equals(
        actual_result.columns,
        expected_result.columns,
        message=f"Result: {actual_result.columns} "
        f"is not equal to the expected: "
        f"{expected_result.columns}",
    )


def load_custom_duckdb_table(db_connection):
    db_connection.execute("create table t (a BIGINT, b BIGINT, c boolean, d boolean)")
    db_connection.execute(
        "INSERT INTO t VALUES "
        "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
        "(-4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (-6, 2, TRUE, TRUE), "
        "(7, 2, FALSE, TRUE), (8, 2, TRUE, TRUE), (9, 2, FALSE, TRUE), "
        "(NULL, 2, FALSE, TRUE);"
    )
