from typing import Dict

from tests.verification import verify_equals
from tests.consumers import AceroConsumer


def run_subtrait_on_duckdb(
    db_connection, sql_query: str, substrait_proto: str
) -> None:
    """
    Run all the substrait plans on the DuckDB consumer and verify the results are equal
    to the result of running the sql query on DuckDB.

    Parameters:
        db_connection:
            DuckDB connection.
        sql_query:
            SQL Query.
        substrait_protos:
            List of substrait protobufs.
    """
    # Calculate results to verify against by running the SQL query on DuckDB.
    duckdb_sql_result_tb = db_connection.query(f"{sql_query}").arrow()

    subtrait_query_result_tb = db_connection.from_substrait(substrait_proto).arrow()

    # Verify results between substrait plan query and sql running against
    # duckdb are equal.
    verify_equals(
        subtrait_query_result_tb.columns,
        duckdb_sql_result_tb.columns,
        message=f"Result: {subtrait_query_result_tb.columns} "
                f"is not equal to the expected: "
                f"{duckdb_sql_result_tb.columns}",
    )


def run_subtrait_on_acero(
    db_connection, sql_query: str, substrait_protos: Dict[str, str]
) -> None:
    """
    Run all the substrait plans on the Acero consumer and verify the results are equal
    to the result of running the sql query on DuckDB.

    Parameters:
        db_connection:
            DuckDB connection.
        sql_query:
            SQL Query.
        substrait_protos:
            List of substrait protobufs.
    """
    # Calculate results to verify against by running the SQL query on DuckDB
    duckdb_sql_result_tb = db_connection.query(f"{sql_query}").arrow()

    for producer, proto_bytes in substrait_protos.items():
        # Run the duckdb produced substrait plan against Acero
        subtrait_query_result_tb = AceroConsumer.run_substrait_query(proto_bytes)

        verify_equals(
            subtrait_query_result_tb.columns,
            duckdb_sql_result_tb.columns,
            message=f"Result: {subtrait_query_result_tb.columns} "
                    f"is not equal to the expected: "
                    f"{duckdb_sql_result_tb.columns}",
        )


def check_subtrait_function_names(
    substrait_plans: str,
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
