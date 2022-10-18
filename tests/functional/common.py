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
