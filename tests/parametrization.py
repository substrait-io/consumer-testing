import pytest

from _pytest.mark.structures import MarkDecorator


def custom_parametrization(test_cases: list[dict]) -> MarkDecorator:
    """
    Wrapper for pytest.mark.parametrize to make parametrized test cases more
    readable. Converts a list of dictionary key-value pairs to the
    corresponding pytest.mark.parametrize syntax.

    Example:
        Input:
            [
                {
                    "test_name": "test_1",
                    "expected_results": 5
                },
                {
                    "test_name": "test_2",
                    "expected_results": 1
                },
            ]
        Output:
            @pytest.mark.parametrize(
                'test_name,expected_results',
                [('test_1', 5), ('test_2', 1)]
            )
    """

    argument_names = ",".join(test_cases[0].keys())
    argument_values = []
    for test_case in test_cases:
        arg_names = ",".join(test_case.keys())
        if argument_names != arg_names:
            error_message = (
                f"Argument names between test cases are inconsistent.  "
                f"First arguments: {argument_names}, inconsistent " 
                f"arguments: {arg_names}"
            )
            raise ValueError(error_message)
        test_case_list = [value for value in test_case.values()]

        argument_values.append(tuple(test_case_list))

    return pytest.mark.parametrize(argnames=argument_names,
                                   argvalues=argument_values)
