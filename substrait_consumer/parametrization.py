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
    has_mark = False

    argument_names = ",".join(
        [key for key in test_cases[0].keys() if not key == "pytest_mark"]
    )
    argument_values = []
    ids = []
    for test_case in test_cases:
        if "pytest_mark" in test_case.keys():
            has_mark = True
        arg_names = ",".join(
            [key for key in test_case.keys() if not key == "pytest_mark"]
        )
        if argument_names != arg_names:
            error_message = (
                f"Argument names between test cases are inconsistent.  "
                f"First arguments: {argument_names}, inconsistent "
                f"arguments: {arg_names}"
            )
            raise ValueError(error_message)
        test_case_list = [value for value in test_case.values()]
        if has_mark:
            mark_value = test_case_list.pop(0)
            if type(mark_value) != MarkDecorator:
                raise AssertionError("Mark is not of type MarkDecorator")
            case_tuple = pytest.param(*tuple(test_case_list), marks=mark_value)
        else:
            case_tuple = tuple(test_case_list)

        has_mark = False

        argument_values.append(case_tuple)
        ids.append(test_case["test_name"])

    return pytest.mark.parametrize(
        argnames=argument_names, argvalues=argument_values, ids=ids
    )
