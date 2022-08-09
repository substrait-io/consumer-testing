from typing import Any


class Verifier:
    """
    Class for handling verification.
    """

    @staticmethod
    def verify_equals(actual: Any, expected: Any, message: str = "") -> bool:
        """
        Verify that 2 objects are equal.  First check to see that object
        types are the same. If they differ, log the objects types and raise
        an error.
        If object types are the same but values are not equal, an error is
        raised and the message is shown.

        Parameters:
            actual:
                Object to evaluate against the expected object.
            expected:
                Object to be evaluated against.
            message:
                Message to be displayed if objects are not equal.

        Returns:
            bool:
                True if successful, otherwise raise an error.
        """
        msg = [f"TEST FAILURE: Verifying equals: {actual} == {expected}."]
        msg = [message] if message else msg

        assert isinstance(actual, type(expected)), (
            f"TEST FAILURE: Object types are not the same. \nActual "
            f"type: {type(actual)}\nExpected type: {type(expected)}"
        )
        assert actual == expected, msg

        return True
