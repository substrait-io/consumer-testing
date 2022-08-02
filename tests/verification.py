import logging
import operator
from typing import Any

# create and configure main logger
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# create console handler with a higher log level
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setLevel(logging.DEBUG)

# create formatter and add it to the handler
FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_HANDLER.setFormatter(FORMATTER)

# add the handler to the logger
logging.getLogger("").addHandler(LOG_HANDLER)


class Verifier:
    """
    Class for handling verification.
    """

    def __init__(self, name: str = "Verifier") -> None:
        """Initializes Verifier class with a logger"""
        self.logger = LOGGER
        self.logger.info(name)

    def fail(self, message: str = "") -> None:
        """
        Fail the test by raising an assertion error

        Parameters:
            message:
                Message to be logged.

        Raises:
            AssertionError:
                An error occurred.

        """
        raise AssertionError(f"TEST FAILURE: {message}")

    def verify_equals(self, actual: Any, expected: Any, message: str = "") -> bool:
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
        msg = [f"Verifying equals: {actual} == {expected}."]
        msg = [message] if message else msg

        if operator.ne(type(actual), type(expected)):
            msg = (
                f"Object types are not the same. \nActual type: "
                f"{type(actual)}\nExpected type: {type(expected)}"
            )
            self.fail(msg)
        elif operator.ne(actual, expected):
            self.fail("\n".join(msg))

        return True
