import logging
from typing import Any

from .verification import Verifier

# create and configure main logger
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# create console handler with a higher log level
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setLevel(logging.INFO)

# create formatter and add it to the handler
FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_HANDLER.setFormatter(FORMATTER)

# add the handler to the logger
logging.getLogger("").addHandler(LOG_HANDLER)


class BaseTest:
    """
    Base class for substrait consumer tests.
    """

    logger = LOGGER
    verifier = Verifier()

    @classmethod
    def fail(cls, message: str = "") -> None:
        """
        Log the message and raise an AssertionError.

        Parameters:
            message:
                message to be logged.
        """
        cls.verifier.fail(message)

    @classmethod
    def verify_equals(cls, actual: Any, expected: Any, message: str = "") -> None:
        """
        Verify 2 objects are equal.

        Parameters:
            actual:
                Challenge value.
            expected:
                Expected value to be verified against.
            message:
                Error message logged if objects aren't equal.
        """
        cls.verifier.verify_equals(actual, expected, message)
