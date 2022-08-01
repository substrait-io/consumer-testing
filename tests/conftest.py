from pathlib import Path

import pytest
from _pytest.main import Config, Session
from _pytest.nodes import Item
from _pytest.runner import CallInfo
from _pytest.terminal import TerminalReporter

FAILURES_FILE = Path() / "failures.log"


@pytest.hookimpl()
def pytest_sessionstart(session: Session):
    if FAILURES_FILE.exists():
        # Delete the file if it already exists so old failures aren't carried over.
        FAILURES_FILE.unlink()
    FAILURES_FILE.touch()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: Item, call: CallInfo):
    outcome = yield  # Run all other pytest_runtest_makereport non wrapped hooks
    result = outcome.get_result()
    if result.when == "call" and result.failed:
        try:
            with open(str(FAILURES_FILE), "a") as f:
                f.write(result.nodeid + "\n")
        except Exception as e:
            print("ERROR", e)
            pass


@pytest.hookimpl(hookwrapper=True)
def pytest_terminal_summary(
    terminalreporter: TerminalReporter, exitstatus: int, config: Config
):
    yield
    print(f"Failures outputted to: {FAILURES_FILE}")
    print(f"to see run\ncat {FAILURES_FILE}")
