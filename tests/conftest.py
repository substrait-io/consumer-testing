import os
from pathlib import Path

import duckdb
import pytest

FAILURES_FILE = Path() / "failures.log"
REALPATH_DIRECTORY = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__))
)


@pytest.hookimpl()
def pytest_sessionstart():
    if FAILURES_FILE.exists():
        # Delete the file if it already exists so old failures aren't carried over.
        FAILURES_FILE.unlink()
    FAILURES_FILE.touch()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport():
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
def pytest_terminal_summary():
    yield
    print(f"Failures logged to: {FAILURES_FILE}")
    print(f"to see run\ncat {FAILURES_FILE}")


@pytest.fixture(scope="class")
def prepare_tpch_parquet_data(scale_factor=0.1):
    """
    Generate TPCH data to be used for testing. Data is generated in tests/data/tpch_parquet

    Parameters:
        scale_factor:
            Scale factor for TPCH data generation.
    """
    con = duckdb.connect()
    con.execute(f"CALL dbgen(sf={scale_factor})")
    con.execute(
        f"EXPORT DATABASE '{REALPATH_DIRECTORY}/data/tpch_parquet' (FORMAT PARQUET);"
    )
