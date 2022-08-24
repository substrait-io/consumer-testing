from pathlib import Path

import duckdb
import pytest


@pytest.fixture(scope="class")
def prepare_tpch_parquet_data(scale_factor=0.1):
    """
    Generate TPCH data to be used for testing. Data is generated in tests/data/tpch_parquet

    Parameters:
        scale_factor:
            Scale factor for TPCH data generation.
    """
    data_path = Path(__file__).parent / "data" / "tpch_parquet"
    data_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CALL dbgen(sf={scale_factor})")
    con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")
