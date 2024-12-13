from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
SCALAR_DIR = CONFIG_DIR / "function" / "arithmetic" / "scalar"
AGGREGATE_DIR = CONFIG_DIR / "function" / "arithmetic" / "aggregate"

SCALAR_FUNCTIONS = tuple(load_json(file) for file in SCALAR_DIR.glob("*.json"))
AGGREGATE_FUNCTIONS = tuple(load_json(file) for file in AGGREGATE_DIR.glob("*.json"))
