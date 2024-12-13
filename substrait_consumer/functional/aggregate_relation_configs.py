from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
AGGREGATE_DIR = CONFIG_DIR / "relation" / "aggregate"
AGGREGATE_RELATION_TESTS = tuple(
    load_json(file) for file in AGGREGATE_DIR.glob("*.json")
)
