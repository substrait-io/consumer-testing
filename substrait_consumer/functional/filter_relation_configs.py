from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
FILTER_DIR = CONFIG_DIR / "relation" / "filter"
FILTER_RELATION_TESTS = tuple(load_json(file) for file in FILTER_DIR.glob("*.json"))
