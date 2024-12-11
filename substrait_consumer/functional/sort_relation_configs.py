from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
SORT_DIR = CONFIG_DIR / "relation" / "sort"
SORT_RELATION_TESTS = tuple(load_json(file) for file in SORT_DIR.glob("*.json"))
