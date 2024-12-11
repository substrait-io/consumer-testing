from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
SET_DIR = CONFIG_DIR / "relation" / "set"
SET_RELATION_TESTS = tuple(load_json(file) for file in SET_DIR.glob("*.json"))
