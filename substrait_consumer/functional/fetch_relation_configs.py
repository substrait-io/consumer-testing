from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
FETCH_DIR = CONFIG_DIR / "relation" / "fetch"
FETCH_RELATION_TESTS = tuple(load_json(file) for file in FETCH_DIR.glob("*.json"))
