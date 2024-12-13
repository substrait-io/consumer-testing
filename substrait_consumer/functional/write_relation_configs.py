from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
WRITE_DIR = CONFIG_DIR / "relation" / "write"
WRITE_RELATION_TESTS = tuple(load_json(file) for file in WRITE_DIR.glob("*.json"))
