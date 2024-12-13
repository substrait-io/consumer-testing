from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
READ_DIR = CONFIG_DIR / "relation" / "read"
READ_RELATION_TESTS = tuple(load_json(file) for file in READ_DIR.glob("*.json"))
