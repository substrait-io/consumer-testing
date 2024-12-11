from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
JOIN_DIR = CONFIG_DIR / "relation" / "join"
JOIN_RELATION_TESTS = tuple(load_json(file) for file in JOIN_DIR.glob("*.json"))
