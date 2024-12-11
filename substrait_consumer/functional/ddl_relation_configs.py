from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
DDL_DIR = CONFIG_DIR / "relation" / "ddl"
DDL_RELATION_TESTS = tuple(load_json(file) for file in DDL_DIR.glob("*.json"))
