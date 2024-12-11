from pathlib import Path

from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent
PROJECT_DIR = CONFIG_DIR / "relation" / "project"
PROJECT_RELATION_TESTS = tuple(load_json(file) for file in PROJECT_DIR.glob("*.json"))
