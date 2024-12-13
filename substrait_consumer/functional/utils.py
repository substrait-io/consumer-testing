import json
from pathlib import Path


def load_json(path: Path) -> str:
    with open(path, "r") as f:
        data = json.load(f)
    data["sql_query"] = (data["sql_query"]["query"], data["sql_query"]["producers"])
    return data
