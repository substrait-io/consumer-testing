import json
from pathlib import Path


def load_json(path: Path) -> str:
    with open(path, "r") as f:
        data = json.load(f)
    sql_query = data["sql_query"]
    if "query" in sql_query:
        query = sql_query["query"]
    else:
        with open(path.parent / sql_query["query_path"], "r") as f:
            query = f.read()
    data["sql_query"] = (query, sql_query["producers"])
    return data
