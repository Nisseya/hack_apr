import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BENCHMARK_PATH = ROOT / "data" / "benchmark_sql.json"


def load_benchmark() -> dict:
    with BENCHMARK_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def find_table_schema(table_name: str, schemas: dict) -> dict | None:
    for schema_name, schema_data in schemas.items():
        table = schema_data.get("tables", {}).get(table_name)
        if table is None:
            continue
        return {
            "database": schema_name,
            "database_description": schema_data.get("description"),
            "source": schema_data.get("source"),
            "table_description": table.get("description"),
            "columns_description": table.get("columns", {}),
            "foreign_keys": schema_data.get("foreign_keys", []),
        }
    return None


def build_table_context(table_name: str, datasets: dict, schemas: dict) -> dict:
    dataset = datasets[table_name]
    return {
        "table_name": table_name,
        "file_name": dataset["file"],
        "format": "parquet",
        "columns": dataset.get("columns", []),
        "schema": dataset.get("dtypes", {}),
        "rows": dataset.get("rows"),
        "cols": dataset.get("cols"),
        "sql_schema": find_table_schema(table_name, schemas),
    }


def build_benchmark_inputs() -> list[dict]:
    benchmark = load_benchmark()
    datasets = benchmark["datasets"]
    schemas = benchmark.get("schemas", {})

    items = []
    for question in benchmark["questions"]:
        table_names = [
            name.strip()
            for name in question["tables_used"].split(",")
            if name.strip()
        ]

        items.append(
            {
                "id": question["id"],
                "question": question["question_natural_language"],
                "category": question["category"],
                "difficulty": question["difficulty"],
                "datasets": {
                    table_name: build_table_context(table_name, datasets, schemas)
                    for table_name in table_names
                },
                "gold_code": question["gold_code"],
            }
        )

    return items