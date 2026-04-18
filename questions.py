import json
from pathlib import Path


def load_benchmark(path: str | Path) -> dict:
    benchmark_path = Path(path)
    with benchmark_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_benchmark_inputs(path: str | Path) -> list[dict]:
    benchmark = load_benchmark(path)
    return benchmark["questions"]