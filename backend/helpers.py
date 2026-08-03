import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import requests
from pydantic import BaseModel


class CodeMetrics(BaseModel):
    response_text: str
    duration_seconds: float
    peak_ram_mb: float
    peak_gpu_mb: float


class ExecutionMetrics(BaseModel):
    stdout: str
    stderr: str
    success: bool
    duration_seconds: float


def _get_descendant_pids(pid: int) -> set[int]:
    result = subprocess.run(
        ["ps", "-e", "-o", "pid=,ppid="],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {pid}

    children: dict[int, list[int]] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            child_pid = int(parts[0])
            parent_pid = int(parts[1])
        except ValueError:
            continue
        children.setdefault(parent_pid, []).append(child_pid)

    all_pids = {pid}
    stack = [pid]

    while stack:
        current = stack.pop()
        for child in children.get(current, []):
            if child not in all_pids:
                all_pids.add(child)
                stack.append(child)

    return all_pids


def _get_tree_ram_mb(pid: int) -> float:
    pids = _get_descendant_pids(pid)
    result = subprocess.run(
        ["ps", "-e", "-o", "pid=,rss="],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0.0

    total_kb = 0
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            line_pid = int(parts[0])
            rss_kb = int(parts[1])
        except ValueError:
            continue
        if line_pid in pids:
            total_kb += rss_kb

    return total_kb / 1024


def _get_total_gpu_mb() -> float:
    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=memory.used",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0.0

    total = 0.0
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            total += float(line)
        except ValueError:
            continue
    return total


def get_code(
    pid: int,
    base_url: str,
    message: str,
    schema: dict,
    timeout: float = 200.0,
    sample_interval: float = 0.2,
) -> CodeMetrics:
    result: dict = {}
    error: dict = {}

    def do_request() -> None:
        try:
            response = requests.post(
                f"{base_url}/chat",
                json={"message": message, "tables": schema},
                timeout=timeout,
            )
            result["response"] = response
        except Exception as exc:
            error["exception"] = exc

    start = time.perf_counter()
    thread = threading.Thread(target=do_request)
    thread.start()

    peak_ram_mb = 0.0
    peak_gpu_mb = 0.0

    while thread.is_alive():
        peak_ram_mb = max(peak_ram_mb, _get_tree_ram_mb(pid))
        peak_gpu_mb = max(peak_gpu_mb, _get_total_gpu_mb())
        time.sleep(sample_interval)

    thread.join()

    duration_seconds = time.perf_counter() - start
    peak_ram_mb = max(peak_ram_mb, _get_tree_ram_mb(pid))
    peak_gpu_mb = max(peak_gpu_mb, _get_total_gpu_mb())

    if "exception" in error:
        raise error["exception"]

    response = result["response"]
    response.raise_for_status()
    data = response.json()

    return CodeMetrics(
        response_text=data.get("response", response.text),
        duration_seconds=duration_seconds,
        peak_ram_mb=peak_ram_mb,
        peak_gpu_mb=peak_gpu_mb,
    )


def _load_table_code(name: str, dataset: dict, project_dir: Path) -> str:
    file_path = Path(dataset["file_name"])
    if not file_path.is_absolute():
        file_path = project_dir / file_path

    file_path = file_path.resolve()
    file_format = dataset.get("format", "parquet")

    if file_format == "csv":
        return f'{name} = pl.read_csv({file_path.as_posix()!r})'

    return f'{name} = pl.read_parquet({file_path.as_posix()!r})'


def _build_runner_code(code: str, datasets: dict[str, dict], project_dir: Path) -> str:
    load_tables = "\n".join(
        _load_table_code(name, dataset, project_dir)
        for name, dataset in datasets.items()
    )

    return f"""
import hashlib
import json
import polars as pl

{load_tables}

{code}

if not isinstance(result, pl.DataFrame):
    raise TypeError("result must be a Polars DataFrame")

result = result.select(result.columns)

if result.columns:
    result = result.sort(result.columns)

csv_text = result.write_csv(file=None)
result_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()[:16]

print(json.dumps({{
    "columns": result.columns,
    "shape": {{
        "rows": result.height,
        "cols": result.width,
    }},
    "hash": result_hash,
    "rows": result.to_dicts(),
}}, default=str))
""".strip()


def execute_code(code: str, datasets: dict[str, dict], project_dir: str | Path) -> ExecutionMetrics:
    project_dir = Path(project_dir).resolve()
    wrapped_code = _build_runner_code(code, datasets, project_dir)

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(wrapped_code)
        script_path = f.name

    python_path = Path("/workspace/hack_apr_env/bin/python")

    try:
        start = time.perf_counter()
        result = subprocess.run(
            [str(python_path), script_path],
            capture_output=True,
            text=True,
            cwd=project_dir,
        )
        duration_seconds = time.perf_counter() - start

        return ExecutionMetrics(
            stdout=result.stdout,
            stderr=result.stderr,
            success=result.returncode == 0,
            duration_seconds=duration_seconds,
        )
    finally:
        os.remove(script_path)