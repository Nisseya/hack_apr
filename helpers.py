import json
import os
import re
import subprocess
import tempfile
import threading
import time

from pydantic import BaseModel
import requests


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


def _parse_mem_to_mb(value: str) -> float:
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", value.strip())
    if not match:
        return 0.0

    number = float(match.group(1))
    unit = match.group(2)

    factors = {
        "B": 1 / (1024 * 1024),
        "KiB": 1 / 1024,
        "MiB": 1,
        "GiB": 1024,
        "Gi": 1024,
        "TiB": 1024 * 1024,
    }
    return number * factors.get(unit, 0.0)


def _get_container_ram_mb(container_id: str) -> float:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container_id],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0.0

    output = result.stdout.strip()
    if not output:
        return 0.0

    used = output.split("/")[0].strip()
    return _parse_mem_to_mb(used)


def _get_container_pids(container_id: str) -> set[int]:
    result = subprocess.run(
        ["docker", "top", container_id, "-eo", "pid"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return set()

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) <= 1:
        return set()

    pids = set()
    for line in lines[1:]:
        try:
            pids.add(int(line))
        except ValueError:
            pass
    return pids


def _get_container_gpu_mb(container_id: str) -> float:
    pids = _get_container_pids(container_id)
    if not pids:
        return 0.0

    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,used_gpu_memory",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0.0

    total = 0.0
    for line in result.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
            used_mb = float(parts[1])
        except ValueError:
            continue
        if pid in pids:
            total += used_mb

    return total


def get_code(
    container_id: str,
    base_url: str,
    message: str,
    schema: dict,
    timeout: float = 30.0,
    sample_interval: float = 0.2,
) -> CodeMetrics:
    result: dict = {}
    error: dict = {}

    def do_request() -> None:
        try:
            response = requests.post(
                f"{base_url}/chat",
                json={"message": message, "schema": schema},
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
        peak_ram_mb = max(peak_ram_mb, _get_container_ram_mb(container_id))
        peak_gpu_mb = max(peak_gpu_mb, _get_container_gpu_mb(container_id))
        time.sleep(sample_interval)

    thread.join()

    duration_seconds = time.perf_counter() - start
    peak_ram_mb = max(peak_ram_mb, _get_container_ram_mb(container_id))
    peak_gpu_mb = max(peak_gpu_mb, _get_container_gpu_mb(container_id))

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


def _build_load_table_code(name: str, dataset: dict) -> str:
    file_name = dataset["file_name"]
    file_format = dataset.get("format", "parquet")

    if file_format == "csv":
        return f'{name} = pl.read_csv("{file_name}")'

    return f'{name} = pl.read_parquet("{file_name}")'


def _build_runner_code(code: str, datasets: dict[str, dict]) -> str:
    load_tables = "\n".join(
        _build_load_table_code(name, dataset)
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

payload = {{
    "columns": result.columns,
    "shape": {{
        "rows": result.height,
        "cols": result.width,
    }},
    "hash": result_hash,
    "rows": result.to_dicts(),
}}

print(json.dumps(payload, default=str))
""".strip()


def execute_code(container_id: str, code: str, datasets: dict[str, dict]) -> ExecutionMetrics:
    wrapped_code = _build_runner_code(code, datasets)

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(wrapped_code)
        host_path = f.name

    container_path = f"/tmp/{os.path.basename(host_path)}"

    try:
        copy_result = subprocess.run(
            ["docker", "cp", host_path, f"{container_id}:{container_path}"],
            capture_output=True,
            text=True,
        )
        if copy_result.returncode != 0:
            return ExecutionMetrics(
                stdout="",
                stderr=copy_result.stderr,
                success=False,
                duration_seconds=0.0,
            )

        start = time.perf_counter()
        run_result = subprocess.run(
            ["docker", "exec", container_id, "python", container_path],
            capture_output=True,
            text=True,
        )
        duration_seconds = time.perf_counter() - start

        return ExecutionMetrics(
            stdout=run_result.stdout,
            stderr=run_result.stderr,
            success=run_result.returncode == 0,
            duration_seconds=duration_seconds,
        )
    finally:
        os.remove(host_path)