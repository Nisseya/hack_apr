"""
Script to run a normal benchmark (select, filters, joins, window_functions,
aggregations, full_pipeline) on a submitted repo.

Usage:
    python run_benchmark.py <repo_url> [--benchmark <name>]
"""

import argparse
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import requests

from helpers import execute_code, get_code
from questions import build_benchmark_inputs

ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = Path("/workspace")
TMP_ROOT = WORKSPACE_ROOT / "tmp"
HF_HOME = WORKSPACE_ROOT / "hf"
UV_CACHE_DIR = WORKSPACE_ROOT / "uv_cache"

BASE_VENV = WORKSPACE_ROOT / "hack_apr_env"
BASE_PYTHON = BASE_VENV / "bin" / "python"

PROTECTED_PACKAGES = {
    "torch", "polars", "fastapi", "uvicorn", "pydantic",
    "requests", "transformers", "accelerate",
}

PUBLIC_BENCHMARKS = {
    "select": ROOT / "data" / "benchmark_select.json",
    "filters": ROOT / "data" / "benchmark_filters.json",
    "joins": ROOT / "data" / "benchmark_joins.json",
    "window_functions": ROOT / "data" / "benchmark_window_functions.json",
    "aggregations": ROOT / "data" / "benchmark_aggregations.json",
    "full_pipeline": ROOT / "data" / "benchmark_full_pipeline.json",
}


def log(step: str, message: str) -> None:
    print(f"[{step}] {message}", flush=True)


def ensure_workspace_dirs() -> None:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    HF_HOME.mkdir(parents=True, exist_ok=True)
    UV_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def uv_env() -> dict[str, str]:
    import os
    env = dict(**os.environ)
    env["TMPDIR"] = str(TMP_ROOT)
    env["HF_HOME"] = str(HF_HOME)
    env["UV_CACHE_DIR"] = str(UV_CACHE_DIR)
    env["HF_HUB_DISABLE_XET"] = "1"
    return env


def get_free_port() -> int:
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def load_questions(path: Path) -> list[dict]:
    raw = build_benchmark_inputs(path)
    result = []
    for q in raw:
        q = dict(q)
        datasets = {}
        for name, dataset in q["datasets"].items():
            item = dict(dataset)
            file_path = Path(item["file_name"])
            if not file_path.is_absolute():
                item["file_name"] = str((ROOT / file_path).resolve())
            datasets[name] = item
        q["datasets"] = datasets
        result.append(q)
    return result


def clone_repo(repo_url: str) -> Path:
    ensure_workspace_dirs()
    repo_dir = Path(tempfile.mkdtemp(prefix="submission_", dir=TMP_ROOT))
    result = subprocess.run(
        ["git", "clone", repo_url, str(repo_dir)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        shutil.rmtree(repo_dir, ignore_errors=True)
        raise RuntimeError(result.stderr.strip() or "git clone failed")
    return repo_dir


def normalize(name: str) -> str:
    return name.strip().lower().replace("_", "-").replace(".", "-")


def req_name(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith(("-", "git+", "http://", "https://")):
        return None
    for sep in ("==", ">=", "<=", "~=", "!=", ">", "<", "[", ";"):
        if sep in line:
            line = line.split(sep, 1)[0]
            break
    line = line.strip()
    return normalize(line) if line else None


def install_deps(repo_dir: Path) -> Path:
    ensure_workspace_dirs()
    requirements = repo_dir / "requirements.txt"
    pyproject = repo_dir / "pyproject.toml"

    if pyproject.exists():
        content = pyproject.read_text(encoding="utf-8", errors="replace").lower()
        blocked = [n for n in PROTECTED_PACKAGES if f'"{n}' in content or f"'{n}" in content]
        if blocked:
            raise RuntimeError(f"Protected packages in pyproject.toml: {', '.join(blocked)}")
        raise RuntimeError("pyproject.toml is not supported, use requirements.txt")

    if not requirements.exists():
        return BASE_VENV

    # filter protected packages
    kept = []
    blocked = []
    for raw_line in requirements.read_text(encoding="utf-8", errors="replace").splitlines():
        name = req_name(raw_line)
        if name and name in PROTECTED_PACKAGES:
            blocked.append(name)
            continue
        if raw_line.strip():
            kept.append(raw_line)

    if blocked:
        raise RuntimeError(f"Protected packages in requirements.txt: {', '.join(set(blocked))}")

    if kept:
        filtered = repo_dir / ".filtered_requirements.txt"
        filtered.write_text("\n".join(kept) + "\n", encoding="utf-8")
        result = subprocess.run(
            ["uv", "pip", "install", "--python", str(BASE_PYTHON), "-r", str(filtered)],
            cwd=repo_dir, capture_output=True, text=True, env=uv_env(),
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())

    return BASE_VENV


def start_server(repo_dir: Path, env_dir: Path, port: int):
    log_path = repo_dir / "server.log"
    python_path = env_dir / "bin" / "python"
    cmd = [
        str(python_path), "-m", "uvicorn", "main:app",
        "--host", "0.0.0.0", "--port", str(port),
        "--timeout-keep-alive", "1200",
    ]
    log_file = open(log_path, "w")
    process = subprocess.Popen(
        cmd, cwd=repo_dir, stdout=log_file, stderr=subprocess.STDOUT,
        env=uv_env(), start_new_session=True,
    )
    log_file.close()
    return process, f"http://127.0.0.1:{port}", log_path


def wait_until_up(process, url: str, log_path: Path, timeout: float = 3600.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            logs = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
            raise RuntimeError(logs or "Submission server exited before startup")
        try:
            if requests.get(url, timeout=0.5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("Submission server did not start in time")


def parse_stdout(stdout: str) -> dict | None:
    import json
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return None
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return None


def compare(gen_exec, gold_exec) -> tuple[bool, bool | None]:
    """Returns (success, exact_match)."""
    if not gen_exec.success:
        return False, False
    if not gold_exec.success:
        return False, None
    gen = parse_stdout(gen_exec.stdout)
    gold = parse_stdout(gold_exec.stdout)
    if gen is None or gold is None:
        return False, False
    match = (gen["hash"] == gold["hash"]
             and gen["shape"] == gold["shape"]
             and gen["columns"] == gold["columns"])
    return True, match


def run_benchmark(repo_url: str, benchmark_name: str) -> None:
    path = PUBLIC_BENCHMARKS.get(benchmark_name)
    if path is None:
        raise ValueError(f"Invalid benchmark. Allowed: {', '.join(PUBLIC_BENCHMARKS)}")

    questions = load_questions(path)
    total = len(questions)

    repo_dir = None
    process = None

    try:
        log("cloning", f"Cloning {repo_url}")
        repo_dir = clone_repo(repo_url)

        log("preparing_env", "Installing dependencies")
        env_dir = install_deps(repo_dir)

        port = get_free_port()
        log("starting_server", f"Starting server on port {port}")
        process, base_url, log_path = start_server(repo_dir, env_dir, port)
        wait_until_up(process, f"{base_url}/", log_path)
        log("server_ready", "Submission server ready")

        correct = 0
        for index, question in enumerate(questions, start=1):
            log("question_started", f"[{index}/{total}] {question['id']}: {question['question'][:80]}")

            gen = get_code(
                pid=process.pid, base_url=base_url,
                message=question["question"], schema=question["datasets"],
            )
            gen_exec = execute_code(gen.response_text, question["datasets"], ROOT)
            gold_exec = execute_code(question["gold_code"], question["datasets"], ROOT)
            success, exact_match = compare(gen_exec, gold_exec)

            if success and exact_match:
                correct += 1
                status = "OK"
            elif success:
                status = "WRONG"
            else:
                status = "FAIL"

            log(
                "question_done",
                f"[{index}/{total}] {question['id']} -> {status} "
                f"(gen={gen.duration_seconds:.1f}s, ram={gen.peak_ram_mb:.0f}MB, "
                f"gpu={gen.peak_gpu_mb:.0f}MB) | correct so far: {correct}/{index}",
            )

        log("done", f"Benchmark '{benchmark_name}' completed: {correct}/{total} correct")

    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if repo_dir is not None:
            shutil.rmtree(repo_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a public benchmark on a repo.")
    parser.add_argument("repo_url", help="Git repo URL to clone and test")
    parser.add_argument(
        "--benchmark", "-b", default="select",
        choices=list(PUBLIC_BENCHMARKS.keys()),
        help="Benchmark to run (default: select)",
    )
    args = parser.parse_args()

    try:
        run_benchmark(args.repo_url, args.benchmark)
    except Exception as exc:
        log("error", str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()