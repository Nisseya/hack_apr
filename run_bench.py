import json
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import requests

from helpers import execute_code, get_code
from questions import build_benchmark_inputs

ROOT = Path(__file__).resolve().parent
TMP_ROOT = Path("/workspace/tmp")

BASE_VENV = Path("/workspace/hack_apr_env")
BASE_PYTHON = BASE_VENV / "bin" / "python"

PROTECTED_PACKAGES = {
    "torch",
    "polars",
    "fastapi",
    "uvicorn",
    "pydantic",
    "requests",
    "transformers",
    "accelerate",
}

BENCHMARK_PATH = ROOT / "data" / "benchmark_select.json"


def get_free_port():
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def clone_repo(url):
    repo_dir = Path(tempfile.mkdtemp(dir=TMP_ROOT))
    r = subprocess.run(["git", "clone", url, str(repo_dir)], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return repo_dir


def requirement_name(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    for sep in ("==", ">=", "<=", "~=", "!=", ">", "<", "[", ";"):
        if sep in line:
            line = line.split(sep, 1)[0]
            break
    return line.strip().lower().replace("_", "-") if line else None


def install_deps(repo_dir):
    req = repo_dir / "requirements.txt"
    if not req.exists():
        return

    lines = []
    for l in req.read_text().splitlines():
        name = requirement_name(l)
        if name and name in PROTECTED_PACKAGES:
            continue
        if l.strip():
            lines.append(l)

    if not lines:
        return

    tmp = repo_dir / ".req.txt"
    tmp.write_text("\n".join(lines))

    r = subprocess.run(
        ["uv", "pip", "install", "--python", str(BASE_PYTHON), "-r", str(tmp)],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr)


def start_server(repo_dir, port):
    log = repo_dir / "server.log"

    p = subprocess.Popen(
        [
            str(BASE_PYTHON),
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(port),
        ],
        cwd=repo_dir,
        stdout=open(log, "w"),
        stderr=subprocess.STDOUT,
    )

    url = f"http://127.0.0.1:{port}"

    for _ in range(300):
        if p.poll() is not None:
            raise RuntimeError(log.read_text())
        try:
            if requests.get(url, timeout=0.5).ok:
                return p, url
        except:
            pass
        time.sleep(0.2)

    raise RuntimeError("server timeout")


def load_questions():
    qs = build_benchmark_inputs(BENCHMARK_PATH)
    out = []

    for q in qs:
        datasets = {}
        for k, d in q["datasets"].items():
            path = Path(d["file_name"])
            if not path.is_absolute():
                d["file_name"] = str((ROOT / path).resolve())
            datasets[k] = d
        q["datasets"] = datasets
        out.append(q)

    return out


def run(repo_url):
    repo_dir = None
    process = None

    try:
        repo_dir = clone_repo(repo_url)
        install_deps(repo_dir)

        port = get_free_port()
        process, url = start_server(repo_dir, port)

        questions = load_questions()

        results = []

        for q in questions:
            gen = get_code(
                pid=process.pid,
                base_url=url,
                message=q["question"],
                schema=q["datasets"],
            )

            exec_res = execute_code(gen.response_text, q["datasets"], ROOT)
            gold = execute_code(q["gold_code"], q["datasets"], ROOT)

            results.append({
                "id": q["id"],
                "success": exec_res.success,
                "stdout": exec_res.stdout,
                "stderr": exec_res.stderr,
            })

        print(json.dumps(results, indent=2))

    finally:
        if process:
            process.terminate()
        if repo_dir:
            shutil.rmtree(repo_dir, ignore_errors=True)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("usage: python run_benchmark.py <repo_url>")
        exit(1)

    run(sys.argv[1])