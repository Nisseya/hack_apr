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
    print(f"[1] Cloning repo: {url}")
    repo_dir = Path(tempfile.mkdtemp(dir=TMP_ROOT))
    r = subprocess.run(["git", "clone", url, str(repo_dir)], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    print(f"[1] Repo cloned -> {repo_dir}")
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
        print("[2] No requirements.txt")
        return

    print("[2] Installing dependencies")

    lines = []
    for l in req.read_text().splitlines():
        name = requirement_name(l)
        if name and name in PROTECTED_PACKAGES:
            print(f"[2] Skipping protected package: {name}")
            continue
        if l.strip():
            lines.append(l)

    if not lines:
        print("[2] No installable dependencies")
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

    print("[2] Dependencies installed")


def start_server(repo_dir, port):
    print(f"[3] Starting server on port {port}")
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
                print(f"[3] Server ready at {url}")
                return p, url
        except:
            pass
        time.sleep(0.2)

    raise RuntimeError("server timeout")


def load_questions():
    print("[4] Loading questions")
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

    print(f"[4] Loaded {len(out)} questions")
    return out


def parse_stdout(stdout):
    lines = [l.strip() for l in stdout.splitlines() if l.strip()]
    if not lines:
        return None
    try:
        return json.loads(lines[-1])
    except:
        return None


def compute_score(generated, executed):
    correct = sum(1 for e in executed if e["success"] and e["exact"])
    if correct == 0:
        return 0.0

    total_gen = sum(g["gen_time"] for g in generated)
    avg_gpu = sum(g["gpu"] for g in generated) / len(generated)
    avg_ram = sum(g["ram"] for g in generated) / len(generated)

    denom = max(total_gen, 1e-6) * max(avg_gpu, 1e-6) ** 0.1 * max(avg_ram, 1e-6) ** 0.01
    return round(correct / denom, 6)


def run(repo_url):
    repo_dir = None
    process = None

    try:
        repo_dir = clone_repo(repo_url)
        install_deps(repo_dir)

        port = get_free_port()
        process, url = start_server(repo_dir, port)

        questions = load_questions()

        generated = []
        executed = []

        for i, q in enumerate(questions, 1):
            print(f"[5] Question {i}/{len(questions)}: {q['id']}")

            gen = get_code(
                pid=process.pid,
                base_url=url,
                message=q["question"],
                schema=q["datasets"],
            )

            print(f"[5] Generated in {gen.duration_seconds:.2f}s")

            generated.append({
                "gen_time": gen.duration_seconds,
                "ram": gen.peak_ram_mb,
                "gpu": gen.peak_gpu_mb,
            })

            gen_exec = execute_code(gen.response_text, q["datasets"], ROOT)
            gold_exec = execute_code(q["gold_code"], q["datasets"], ROOT)

            gen_payload = parse_stdout(gen_exec.stdout)
            gold_payload = parse_stdout(gold_exec.stdout)

            exact = False
            if gen_payload and gold_payload:
                exact = (
                    gen_payload["hash"] == gold_payload["hash"]
                    and gen_payload["shape"] == gold_payload["shape"]
                    and gen_payload["columns"] == gold_payload["columns"]
                )

            print(f"[5] success={gen_exec.success} exact={exact}")

            executed.append({
                "id": q["id"],
                "success": gen_exec.success,
                "exact": exact,
            })

        score = compute_score(generated, executed)

        correct = sum(1 for e in executed if e["success"] and e["exact"])

        print("\n=== FINAL ===")
        print(f"Score   : {score}")
        print(f"Correct : {correct}/{len(executed)}")

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