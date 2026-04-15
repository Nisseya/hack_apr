import json
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
import os

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from helpers import execute_code, get_code
from providers import RunProviderRequest, call_provider_api
from questions import build_benchmark_inputs


app = FastAPI()
ROOT = Path(__file__).resolve().parent
questions = build_benchmark_inputs()


class RunRepoRequest(BaseModel):
    repo_url: str


class GeneratedAnswer(BaseModel):
    id: str
    question: str
    code: str
    generation_duration_seconds: float
    peak_ram_mb: float
    peak_gpu_mb: float


class ExecutedAnswer(BaseModel):
    id: str
    stdout: str
    stderr: str
    success: bool
    execution_duration_seconds: float
    exact_match: bool | None = None
    generated_hash: str | None = None
    gold_hash: str | None = None
    generated_shape: dict | None = None
    gold_shape: dict | None = None
    generated_columns: list[str] | None = None
    gold_columns: list[str] | None = None


class RunRepoResponse(BaseModel):
    generator_pid: int
    repo_dir: str
    url: str
    generated_answers: list[GeneratedAnswer]
    executed_answers: list[ExecutedAnswer]
    generator_logs: str


class RunProviderResponse(BaseModel):
    provider: str
    model: str
    total_generation_duration_seconds: float
    total_execution_duration_seconds: float
    generated_answers: list[GeneratedAnswer]
    executed_answers: list[ExecutedAnswer]


def _with_absolute_dataset_paths(question: dict) -> dict:
    question = dict(question)
    datasets = {}

    for name, dataset in question["datasets"].items():
        dataset = dict(dataset)
        file_path = Path(dataset["file_name"])
        if not file_path.is_absolute():
            dataset["file_name"] = str((ROOT / file_path).resolve())
        datasets[name] = dataset

    question["datasets"] = datasets
    return question


questions = [_with_absolute_dataset_paths(question) for question in questions]


def get_free_port() -> int:
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_until_up(url: str, timeout: float = 180.0) -> None:
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if response.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(1)

    raise HTTPException(status_code=500, detail="Submission server did not start in time")


def install_repo_dependencies(repo_dir: Path) -> None:
    env = os.environ.copy()
    env["TMPDIR"] = "/workspace/tmp"
    print("Installing deps in:", repo_dir)

    if (repo_dir / "pyproject.toml").exists():
        print("Detected uv project")
        result = subprocess.run(
            ["uv", "sync"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
    elif (repo_dir / "requirements.txt").exists():
        print("Detected requirements.txt")

        subprocess.run(["python", "-m", "venv", ".venv"], cwd=repo_dir)

        result = subprocess.run(
            [str(repo_dir / ".venv" / "bin" / "pip"), "install", "-r", "requirements.txt"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
    else:
        print("No dependencies file found")
        return

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception("Dependency install failed")

def start_submission_server(repo_dir: Path, port: int):
    print("Starting server from:", repo_dir)

    log_path = repo_dir / "server.log"

    if (repo_dir / "pyproject.toml").exists():
        cmd = ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", str(port)]
    else:
        python_path = repo_dir / ".venv" / "bin" / "python"
        cmd = [str(python_path), "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", str(port)]

    print("CMD:", " ".join(cmd))

    log_file = open(log_path, "w")

    process = subprocess.Popen(
        cmd,
        cwd=repo_dir,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )

    return process, f"http://127.0.0.1:{port}", log_path


def clone_repo(repo_url: str) -> Path:
    repo_dir = Path(tempfile.mkdtemp(prefix="submission_"))
    result = subprocess.run(
        ["git", "clone", repo_url, str(repo_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        shutil.rmtree(repo_dir, ignore_errors=True)
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )
    return repo_dir


def read_text_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def parse_execution_stdout(stdout: str) -> dict | None:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return None

    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return None


def compare_execution_results(
    question_id: str,
    generated_execution,
    gold_execution,
) -> ExecutedAnswer:
    if not generated_execution.success:
        return ExecutedAnswer(
            id=question_id,
            stdout=generated_execution.stdout,
            stderr=generated_execution.stderr,
            success=False,
            execution_duration_seconds=generated_execution.duration_seconds,
            exact_match=False,
        )

    if not gold_execution.success:
        return ExecutedAnswer(
            id=question_id,
            stdout=generated_execution.stdout,
            stderr=gold_execution.stderr,
            success=False,
            execution_duration_seconds=generated_execution.duration_seconds,
            exact_match=None,
        )

    generated_payload = parse_execution_stdout(generated_execution.stdout)
    gold_payload = parse_execution_stdout(gold_execution.stdout)

    if generated_payload is None or gold_payload is None:
        return ExecutedAnswer(
            id=question_id,
            stdout=generated_execution.stdout,
            stderr=generated_execution.stderr,
            success=False,
            execution_duration_seconds=generated_execution.duration_seconds,
            exact_match=False,
        )

    exact_match = (
        generated_payload["hash"] == gold_payload["hash"]
        and generated_payload["shape"] == gold_payload["shape"]
        and generated_payload["columns"] == gold_payload["columns"]
    )

    return ExecutedAnswer(
        id=question_id,
        stdout=generated_execution.stdout,
        stderr=generated_execution.stderr,
        success=True,
        execution_duration_seconds=generated_execution.duration_seconds,
        exact_match=exact_match,
        generated_hash=generated_payload["hash"],
        gold_hash=gold_payload["hash"],
        generated_shape=generated_payload["shape"],
        gold_shape=gold_payload["shape"],
        generated_columns=generated_payload["columns"],
        gold_columns=gold_payload["columns"],
    )


def generate_answers_from_repo(pid: int, base_url: str) -> list[GeneratedAnswer]:
    generated_answers = []

    for question in questions:
        generation = get_code(
            pid=pid,
            base_url=base_url,
            message=question["question"],
            schema=question["datasets"],
        )
        generated_answers.append(
            GeneratedAnswer(
                id=question["id"],
                question=question["question"],
                code=generation.response_text,
                generation_duration_seconds=generation.duration_seconds,
                peak_ram_mb=generation.peak_ram_mb,
                peak_gpu_mb=generation.peak_gpu_mb,
            )
        )

    return generated_answers


def generate_answers_from_provider(payload: RunProviderRequest) -> tuple[list[GeneratedAnswer], float]:
    generated_answers = []
    total_generation_duration_seconds = 0.0

    for question in questions:
        generation = call_provider_api(
            provider=payload.config.provider,
            model=payload.config.model_name,
            api_key=payload.config.api_key,
            prompt=question["question"],
            schema=question["datasets"],
            temp=payload.config.temperature,
            max_tokens=payload.config.max_tokens,
            extra_system_prompt=payload.config.system_prompt,
        )
        total_generation_duration_seconds += generation.duration_seconds

        generated_answers.append(
            GeneratedAnswer(
                id=question["id"],
                question=question["question"],
                code=generation.response_text,
                generation_duration_seconds=generation.duration_seconds,
                peak_ram_mb=generation.peak_ram_mb,
                peak_gpu_mb=generation.peak_gpu_mb,
            )
        )

    return generated_answers, total_generation_duration_seconds


def execute_answers(generated_answers: list[GeneratedAnswer]) -> tuple[list[ExecutedAnswer], float]:
    executed_answers = []
    total_execution_duration_seconds = 0.0

    for generated_answer, question in zip(generated_answers, questions, strict=True):
        generated_execution = execute_code(
            generated_answer.code,
            question["datasets"],
            ROOT,
        )
        gold_execution = execute_code(
            question["gold_code"],
            question["datasets"],
            ROOT,
        )

        total_execution_duration_seconds += generated_execution.duration_seconds

        executed_answers.append(
            compare_execution_results(
                question["id"],
                generated_execution,
                gold_execution,
            )
        )

    return executed_answers, total_execution_duration_seconds


@app.post("/run-repo", response_model=RunRepoResponse)
def run_repo(payload: RunRepoRequest) -> RunRepoResponse:
    repo_dir = None
    process = None
    log_path = None

    print("\n===== RUN REPO START =====")
    print("Repo URL:", payload.repo_url)

    try:
        print("[1] Cloning repo...")
        repo_dir = clone_repo(payload.repo_url)
        print("Cloned to:", repo_dir)

        print("[2] Installing dependencies...")
        install_repo_dependencies(repo_dir)
        print("Dependencies installed")

        port = get_free_port()
        print("[3] Starting server on port:", port)

        process, base_url, log_path = start_submission_server(repo_dir, port)
        print("Server started PID:", process.pid)
        print("Base URL:", base_url)

        print("[4] Waiting for server...")
        wait_until_up(f"{base_url}/docs")
        print("Server is up")

        print("[5] Generating answers...")
        generated_answers = generate_answers_from_repo(process.pid, base_url)
        print("Generated:", len(generated_answers))

        print("[6] Executing answers...")
        executed_answers, _ = execute_answers(generated_answers)
        print("Execution done")

        logs = read_text_file(log_path)

        print("===== RUN REPO SUCCESS =====\n")

        return RunRepoResponse(
            generator_pid=process.pid,
            repo_dir=str(repo_dir),
            url=base_url,
            generated_answers=generated_answers,
            executed_answers=executed_answers,
            generator_logs=logs,
        )

    except Exception as e:
        print("===== ERROR =====")
        print("Error:", str(e))

        if log_path:
            print("===== SERVER LOGS =====")
            print(read_text_file(log_path))

        raise HTTPException(status_code=500, detail=str(e))

    finally:
        print("[CLEANUP]")
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

        if repo_dir is not None:
            shutil.rmtree(repo_dir, ignore_errors=True)

        print("===== RUN REPO END =====\n")


@app.post("/run-provider-experiment", response_model=RunProviderResponse)
def run_provider_experiment(payload: RunProviderRequest) -> RunProviderResponse:
    try:
        generated_answers, total_generation_duration_seconds = generate_answers_from_provider(payload)
        executed_answers, total_execution_duration_seconds = execute_answers(generated_answers)

        return RunProviderResponse(
            provider=payload.config.provider.value,
            model=payload.config.model_name,
            total_generation_duration_seconds=total_generation_duration_seconds,
            total_execution_duration_seconds=total_execution_duration_seconds,
            generated_answers=generated_answers,
            executed_answers=executed_answers,
        )
    except requests.HTTPError as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=detail)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=str(exc))