import asyncio
import hashlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from helpers import execute_code, get_code
from providers import RunProviderRequest, call_provider_api
from questions import build_benchmark_inputs

app = FastAPI()

ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = Path("/workspace")
TMP_ROOT = WORKSPACE_ROOT / "tmp"
HF_HOME = WORKSPACE_ROOT / "hf"
UV_CACHE_DIR = WORKSPACE_ROOT / "uv_cache"
VENVS_ROOT = WORKSPACE_ROOT / "venvs"
BASE_VENV = VENVS_ROOT / "base"
BASE_PYTHON = BASE_VENV / "bin" / "python"
CACHE_VENVS_ROOT = VENVS_ROOT / "cache"

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
    env_dir: str
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


def ensure_workspace_dirs() -> None:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    HF_HOME.mkdir(parents=True, exist_ok=True)
    UV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    VENVS_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_VENVS_ROOT.mkdir(parents=True, exist_ok=True)


def uv_env() -> dict[str, str]:
    env = os.environ.copy()
    env["TMPDIR"] = str(TMP_ROOT)
    env["HF_HOME"] = str(HF_HOME)
    env["UV_CACHE_DIR"] = str(UV_CACHE_DIR)
    env["UV_LINK_MODE"] = "copy"
    return env


def get_free_port() -> int:
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def read_text_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def clone_repo(repo_url: str) -> Path:
    ensure_workspace_dirs()
    repo_dir = Path(tempfile.mkdtemp(prefix="submission_", dir=TMP_ROOT))
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


def dependency_fingerprint(repo_dir: Path) -> str:
    requirements = repo_dir / "requirements.txt"
    pyproject = repo_dir / "pyproject.toml"
    lock = repo_dir / "uv.lock"

    hasher = hashlib.sha256()

    if requirements.exists():
        hasher.update(b"requirements.txt\n")
        hasher.update(requirements.read_bytes())

    if pyproject.exists():
        hasher.update(b"pyproject.toml\n")
        hasher.update(pyproject.read_bytes())

    if lock.exists():
        hasher.update(b"uv.lock\n")
        hasher.update(lock.read_bytes())

    digest = hasher.hexdigest()
    return digest if digest else "no_deps"


def ensure_base_env() -> None:
    ensure_workspace_dirs()

    if BASE_PYTHON.exists():
        return

    result = subprocess.run(
        ["uv", "venv", str(BASE_VENV)],
        capture_output=True,
        text=True,
        env=uv_env(),
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )

    result = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(BASE_PYTHON),
            "fastapi",
            "uvicorn",
            "polars",
            "pandas",
            "numpy",
            "requests",
        ],
        capture_output=True,
        text=True,
        env=uv_env(),
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )


def ensure_cached_env(repo_dir: Path) -> Path:
    ensure_base_env()

    env_dir = CACHE_VENVS_ROOT / dependency_fingerprint(repo_dir)
    python_path = env_dir / "bin" / "python"

    if python_path.exists():
        return env_dir

    result = subprocess.run(
        [
            str(BASE_PYTHON),
            "-m",
            "venv",
            "--system-site-packages",
            str(env_dir),
        ],
        capture_output=True,
        text=True,
        env=uv_env(),
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )

    requirements = repo_dir / "requirements.txt"
    pyproject = repo_dir / "pyproject.toml"
    lock = repo_dir / "uv.lock"

    if requirements.exists():
        result = subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python_path),
                "-r",
                str(requirements),
            ],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            env=uv_env(),
        )
        if result.returncode != 0:
            shutil.rmtree(env_dir, ignore_errors=True)
            raise HTTPException(
                status_code=500,
                detail=result.stderr.strip() or result.stdout.strip(),
            )
        return env_dir

    if pyproject.exists():
        env = uv_env()
        env["UV_PROJECT_ENVIRONMENT"] = str(env_dir)

        cmd = ["uv", "sync"]
        if lock.exists():
            cmd.append("--frozen")

        result = subprocess.run(
            cmd,
            cwd=repo_dir,
            capture_output=True,
            text=True,
            env=env,
        )
        if result.returncode != 0:
            shutil.rmtree(env_dir, ignore_errors=True)
            raise HTTPException(
                status_code=500,
                detail=result.stderr.strip() or result.stdout.strip(),
            )

    return env_dir


def install_repo_dependencies(repo_dir: Path) -> Path:
    return ensure_cached_env(repo_dir)


def start_submission_server(repo_dir: Path, env_dir: Path, port: int):
    log_path = repo_dir / "server.log"
    python_path = env_dir / "bin" / "python"
    cmd = [
        str(python_path),
        "-m",
        "uvicorn",
        "main:app",
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
    ]

    log_file = open(log_path, "w")

    process = subprocess.Popen(
        cmd,
        cwd=repo_dir,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )

    log_file.close()
    return process, f"http://127.0.0.1:{port}", log_path


def wait_until_up(process: subprocess.Popen, url: str, log_path: Path, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout

    while time.time() < deadline:
        if process.poll() is not None:
            logs = read_text_file(log_path)
            raise HTTPException(
                status_code=500,
                detail=logs or "Submission server exited before startup",
            )

        try:
            response = requests.get(url, timeout=0.5)
            if response.ok:
                return
        except requests.RequestException:
            pass

        time.sleep(0.3)

    logs = read_text_file(log_path)
    raise HTTPException(
        status_code=500,
        detail=logs or "Submission server did not start in time",
    )


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


def run_single_question(process: subprocess.Popen, base_url: str, question: dict) -> tuple[GeneratedAnswer, ExecutedAnswer]:
    generation = get_code(
        pid=process.pid,
        base_url=base_url,
        message=question["question"],
        schema=question["datasets"],
    )

    generated_answer = GeneratedAnswer(
        id=question["id"],
        question=question["question"],
        code=generation.response_text,
        generation_duration_seconds=generation.duration_seconds,
        peak_ram_mb=generation.peak_ram_mb,
        peak_gpu_mb=generation.peak_gpu_mb,
    )

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

    executed_answer = compare_execution_results(
        question["id"],
        generated_execution,
        gold_execution,
    )

    return generated_answer, executed_answer


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


def sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.post("/run-repo", response_model=RunRepoResponse)
def run_repo(payload: RunRepoRequest) -> RunRepoResponse:
    repo_dir = None
    env_dir = None
    process = None
    log_path = None

    try:
        repo_dir = clone_repo(payload.repo_url)
        env_dir = install_repo_dependencies(repo_dir)

        port = get_free_port()
        process, base_url, log_path = start_submission_server(repo_dir, env_dir, port)
        wait_until_up(process, f"{base_url}/", log_path)

        generated_answers = generate_answers_from_repo(process.pid, base_url)
        executed_answers, _ = execute_answers(generated_answers)
        logs = read_text_file(log_path)

        return RunRepoResponse(
            generator_pid=process.pid,
            repo_dir=str(repo_dir),
            env_dir=str(env_dir),
            url=base_url,
            generated_answers=generated_answers,
            executed_answers=executed_answers,
            generator_logs=logs,
        )

    except HTTPException:
        raise
    except Exception as e:
        logs = read_text_file(log_path) if log_path else ""
        raise HTTPException(status_code=500, detail=logs or str(e))
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

        if repo_dir is not None:
            shutil.rmtree(repo_dir, ignore_errors=True)


@app.post("/run-repo-stream")
async def run_repo_stream(payload: RunRepoRequest):
    async def event_generator():
        repo_dir = None
        env_dir = None
        process = None
        log_path = None
        base_url = None

        try:
            yield sse_event("status", {"step": "cloning", "message": "Cloning repository"})
            repo_dir = clone_repo(payload.repo_url)

            yield sse_event("status", {"step": "preparing_env", "message": "Preparing environment"})
            env_dir = install_repo_dependencies(repo_dir)

            port = get_free_port()
            yield sse_event("status", {"step": "starting_server", "message": f"Starting server on port {port}"})
            process, base_url, log_path = start_submission_server(repo_dir, env_dir, port)

            wait_until_up(process, f"{base_url}/", log_path)
            yield sse_event("status", {"step": "server_ready", "message": "Submission server is ready"})

            for question in questions:
                yield sse_event(
                    "status",
                    {
                        "step": "question_started",
                        "question_id": question["id"],
                        "question": question["question"],
                    },
                )

                generated_answer, executed_answer = run_single_question(process, base_url, question)

                yield sse_event(
                    "question_result",
                    {
                        "question_id": question["id"],
                        "question": question["question"],
                        "generated_answer": generated_answer.model_dump(),
                        "executed_answer": executed_answer.model_dump(),
                    },
                )

                await asyncio.sleep(0)

            logs = read_text_file(log_path) if log_path else ""
            yield sse_event(
                "done",
                {
                    "success": True,
                    "repo_dir": str(repo_dir) if repo_dir else None,
                    "env_dir": str(env_dir) if env_dir else None,
                    "generator_pid": process.pid if process else None,
                    "url": base_url,
                    "generator_logs": logs,
                },
            )

        except Exception as e:
            logs = read_text_file(log_path) if log_path else ""
            yield sse_event(
                "error",
                {
                    "message": str(e),
                    "generator_logs": logs,
                },
            )
        finally:
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

            if repo_dir is not None:
                shutil.rmtree(repo_dir, ignore_errors=True)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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