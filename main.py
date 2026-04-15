import json
import shlex
import socket
import subprocess
import time

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from helpers import execute_code, get_code
from providers import RunProviderRequest, call_provider_api
from questions import build_benchmark_inputs


app = FastAPI()
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
    generator_container_id: str
    executor_container_id: str
    url: str
    generated_answers: list[GeneratedAnswer]
    executed_answers: list[ExecutedAnswer]
    generator_logs: str
    executor_logs: str


class RunProviderResponse(BaseModel):
    provider: str
    model: str
    total_generation_duration_seconds: float
    total_execution_duration_seconds: float
    generated_answers: list[GeneratedAnswer]
    executed_answers: list[ExecutedAnswer]


def get_free_port() -> int:
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_until_up(url: str, timeout: float = 3600.0) -> None:
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if response.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(1)

    raise HTTPException(status_code=500, detail="Container server did not start in time")


def run_executor_container() -> str:
    result = subprocess.run(
        ["docker", "run", "-d", "polars-executor:latest"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )
    return result.stdout.strip()


def run_generator_container(repo_url: str, port: int) -> tuple[str, str]:
    base_url = f"http://127.0.0.1:{port}"
    escaped_repo_url = shlex.quote(repo_url)

    result = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--gpus",
            "all",
            "-p",
            f"{port}:8000",
            "gpu-fastapi-base:cu121",
            "sh",
            "-lc",
            (
                f"git clone {escaped_repo_url} /app && "
                "cd /app && "
                "uv pip install --system -r requirements.txt && "
                "uvicorn main:app --host 0.0.0.0 --port 8000"
            ),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )

    container_id = result.stdout.strip()
    wait_until_up(f"{base_url}/docs")
    return container_id, base_url


def get_container_logs(container_id: str) -> str:
    result = subprocess.run(
        ["docker", "logs", container_id],
        capture_output=True,
        text=True,
    )
    return (result.stdout + "\n" + result.stderr).strip()


def remove_container(container_id: str | None) -> None:
    if not container_id:
        return
    subprocess.run(
        ["docker", "rm", "-f", container_id],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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
            stderr=gold_execution.stderr or generated_execution.stderr,
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
        and generated_payload["columns"] == gold_payload["columns"]
        and generated_payload["shape"] == gold_payload["shape"]
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


def generate_answers_from_repo(
    generator_container_id: str,
    base_url: str,
) -> list[GeneratedAnswer]:
    answers = []

    for question in questions:
        generation = get_code(
            container_id=generator_container_id,
            base_url=base_url,
            message=question["question"],
            schema=question["datasets"],
        )
        answers.append(
            GeneratedAnswer(
                id=question["id"],
                question=question["question"],
                code=generation.response_text,
                generation_duration_seconds=generation.duration_seconds,
                peak_ram_mb=generation.peak_ram_mb,
                peak_gpu_mb=generation.peak_gpu_mb,
            )
        )

    return answers


def generate_answers_from_provider(payload: RunProviderRequest) -> tuple[list[GeneratedAnswer], float]:
    answers = []
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

        answers.append(
            GeneratedAnswer(
                id=question["id"],
                question=question["question"],
                code=generation.response_text,
                generation_duration_seconds=generation.duration_seconds,
                peak_ram_mb=generation.peak_ram_mb,
                peak_gpu_mb=generation.peak_gpu_mb,
            )
        )

    return answers, total_generation_duration_seconds


def execute_answers(
    executor_container_id: str,
    generated_answers: list[GeneratedAnswer],
) -> tuple[list[ExecutedAnswer], float]:
    executed_answers = []
    total_execution_duration_seconds = 0.0

    for generated_answer, question in zip(generated_answers, questions, strict=True):
        generated_execution = execute_code(
            executor_container_id,
            generated_answer.code,
            question["datasets"],
        )
        gold_execution = execute_code(
            executor_container_id,
            question["gold_code"],
            question["datasets"],
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
    generator_container_id = None
    executor_container_id = None

    try:
        port = get_free_port()
        generator_container_id, base_url = run_generator_container(payload.repo_url, port)
        executor_container_id = run_executor_container()

        generated_answers = generate_answers_from_repo(generator_container_id, base_url)
        executed_answers, _ = execute_answers(executor_container_id, generated_answers)

        return RunRepoResponse(
            generator_container_id=generator_container_id,
            executor_container_id=executor_container_id,
            url=base_url,
            generated_answers=generated_answers,
            executed_answers=executed_answers,
            generator_logs=get_container_logs(generator_container_id),
            executor_logs=get_container_logs(executor_container_id),
        )
    except requests.RequestException:
        raise HTTPException(status_code=500, detail="Request to generator container failed")
    finally:
        remove_container(generator_container_id)
        remove_container(executor_container_id)


@app.post("/run-provider-experiment", response_model=RunProviderResponse)
def run_provider_experiment(payload: RunProviderRequest) -> RunProviderResponse:
    executor_container_id = None

    try:
        executor_container_id = run_executor_container()

        generated_answers, total_generation_duration_seconds = generate_answers_from_provider(payload)
        executed_answers, total_execution_duration_seconds = execute_answers(
            executor_container_id,
            generated_answers,
        )

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
    finally:
        remove_container(executor_container_id)