"""Orchestrator app for the GPU pool.

The orchestrator is a lightweight FastAPI app that does NOT execute benchmarks
itself. It keeps a pool of warm RunPod GPU pods (which run the regular backend
image as workers) and forwards each submission to a free worker, relaying the
SSE stream back to the frontend.

It also exposes admin endpoints to inspect and manually scale the pool.

Run it with:
    RUNPOD_API_KEY=... RUNPOD_POOL_SIZE=2 uvicorn pool:app --port 8000
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from gpu_pool import GpuPool
from runpod_client import RunPodError

logger = logging.getLogger(__name__)

app = FastAPI(title="hack-apr orchestrator")

# Populated lazily on first use (needs RUNPOD_API_KEY).
_pool: GpuPool | None = None


def get_pool() -> GpuPool:
    global _pool
    if _pool is None:
        _pool = GpuPool()
    return _pool


def _normalize(url: str) -> str:
    return url.rstrip("/")


def _sse_error(message: str) -> str:
    return f"event: error\ndata: {json.dumps({'message': message})}\n\n"


async def _forward_sse(target: str, payload: dict[str, Any]) -> StreamingResponse:
    """POST to a worker and relay its SSE stream to the caller."""
    headers = {"Content-Type": "application/json"}

    async def event_generator():
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream(
                    "POST", target, json=payload, headers=headers
                ) as resp:
                    if resp.status_code != 200:
                        text = await resp.aread()
                        yield _sse_error(text.decode(errors="replace"))
                        return
                    async for line in resp.aiter_lines():
                        if line:
                            yield line + "\n"
        except Exception as exc:  # noqa: BLE001
            yield _sse_error(str(exc))

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _route_to_worker(payload: dict[str, Any], path: str) -> StreamingResponse:
    """Acquire a free worker, forward to it, and release it when done."""
    pool = get_pool()
    try:
        worker = await asyncio.to_thread(pool.acquire_worker)
    except RunPodError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    url = worker.pod.http_url
    if not url:
        pool.release_worker(worker.pod.id)
        raise HTTPException(status_code=503, detail="Worker pod has no public URL")
    target = f"{_normalize(url)}{path}"
    response = await _forward_sse(target, payload)
    original = response.body_iterator

    async def wrapped():
        try:
            async for chunk in original:
                yield chunk
        finally:
            pool.release_worker(worker.pod.id)

    return StreamingResponse(
        wrapped(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---- Forwarded endpoints (same contract as the worker backend) ----

@app.post("/run-repo-stream")
async def run_repo_stream(payload: dict) -> StreamingResponse:
    return await _route_to_worker(payload, "/run-repo-stream")


@app.post("/submit_final")
async def submit_final(payload: dict) -> StreamingResponse:
    return await _route_to_worker(payload, "/submit_final")


# ---- Pool admin ----

@app.get("/pool/health")
def pool_health() -> dict:
    return get_pool().health()


@app.post("/pool/scale")
def pool_scale(body: dict) -> dict:
    """Manually set the target pool size. body: {"size": int}"""
    size = body.get("size")
    if not isinstance(size, int) or size < 0:
        raise HTTPException(status_code=400, detail="size must be a non-negative int")
    pool = get_pool()
    pool.set_target_size(size)
    return {"target_size": pool.target_size, "size": pool.size}


@app.post("/pool/reconcile")
def pool_reconcile() -> dict:
    pool = get_pool()
    pool.reconcile()
    return pool.health()
