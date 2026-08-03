# GPU worker image for the hack-apr pool.
#
# This image pre-installs the base environment (torch, transformers, polars,
# ...) so that worker pods start warm: heavy deps are already in the image,
# and models are cached on the persistent volume, avoiding re-download on
# every submission.
#
# Build & push once, then reference it via RUNPOD_POOL_IMAGE.

FROM python:3.12-slim

# System deps for building/installing packages and git clone
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Base environment lives under /workspace (matches backend expectations)
ENV WORKSPACE_ROOT=/workspace
ENV BASE_VENV=/workspace/hack_apr_env
ENV HF_HOME=/workspace/hf
ENV UV_CACHE_DIR=/workspace/uv_cache

WORKDIR /app

# Copy application code first (needed for pyproject/uv.lock)
COPY backend/pyproject.toml backend/uv.lock ./

# Create the base venv and pre-install the heavy deps so pods start warm
RUN uv venv "$BASE_VENV" --python 3.12 && \
    uv pip install --python "$BASE_VENV" \
        torch \
        transformers \
        accelerate \
        polars \
        fastapi \
        uvicorn \
        pydantic \
        requests

# Copy application code
COPY backend/ .

# Scratch dirs
RUN mkdir -p /workspace/tmp /workspace/hf /workspace/uv_cache

EXPOSE 8000

CMD ["/workspace/hack_apr_env/bin/python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
