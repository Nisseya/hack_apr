FROM python:3.12-slim

# System deps for building/installing packages and git clone
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install project dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Copy application code
COPY . .

# The backend expects a base venv + scratch dirs under /workspace
ENV WORKSPACE_ROOT=/workspace
RUN mkdir -p /workspace/tmp /workspace/hf /workspace/uv_cache

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
