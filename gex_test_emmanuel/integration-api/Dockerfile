FROM python:3.13-slim AS builder

WORKDIR /app

# Install system build deps required to compile wheels if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libffi-dev \
        libssl-dev \
        pkg-config \
        git \
        curl \
        ca-certificates \
        cargo && \
    rm -rf /var/lib/apt/lists/*

# Ensure pip, wheel and setuptools are up-to-date, then install uv CLI
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./

# Use uv to create/sync environment from uv.lock (frozen)
RUN uv sync --frozen --no-dev

COPY app ./app
COPY main.py .

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    UV_WORKERS=4

WORKDIR /app

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash appuser

COPY --from=builder /app /app


# Ensure the virtual environment created during the builder stage is
# available on PATH so CLI entrypoints installed into .venv/bin (like
# `uv`) can be executed in the final image.
ENV PATH="/app/.venv/bin:$PATH"

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

CMD ["/app/.venv/bin/python", "-m", "uvicorn", "app.main:app", \
    "--host", "0.0.0.0", \
    "--port", "8080", \
    "--workers", "4", \
    "--loop", "uvloop", \
    "--http", "httptools"]
