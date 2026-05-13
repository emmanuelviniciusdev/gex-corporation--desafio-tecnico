FROM python:3.13-slim AS builder

WORKDIR /app

# Keep base small; install only what we need to build wheels if any dep requires it
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libffi-dev \
        libssl-dev \
        pkg-config \
        cargo \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Ensure pip tooling is up-to-date and install uv
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uv

# Copy lockfile and pyproject, then create a venv with pinned deps
COPY pyproject.toml uv.lock ./
# Allow lock refresh so new dependencies (e.g. cryptography) are resolved
RUN uv sync --no-dev

# Copy application code
# Copy application code into a package directory so module import works
COPY . /app/integration_consumer_webhook

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

WORKDIR /app

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash appuser

COPY --from=builder /app /app

# Make venv binaries available on PATH
ENV PATH="/app/.venv/bin:$PATH"

RUN chown -R appuser:appgroup /app
USER appuser

# No network ports to expose (message consumer only)

# Basic healthcheck: verify interpreter and module import are available
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD /app/.venv/bin/python -c "import importlib; importlib.import_module('integration_consumer_webhook.main')" || exit 1

CMD ["/app/.venv/bin/python", "-m", "integration_consumer_webhook.main"]
