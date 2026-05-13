FROM python:3.13-slim AS builder

WORKDIR /app

# Minimal build tooling for potential wheels
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

# Tooling + uv
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uv

# Use subproject lockfile for reproducible deps
COPY pyproject.toml uv.lock ./
# Allow lock refresh so new dependencies (e.g. cryptography) are resolved
RUN uv sync --no-dev

# Copy application code into a package directory so module import works
COPY . /app/integration_consumer_channels

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

WORKDIR /app

RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash appuser

COPY --from=builder /app /app

# Add the virtualenv to PATH
ENV PATH="/app/.venv/bin:$PATH"

RUN chown -R appuser:appgroup /app
USER appuser

# Healthcheck: basic module import to verify env
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD /app/.venv/bin/python -c "import importlib; importlib.import_module('integration_consumer_channels.main')" || exit 1

CMD ["/app/.venv/bin/python", "-m", "integration_consumer_channels.main"]
