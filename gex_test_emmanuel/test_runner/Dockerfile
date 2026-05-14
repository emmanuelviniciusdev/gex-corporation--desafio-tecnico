FROM python:3.13-slim

WORKDIR /app

# Install only what we need to run the webhook test runner
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir httpx

# Copy runner package into an importable package path: /app/test_runner
COPY . /app/test_runner

# Default command sends payloads to the integration API inside the compose network
# You can override args via `command:` in docker-compose if needed
CMD ["python", "-m", "test_runner.main", "--url", "http://integration_api:8080"]
