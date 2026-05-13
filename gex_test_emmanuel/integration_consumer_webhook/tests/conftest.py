import os
import shutil
import socket
import subprocess
import sys
import time
import types

import pytest

pkg_name = "integration_consumer_webhook"
# package directory is the parent of the tests/ folder
pkg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Ensure package directory is importable so tests can import top-level modules (e.g. `import consumer`).
if pkg_path not in sys.path:
    sys.path.insert(0, pkg_path)

if pkg_name not in sys.modules:
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [pkg_path]
    sys.modules[pkg_name] = pkg


def _which_any(names):
    # prefer PATH lookup
    for n in names:
        p = shutil.which(n)
        if p:
            return p
    # fallback to common docker locations (Homebrew, Docker Desktop, system)
    common_paths = [
        "/usr/local/bin/docker",
        "/opt/homebrew/bin/docker",
        "/usr/bin/docker",
        "/bin/docker",
        "/snap/bin/docker",
    ]
    for p in common_paths:
        if os.path.exists(p) and os.access(p, os.X_OK):
            return p
    return None


def _start_rabbitmq_container():
    docker_bin = _which_any(("docker", "podman"))
    if docker_bin is None:
        print("[conftest] No 'docker' or 'podman' CLI found; cannot autostart RabbitMQ", file=sys.stderr)
        return None

    RABBIT_USER = os.environ.get("RABBITMQ_TEST_USER", "pytest")
    RABBIT_PW = os.environ.get("RABBITMQ_TEST_PASS", "pytest")

    # Verify Docker daemon is responsive
    try:
        info = subprocess.run([docker_bin, "info"], capture_output=True, text=True, timeout=10)
        if info.returncode != 0:
            print(f"[conftest] '{docker_bin} info' failed: {info.stderr.strip()}", file=sys.stderr)
            return None
    except Exception as exc:
        print(f"[conftest] Running '{docker_bin} info' failed: {exc}", file=sys.stderr)
        return None

    # remove any previous container silently
    subprocess.run([docker_bin, "rm", "-f", "pytest-rabbitmq"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    proc = subprocess.run([docker_bin, "run", "-d", "--rm", "-p", "5672:5672", "-p", "15672:15672", "--name", "pytest-rabbitmq", "-e", f"RABBITMQ_DEFAULT_USER={RABBIT_USER}", "-e", f"RABBITMQ_DEFAULT_PASS={RABBIT_PW}", "rabbitmq:3-management"], capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"[conftest] Failed to start RabbitMQ container: {proc.stderr.strip()}", file=sys.stderr)
        return None
    container_id = proc.stdout.strip()

    # verify broker is accepting AMQP frames using aio-pika if available
    url = f"amqp://{RABBIT_USER}:{RABBIT_PW}@127.0.0.1/"
    try:
        import asyncio

        import aio_pika
    except Exception:
        aio_pika = None
    if aio_pika:
        async def _wait_aio():
            for _ in range(60):
                try:
                    conn = await aio_pika.connect_robust(url)
                    await conn.close()
                    return True
                except Exception:
                    await asyncio.sleep(1)
            return False
        ok = asyncio.run(_wait_aio())
        if ok:
            return container_id

    # try management HTTP API as a secondary readiness check
    mgmt_url = "http://127.0.0.1:15672/api/overview"
    import base64
    import urllib.request
    auth = base64.b64encode(f"{RABBIT_USER}:{RABBIT_PW}".encode()).decode()
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            req = urllib.request.Request(mgmt_url, headers={"Authorization": "Basic " + auth})
            with urllib.request.urlopen(req, timeout=2) as resp:
                if getattr(resp, "status", None) == 200:
                    return container_id
        except Exception:
            time.sleep(1)

    # fallback to TCP port readiness
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", 5672), timeout=1):
                return container_id
        except Exception:
            time.sleep(0.5)

    print("[conftest] Timeout waiting for RabbitMQ to be ready", file=sys.stderr)
    try:
        logs = subprocess.run([docker_bin, "logs", "pytest-rabbitmq"], capture_output=True, text=True, timeout=10)
        print("[conftest] RabbitMQ container logs:\n" + logs.stdout, file=sys.stderr)
    except Exception:
        pass
    subprocess.run([docker_bin, "rm", "-f", "pytest-rabbitmq"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return None


@pytest.fixture(scope="session", autouse=True)
def ensure_rabbitmq():
    # If user already provided RABBITMQ_URL, respect it
    if os.environ.get("RABBITMQ_URL"):
        yield
        return
    container_id = _start_rabbitmq_container()
    if container_id:
        RABBIT_USER = os.environ.get("RABBITMQ_TEST_USER", "pytest")
        RABBIT_PW = os.environ.get("RABBITMQ_TEST_PASS", "pytest")
        os.environ["RABBITMQ_URL"] = f"amqp://{RABBIT_USER}:{RABBIT_PW}@127.0.0.1/"
        print(f"[conftest] Started RabbitMQ container {container_id[:12]}", file=sys.stderr)
    else:
        print("[conftest] Did not start RabbitMQ container; integration tests will be skipped.", file=sys.stderr)
    try:
        yield
    finally:
        if container_id:
            docker_bin = _which_any(("docker", "podman"))
            if docker_bin:
                subprocess.run([docker_bin, "rm", "-f", "pytest-rabbitmq"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
