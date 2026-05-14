"""Asynchronous webhook test runner.

Reads test_runner/webhook_payloads.json and POSTs each entry to the target server.
Defaults:
  base URL: http://localhost:8080
  path prefix: /webhooks

Requires: httpx (pip install httpx)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path


try:
    import httpx
except Exception:  # pragma: no cover - helpful error if dependency missing
    print("Missing dependency 'httpx'. Install with: pip install httpx", file=sys.stderr)
    raise


DEFAULT_BASE_URL = "http://localhost:8080"
DEFAULT_PATH = "/webhooks"
DEFAULT_CONCURRENCY = 10
DEFAULT_TIMEOUT = 10.0


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


async def post_payload(client: httpx.AsyncClient, base_url: str, path_prefix: str, entry: dict, idx: int, sem: asyncio.Semaphore):
    gateway = entry.get("gateway")
    headers = entry.get("headers") or {}
    body = entry.get("body") or {}

    if not gateway:
        logging.error("[%d] missing gateway in payload, skipping", idx)
        return

    if not path_prefix.startswith("/"):
        path_prefix = "/" + path_prefix

    url = f"{base_url.rstrip('/')}{path_prefix.rstrip('/')}/{gateway}"

    async with sem:
        try:
            resp = await client.post(url, json=body, headers=headers, timeout=DEFAULT_TIMEOUT)
            txt = resp.text or ""
            logging.info("[%d] POST %s -> %s", idx, url, resp.status_code)
            if resp.status_code >= 400:
                logging.warning("[%d] response body: %s", idx, txt[:1000])
        except Exception as exc:
            logging.exception("[%d] failed to POST to %s: %s", idx, url, exc)


async def run_all(payloads: list[dict], base_url: str, path_prefix: str, concurrency: int):
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [asyncio.create_task(post_payload(client, base_url, path_prefix, p, i + 1, sem)) for i, p in enumerate(payloads)]
        await asyncio.gather(*tasks)


def load_payloads(path: Path) -> list[dict]:
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Send webhook payloads to a local server (async)")
    p.add_argument("--url", default=DEFAULT_BASE_URL, help="Base URL of the server (default: %(default)s)")
    p.add_argument("--path", default=DEFAULT_PATH, help="Path prefix for webhook endpoint (default: %(default)s). Example: /webhooks or /webhook")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Number of concurrent requests")
    p.add_argument("--file", type=str, default=str(Path(__file__).parent / "webhook_payloads.json"), help="Payloads JSON file")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payloads_path = Path(args.file)
    if not payloads_path.exists():
        logging.error("Payloads file not found: %s", payloads_path)
        return 2

    try:
        payloads = load_payloads(payloads_path)
    except Exception:
        logging.exception("Failed to load payloads from %s", payloads_path)
        return 3

    try:
        asyncio.run(run_all(payloads, args.url, args.path, args.concurrency))
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
        return 130
    except Exception:
        logging.exception("Unexpected error while sending payloads")
        return 1

    logging.info("All done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
