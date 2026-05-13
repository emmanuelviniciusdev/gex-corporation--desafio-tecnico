"""Configuration for integration_consumer_channels read from environment variables."""

from __future__ import annotations

import os

RABBITMQ_URL: str | None = os.environ.get("RABBITMQ_URL")
CONSUMER_CONCURRENCY: int = int(os.environ.get("CONSUMER_CONCURRENCY", "10"))
