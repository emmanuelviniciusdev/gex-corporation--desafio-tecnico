"""Configuration for integration_consumer_webhook read from environment variables."""

from __future__ import annotations

import os
from typing import Optional

RABBITMQ_URL: Optional[str] = os.environ.get("RABBITMQ_URL")
CONSUMER_CONCURRENCY: int = int(os.environ.get("CONSUMER_CONCURRENCY", "10"))
