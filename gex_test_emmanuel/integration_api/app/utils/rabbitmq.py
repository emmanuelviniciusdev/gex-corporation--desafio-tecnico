"""Async RabbitMQ publisher utility using aio-pika.

Provides a RabbitPublisher class that can be attached to FastAPI app.state
and used to publish messages asynchronously.
"""
import json
import logging
from typing import Any

import aio_pika
from aio_pika import ExchangeType, Message, connect_robust

logger = logging.getLogger(__name__)


class RabbitPublisher:
    def __init__(self, url: str, prefetch_count: int = 0) -> None:
        self._url = url
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.RobustChannel | None = None
        self._prefetch_count = prefetch_count

    async def connect(self) -> None:
        """Establish a robust connection and channel."""
        if self._connection and not self._connection.is_closed:
            return
        self._connection = await connect_robust(self._url)
        self._channel = await self._connection.channel()
        if self._prefetch_count:
            await self._channel.set_qos(prefetch_count=self._prefetch_count)

    async def close(self) -> None:
        """Close channel and connection gracefully."""
        try:
            if self._channel and not self._channel.is_closed:
                await self._channel.close()
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
        except Exception:
            logger.exception("Error closing RabbitMQ connection")

    async def publish(
        self,
        routing_key: str,
        message: Any,
        exchange_name: str = "",
        content_type: str = "application/json",
    ) -> None:
        """Publish a message to the default exchange or a named direct exchange.

        message may be bytes, str or a JSON-serializable object.
        """
        if not self._channel:
            await self.connect()

        if isinstance(message, (bytes, bytearray)):
            body = bytes(message)
        elif isinstance(message, str):
            body = message.encode("utf-8")
        else:
            body = json.dumps(message).encode("utf-8")

        msg = Message(body, content_type=content_type)

        try:
            if exchange_name:
                exchange = await self._channel.declare_exchange(
                    exchange_name, ExchangeType.DIRECT, durable=True
                )
                await exchange.publish(msg, routing_key=routing_key)
            else:
                await self._channel.default_exchange.publish(msg, routing_key=routing_key)
        except Exception:
            logger.exception("Failed to publish message to RabbitMQ")


async def publish_message_from_app(app, routing_key: str, message: Any, exchange_name: str = "") -> None:
    """Convenience helper that reads the publisher from app.state and publishes.

    Example:
        await publish_message_from_app(app, "events.created", {"id": 1})
    """
    publisher: RabbitPublisher | None = getattr(app.state, "rabbit", None)
    if not publisher:
        logger.warning("Rabbit publisher not initialized; message dropped")
        return
    await publisher.publish(routing_key=routing_key, message=message, exchange_name=exchange_name)
