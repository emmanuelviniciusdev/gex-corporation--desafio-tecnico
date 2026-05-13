"""Main consumer that runs the channels queue service.

To add other queue handlers, add modules under services/ exposing make_handler(...)
and register them in SERVICES mapping.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika
import aiomysql

try:
    from .consumer import AsyncConsumer, AsyncMessageSource
except Exception:  # pragma: no cover - support running as a script
    from consumer import AsyncConsumer, AsyncMessageSource

# import services
try:
    from .services import sms as sms_service
except Exception:  # pragma: no cover - support running as a script
    from services import sms as sms_service

logger = logging.getLogger("integration_consumer_channels.main")

RABBITMQ_URL = os.environ.get("RABBITMQ_URL")
MQ_PREFETCH = int(os.environ.get("MQ_PREFETCH", "10"))

# Register services here: queue_name -> service module
SERVICES: dict[str, Any] = {
    sms_service.QUEUE_NAME: sms_service,
}


class AioPikaSource(AsyncMessageSource):
    def __init__(self, queue: aio_pika.Queue) -> None:
        self._queue = queue

    async def __aiter__(self):
        async with self._queue.iterator() as queue_iter:
            async for incoming in queue_iter:
                yield incoming


async def run() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

    if not RABBITMQ_URL:
        logger.error("RABBITMQ_URL not set")
        return

    # DB defaults - override with environment variables
    DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
    DB_PORT = int(os.environ.get("DB_PORT", "3306"))
    DB_USER = os.environ.get("DB_USER", "root")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
    DB_NAME = os.environ.get("DB_NAME", "db_integration")

    db_pool = await aiomysql.create_pool(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_NAME, autocommit=False, maxsize=10)

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=MQ_PREFETCH)

        consumers: list[AsyncConsumer] = []

        # declare and start each configured queue service
        for queue_name, service in SERVICES.items():
            try:
                queue = await channel.declare_queue(queue_name, durable=True)
            except Exception:
                logger.exception("failed to declare queue %s", queue_name)
                continue

            source = AioPikaSource(queue)
            handler_factory: Callable[[Any, aio_pika.Channel], Callable[[Any], Awaitable[None]]] = service.make_handler
            handler = handler_factory(db_pool, channel)
            consumer = AsyncConsumer(source, handler)
            await consumer.start()
            consumers.append(consumer)
            logger.info("started consumer for queue %s", queue_name)

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            for c in consumers:
                await c.stop()
            db_pool.close()
            await db_pool.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
