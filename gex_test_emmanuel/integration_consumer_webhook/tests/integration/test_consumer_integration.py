import asyncio
import os

import config
from consumer import AsyncConsumer


def test_rabbitmq_integration() -> None:
    """Integration test against a real RabbitMQ instance.

    This test is skipped unless RABBITMQ_URL is set in the environment and
    aio-pika is installed. It demonstrates how to wire a real aio-pika
    queue into the AsyncConsumer by providing an async iterator wrapper.
    """
    # prefer an explicit environment override so test fixtures can start a broker
    RABBITMQ_URL = os.environ.get("RABBITMQ_URL") or config.RABBITMQ_URL
    if not RABBITMQ_URL:
        import pytest

        pytest.skip("RABBITMQ_URL not set")

    try:
        import aio_pika
    except Exception:
        import pytest

        pytest.skip("aio-pika not installed")

    async def _test():
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("queue_test", durable=False)

            # publish a test message
            await channel.default_exchange.publish(
                aio_pika.Message(body=b"hello"), routing_key=queue.name
            )

            received = []

            async def handler(msg):
                # if msg is aio_pika.IncomingMessage, process accordingly
                if hasattr(msg, "body"):
                    # aio-pika message
                    if hasattr(msg, "process"):
                        async with msg.process():
                            received.append(msg.body)
                    else:
                        received.append(msg.body)
                else:
                    received.append(msg)

            class AioPikaSource:
                def __init__(self, queue):
                    self._queue = queue

                async def __aiter__(self):
                    async with self._queue.iterator() as queue_iter:
                        async for incoming in queue_iter:
                            yield incoming

            source = AioPikaSource(queue)
            consumer = AsyncConsumer(source, handler, concurrency=2)
            await consumer.start()
            # allow some time for the consumer to receive the message
            await asyncio.sleep(1.0)
            await consumer.stop()
            return received

    received = asyncio.run(_test())
    assert received, "No message received from RabbitMQ"
