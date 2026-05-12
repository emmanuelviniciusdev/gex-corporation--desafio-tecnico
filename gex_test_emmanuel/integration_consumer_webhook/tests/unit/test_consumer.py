import asyncio
import time

from consumer import AsyncConsumer, InMemoryMessageSource


def test_processes_messages_concurrently() -> None:
    async def _test():
        messages = list(range(8))
        processed = []

        async def handler(msg):
            # simulate I/O-bound work
            await asyncio.sleep(0.05)
            processed.append(msg)

        source = InMemoryMessageSource(messages)
        consumer = AsyncConsumer(source, handler, concurrency=4)
        await consumer.start()
        # wait until source is consumed and handlers finished
        await consumer.wait_until_done(timeout=5.0)
        await consumer.stop()
        return processed

    start = time.time()
    processed = asyncio.run(_test())
    duration = time.time() - start

    assert set(processed) == set(range(8))
    # sequential time would be ~0.05 * 8 = 0.4s; with concurrency should be noticeably less
    assert duration < 0.05 * 8


def test_stop_waits_for_tasks_finish() -> None:
    async def _test():
        messages = [1, 2, 3]
        processed = []

        async def handler(msg):
            await asyncio.sleep(0.2)
            processed.append(msg)

        source = InMemoryMessageSource(messages)
        consumer = AsyncConsumer(source, handler, concurrency=3)
        await consumer.start()
        # let the consumer pick up tasks
        await asyncio.sleep(0.05)
        # stopping should wait for the handler tasks to complete
        await consumer.stop()
        return processed

    processed = asyncio.run(_test())
    assert set(processed) == set([1, 2, 3])
