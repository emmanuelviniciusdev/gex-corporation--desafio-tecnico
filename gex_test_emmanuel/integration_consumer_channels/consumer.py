"""Async, concurrency-aware consumer skeleton.

This module provides a small, broker-agnostic AsyncConsumer that accepts an
async-iterable message source and a coroutine handler. It is intentionally
minimal so tests can run without RabbitMQ. For real RabbitMQ usage, provide
an async iterator that yields messages (for example, wrapping aio-pika's
queue.iterator()).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import Any

try:
    from .config import CONSUMER_CONCURRENCY
except Exception:  # pragma: no cover - support running as a script
    from config import CONSUMER_CONCURRENCY

Handler = Callable[[Any], Awaitable[None]]


class AsyncMessageSource:
    """Abstract async iterable message source."""

    async def __aiter__(self) -> AsyncIterator[Any]:
        raise NotImplementedError


class InMemoryMessageSource(AsyncMessageSource):
    """Simple async iterable that yields a provided list of messages.

    Useful for unit tests and local development.
    """

    def __init__(self, messages: Iterable[Any], delay: float = 0.0) -> None:
        self._messages = list(messages)
        self._delay = float(delay)

    async def __aiter__(self) -> AsyncIterator[Any]:
        for m in self._messages:
            if self._delay:
                # allow concurrent scheduling
                await asyncio.sleep(self._delay)
            yield m


class AsyncConsumer:
    """Broker-agnostic async consumer with configurable concurrency.

    Usage:
      consumer = AsyncConsumer(source, handler, concurrency=10)
      await consumer.start()
      await consumer.wait_until_done()
      await consumer.stop()

    The consumer expects `source` to be an async iterable (supporting "async for").
    """

    def __init__(self, source: AsyncMessageSource, handler: Handler, concurrency: int | None = None) -> None:
        # Default concurrency comes from environment via config.CONSUMER_CONCURRENCY
        concurrency_value = CONSUMER_CONCURRENCY if concurrency is None else int(concurrency)
        self._source = source
        self._handler = handler
        self._concurrency = max(1, int(concurrency_value))
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._tasks: set[asyncio.Task] = set()
        self._run_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the background task that consumes messages from the source."""
        if self._run_task is None or self._run_task.done():
            self._run_task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            async for message in self._source:
                await self._semaphore.acquire()
                task = asyncio.create_task(self._run_handler(message))
                self._tasks.add(task)
                # ensure bookkeeping and semaphore release when task completes
                task.add_done_callback(self._task_done_cb)
                # debug scheduling
                try:
                    print(f"[consumer] scheduled task for message: {message!r}")
                except Exception:
                    pass
        except asyncio.CancelledError:
            # Consumer was asked to stop; fall through to draining tasks
            pass
        finally:
            # Ensure draining handler tasks is shielded from cancellation of the run task
            await asyncio.shield(self._drain_tasks())

    async def _run_handler(self, message: Any) -> None:
        try:
            # debug start
            try:
                print(f"[consumer] handler start: {message!r}")
            except Exception:
                pass
            await self._handler(message)
            # debug end
            try:
                print(f"[consumer] handler end: {message!r}")
            except Exception:
                pass
        except Exception as exc:  # pragma: no cover - decision/error handling
            # In a real app, replace prints with structured logging
            print(f"integration_consumer_channels: handler raised: {exc!r}")

    def _task_done_cb(self, task: asyncio.Task) -> None:
        # Called in the event loop when a task completes
        try:
            print(f"[consumer] _task_done_cb: removing task {task!r}")
        except Exception:
            pass
        self._tasks.discard(task)
        try:
            self._semaphore.release()
        except Exception:
            pass

    async def wait_until_done(self, timeout: float | None = None) -> None:
        """Wait until the source iteration completes and all handler tasks finish.

        If timeout is provided, wait at most `timeout` seconds for the source
        to complete; handler tasks will still be awaited afterwards.
        """
        if self._run_task is not None:
            try:
                await asyncio.wait_for(self._run_task, timeout=timeout)
            except TimeoutError:
                # the run task is still active; proceed to drain currently running tasks
                pass
        await self._drain_tasks()

    async def _drain_tasks(self) -> None:
        tasks = list(self._tasks)
        try:
            print(f"[consumer] _drain_tasks: found {len(tasks)} tasks")
        except Exception:
            pass
        if not tasks:
            return
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            print("[consumer] _drain_tasks: gathered tasks")
        except Exception:
            pass
        self._tasks.clear()

    async def stop(self) -> None:
        """Stop consumption and wait for running handlers to finish."""
        try:
            print(f"[consumer] stop: run_task is {self._run_task!r}, done={(self._run_task.done() if self._run_task else 'N/A')}" )
        except Exception:
            pass
        if self._run_task is not None and not self._run_task.done():
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
        try:
            print("[consumer] stop: awaiting _drain_tasks()")
        except Exception:
            pass
        await self._drain_tasks()
        try:
            print("[consumer] stop: _drain_tasks complete")
        except Exception:
            pass
