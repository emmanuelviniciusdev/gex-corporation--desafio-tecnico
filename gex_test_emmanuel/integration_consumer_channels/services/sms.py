"""Business logic for the "dist.sms" queue.

The handler performs a simple POST to https://webhook.site/69ea9bfe-7670-47a0-bae9-0bdcb81d9219 with no body and
retries with exponential backoff on transient failures.

Expose:
- QUEUE_NAME: str
- make_handler(pool, publish_channel) -> Callable[[Any], Awaitable[None]]
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import UTC, datetime
from typing import Any

import aio_pika
import aiohttp

logger = logging.getLogger("integration_consumer_channels.services.sms")

QUEUE_NAME = "dist.sms"


class ProcessingError(Exception):
    """Raised for transient processing failures that should be retried."""


async def _publish_consumer_failed(publish_channel: aio_pika.Channel, msg_obj: dict, error_message: str) -> None:
    # Preserve/ensure correlation_id inside nested payload when possible
    payload_field = msg_obj.get("payload")
    payload_obj = payload_field
    try:
        if isinstance(payload_field, str):
            payload_obj = json.loads(payload_field)
        elif isinstance(payload_field, dict):
            payload_obj = dict(payload_field)
    except Exception:
        payload_obj = payload_field

    if isinstance(payload_obj, dict):
        corr = msg_obj.get("correlation_id")
        if corr and "correlation_id" not in payload_obj:
            payload_obj["correlation_id"] = corr

    dead = {
        "error_message": error_message,
        "payload": payload_obj,
        "received_at": msg_obj.get("received_at"),
    }
    try:
        body = json.dumps(dead, default=str).encode("utf-8")
        await publish_channel.default_exchange.publish(aio_pika.Message(body=body), routing_key="channels.dead.consumer_failed")
    except Exception:
        logger.exception("failed to publish channels.dead.consumer_failed")


async def _post_once(msg_obj: dict, session: aiohttp.ClientSession) -> None:
    url = "https://webhook.site/69ea9bfe-7670-47a0-bae9-0bdcb81d9219"
    try:
        async with session.post(url) as resp:
            status = resp.status
            if status >= 400:
                text = await resp.text()
                raise ProcessingError(f"POST {url} returned status {status}: {text}")
    except aiohttp.ClientError as exc:
        raise ProcessingError(f"http client error: {exc}") from exc


async def _process_with_retry(
    msg_obj: dict,
    session: aiohttp.ClientSession,
    publish_channel: aio_pika.Channel | None,
) -> tuple[bool, str | None]:
    """Process message with exponential backoff retries.

    Returns (success: bool, error_message: str|None).
    """
    max_attempts = 3
    delays = [1, 4, 16]
    for attempt in range(1, max_attempts + 1):
        try:
            await _post_once(msg_obj, session)
            # Use non-reserved key in logging extra to avoid LogRecord overwrite errors
            logger.info("posted sms for message", extra={"msg_obj": msg_obj})
            return True, None
        except ProcessingError as exc:
            logger.warning("posting attempt %d/%d failed: %s", attempt, max_attempts, exc)
            if attempt < max_attempts:
                await asyncio.sleep(delays[attempt - 1])
                continue
            tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
            error_message = f"{exc} | traceback: {''.join(tb)}"
            if publish_channel:
                try:
                    await _publish_consumer_failed(publish_channel, msg_obj, error_message)
                except Exception:
                    logger.exception("failed to publish consumer_failed after retries")
            logger.error("message processing failed after %d attempts", max_attempts)
            return False, error_message
        except Exception as exc:
            logger.exception("unexpected error during posting (attempt %d)", attempt)
            if attempt < max_attempts:
                await asyncio.sleep(delays[attempt - 1])
                continue
            error_message = str(exc)
            if publish_channel:
                try:
                    await _publish_consumer_failed(publish_channel, msg_obj, error_message)
                except Exception:
                    logger.exception("failed to publish consumer_failed after unexpected error")
            return False, error_message


async def _mark_delivered(pool: Any, order_id: int, channel: str) -> None:
    """Mark the distribution_status row for the given order/channel as delivered
    and compute/log the lag between distribution_status.created_at and now.
    """
    if not pool:
        logger.warning("no DB pool provided; skipping distribution_status update for order_id=%s channel=%s", order_id, channel)
        return
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT created_at FROM distribution_status WHERE order_id=%s AND channel=%s LIMIT 1", (order_id, channel))
                row = await cur.fetchone()
                if not row:
                    logger.warning("distribution_status row not found for order_id=%s channel=%s", order_id, channel)
                    return
                created_at = row[0]
                # normalize to naive UTC if tz-aware
                if getattr(created_at, "tzinfo", None) is not None:
                    created_at = created_at.astimezone(UTC).replace(tzinfo=None)
                now = datetime.now(UTC).replace(tzinfo=None)
                total_sec = (now - created_at).total_seconds()
                # store lag with millisecond precision (non-negative)
                lag_milliseconds = int(max(0, round(total_sec * 1000)))
                await cur.execute(
                    "UPDATE distribution_status SET status='delivered', delivered_at=NOW(6), lag_db_channel_milliseconds=%s WHERE order_id=%s AND channel=%s",
                    (lag_milliseconds, order_id, channel),
                )
            await conn.commit()
        logger.info("marked distribution_status delivered order_id=%s channel=%s lag_milliseconds=%d", order_id, channel, lag_milliseconds)
    except Exception:
        logger.exception("failed to update distribution_status for order_id=%s channel=%s", order_id, channel)


async def _mark_failed(pool: Any, order_id: int, channel: str, error_message: str | None) -> None:
    """Mark the distribution_status row as failed, persist the error message,
    and update the lag in milliseconds since the distribution record was created.

    The lag is computed server-side using `created_at` -> `NOW(6)` to avoid an
    extra round-trip; it is clamped to non-negative and rounded to milliseconds.
    """
    if not pool:
        logger.warning("no DB pool provided; skipping distribution_status failed update for order_id=%s channel=%s", order_id, channel)
        return
    # Truncate very long messages to a reasonable size to avoid oversized packets
    msg = (error_message or "").strip()
    if len(msg) > 4000:
        msg = msg[:4000]
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    (
                        "UPDATE distribution_status "
                        "SET status='failed', error_message=%s, "
                        "    lag_db_channel_milliseconds = GREATEST(0, ROUND(TIMESTAMPDIFF(MICROSECOND, created_at, NOW(6)) / 1000)) "
                        "WHERE order_id=%s AND channel=%s"
                    ),
                    (msg, order_id, channel),
                )
            await conn.commit()
        logger.info("marked distribution_status failed order_id=%s channel=%s", order_id, channel)
    except Exception:
        logger.exception("failed to update distribution_status (failed) for order_id=%s channel=%s", order_id, channel)


def make_handler(pool: Any, publish_channel: aio_pika.Channel):
    # create aiohttp session with reasonable timeout
    timeout = aiohttp.ClientTimeout(total=10)
    session = aiohttp.ClientSession(timeout=timeout)

    async def handler(raw_msg: Any) -> None:
        if hasattr(raw_msg, "body"):
            body = raw_msg.body
            try:
                msg_obj = json.loads(body.decode()) if isinstance(body, (bytes, bytearray)) else json.loads(body)
            except Exception:
                logger.exception("failed to decode message body")
                return
            if hasattr(raw_msg, "process"):
                # keep message un-acked until processing+retries complete
                async with raw_msg.process():
                    success, error_message = await _process_with_retry(msg_obj, session, publish_channel)
                    if success:
                        order_id = msg_obj.get("order_id")
                        channel = msg_obj.get("channel")
                        try:
                            await _mark_delivered(pool, order_id, channel)
                        except Exception:
                            logger.exception("failed to mark distribution delivered")
                    else:
                        order_id = msg_obj.get("order_id")
                        channel = msg_obj.get("channel")
                        try:
                            await _mark_failed(pool, order_id, channel, error_message)
                        except Exception:
                            logger.exception("failed to mark distribution failed")
            else:
                success, error_message = await _process_with_retry(msg_obj, session, publish_channel)
                if success:
                    order_id = msg_obj.get("order_id")
                    channel = msg_obj.get("channel")
                    try:
                        await _mark_delivered(pool, order_id, channel)
                    except Exception:
                        logger.exception("failed to mark distribution delivered")
                else:
                    order_id = msg_obj.get("order_id")
                    channel = msg_obj.get("channel")
                    try:
                        await _mark_failed(pool, order_id, channel, error_message)
                    except Exception:
                        logger.exception("failed to mark distribution failed")
        else:
            if isinstance(raw_msg, dict):
                msg_obj = raw_msg
            elif isinstance(raw_msg, (bytes, bytearray)):
                try:
                    msg_obj = json.loads(raw_msg.decode())
                except Exception:
                    logger.exception("failed to decode raw message bytes")
                    return
            else:
                try:
                    msg_obj = json.loads(str(raw_msg))
                except Exception:
                    logger.exception("failed to decode raw message")
                    return
            success, error_message = await _process_with_retry(msg_obj, session, publish_channel)
            if success:
                order_id = msg_obj.get("order_id")
                channel = msg_obj.get("channel")
                try:
                    await _mark_delivered(pool, order_id, channel)
                except Exception:
                    logger.exception("failed to mark distribution delivered")
            else:
                order_id = msg_obj.get("order_id")
                channel = msg_obj.get("channel")
                try:
                    await _mark_failed(pool, order_id, channel, error_message)
                except Exception:
                    logger.exception("failed to mark distribution failed")

    return handler
