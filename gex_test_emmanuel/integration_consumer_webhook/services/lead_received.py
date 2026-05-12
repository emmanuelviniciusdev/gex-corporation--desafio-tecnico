"""Business logic for the "lead.received" queue.

Expose:
- QUEUE_NAME: str
- make_handler(pool, publish_channel) -> Callable[[Any], Awaitable[None]]

All DB interactions are performed via an aiomysql.Pool instance passed in.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
import traceback
from datetime import datetime, timezone
from typing import Any

import aio_pika
import aiomysql

logger = logging.getLogger("integration_consumer_webhook.services.lead_received")

QUEUE_NAME = "lead.received"

CHANNELS = [
    ("SMS", "dist.sms"),
    ("EMAIL", "dist.email"),
    ("CALL_CENTER", "dist.call_center"),
    ("WHATSAPP", "dist.whatsapp"),
]


class ProcessingError(Exception):
    """Raised for transient processing failures that should be retried."""


def _parse_iso_datetime(value: str) -> datetime:
    if value is None:
        raise ValueError("missing datetime")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # return naive UTC datetime suitable for MySQL DATETIME(6)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


async def _ensure_lead(conn: aiomysql.Connection, cur: aiomysql.Cursor, email: str, email_raw: str, first_name: str, last_name: str, phone: str | None, phone_raw: str | None, phone_valid: int, country: str | None) -> int:
    await cur.execute("SELECT id FROM leads WHERE email = %s", (email,))
    row = await cur.fetchone()
    if row:
        return int(row[0])
    await cur.execute(
        "INSERT INTO leads (email, email_raw, first_name, last_name, phone, phone_raw, phone_valid, country, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(6), NOW(6))",
        (email, email_raw, first_name, last_name, phone, phone_raw, phone_valid, country),
    )
    lid = cur.lastrowid
    if not lid:
        await cur.execute("SELECT LAST_INSERT_ID()")
        row = await cur.fetchone()
        lid = row[0]
    return int(lid)


async def _ensure_order(conn: aiomysql.Connection, cur: aiomysql.Cursor, lead_id: int, raw_payload_id: int | None, gateway: str, transaction_id: str, transaction_time: datetime, product_id: str, product_name: str, product_niche: str | None, quantity: int, amount_usd: float, payment_method: str, payment_status: str) -> int:
    await cur.execute("SELECT id FROM orders WHERE gateway = %s AND transaction_id = %s", (gateway, transaction_id))
    row = await cur.fetchone()
    if row:
        return int(row[0])
    await cur.execute(
        "INSERT INTO orders (lead_id, raw_payload_id, gateway, transaction_id, transaction_time, product_id, product_name, product_niche, quantity, amount_usd, payment_method, payment_status, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(6))",
        (lead_id, raw_payload_id, gateway, transaction_id, transaction_time, product_id, product_name, product_niche, quantity, amount_usd, payment_method, payment_status),
    )
    oid = cur.lastrowid
    if not oid:
        await cur.execute("SELECT LAST_INSERT_ID()")
        row = await cur.fetchone()
        oid = row[0]
    return int(oid)


async def _insert_lead_event(cur: aiomysql.Cursor, order_id: int, transaction_id: str, correlation_id: str, event: str, gateway_time: datetime, persisted_at: datetime, lag_seconds: int) -> int:
    await cur.execute(
        "INSERT INTO lead_events (order_id, transaction_id, correlation_id, event, gateway_time, persisted_at, lag_seconds) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (order_id, transaction_id, correlation_id, event, gateway_time, persisted_at, lag_seconds),
    )
    eid = cur.lastrowid
    if not eid:
        await cur.execute("SELECT LAST_INSERT_ID()")
        row = await cur.fetchone()
        eid = row[0]
    return int(eid)


async def _ensure_distribution_entries(cur: aiomysql.Cursor, order_id: int) -> None:
    for channel, _queue in CHANNELS:
        await cur.execute(
            "INSERT IGNORE INTO distribution_status (order_id, channel, status, created_at) VALUES (%s, %s, 'pending', NOW(6))",
            (order_id, channel),
        )


async def _publish_distribution_messages(publish_channel: aio_pika.Channel, order_id: int, transaction_id: str, payload_obj: Any) -> None:
    for chan, queue_name in CHANNELS:
        body = json.dumps({"order_id": order_id, "transaction_id": transaction_id, "channel": chan, "payload": payload_obj}, default=str).encode("utf-8")
        await publish_channel.default_exchange.publish(aio_pika.Message(body=body), routing_key=queue_name)


async def _publish_consumer_failed(publish_channel: aio_pika.Channel, msg_obj: dict, error_message: str) -> None:
    # mirror shape used by other dead messages
    dead = {
        "id_raw_payload": msg_obj.get("id_raw_payload"),
        "id_processed_webhook": msg_obj.get("id_processed_webhook"),
        "error_message": error_message,
        "gateway": msg_obj.get("gateway"),
        "received_at": msg_obj.get("received_at"),
        "payload": msg_obj.get("payload"),
    }
    try:
        body = json.dumps(dead, default=str).encode("utf-8")
        await publish_channel.default_exchange.publish(aio_pika.Message(body=body), routing_key="lead.dead.consumer_failed")
    except Exception:
        logger.exception("failed to publish lead.dead.consumer_failed")


def _get_gateway_from_message(msg_obj: dict) -> str:
    return msg_obj.get("gateway", "webhook")


async def _process_once(msg_obj: dict, pool: aiomysql.Pool, publish_channel: aio_pika.Channel) -> None:
    """Perform a single attempt to process the message. Raise ProcessingError on transient failures.

    Non-retryable conditions (invalid payload/format) return normally without raising.
    """
    payload_field = msg_obj.get("payload")
    if isinstance(payload_field, str):
        try:
            payload = json.loads(payload_field)
        except Exception:
            logger.exception("failed to decode nested payload JSON")
            return
    elif isinstance(payload_field, dict):
        payload = payload_field
    else:
        logger.warning("message payload missing or invalid")
        return

    transaction_id = payload.get("transaction_id")
    transaction_time_raw = payload.get("transaction_time")
    event = payload.get("event")
    received_at_raw = msg_obj.get("received_at")

    # ensure required timestamps are present
    if not transaction_id or not transaction_time_raw or not received_at_raw:
        logger.warning("missing transaction_id, transaction_time or received_at")
        return

    try:
        transaction_time_dt = _parse_iso_datetime(transaction_time_raw)
    except Exception as exc:  # pragma: no cover - validation path
        logger.exception("invalid transaction_time format")
        return

    try:
        gateway_time = _parse_iso_datetime(received_at_raw)
    except Exception as exc:  # pragma: no cover - validation path
        logger.exception("invalid received_at format")
        return

    customer = payload.get("customer", {})
    email = (customer.get("email") or "").strip().lower()
    email_raw = customer.get("email") or ""
    first_name = customer.get("first_name") or "Customer"
    last_name = customer.get("last_name") or ""
    phone = customer.get("phone")
    phone_raw = phone
    phone_valid = 1 if phone else 0
    country = customer.get("country")

    product = payload.get("product", {})
    product_id = product.get("id") or ""
    product_name = product.get("name") or ""
    product_niche = product.get("niche")

    # quantity may be top-level or inside product
    quantity_raw = payload.get("quantity")
    if quantity_raw is None:
        quantity_raw = product.get("quantity")
    try:
        quantity = int(quantity_raw) if quantity_raw is not None else 1
    except Exception:
        quantity = 1

    payment = payload.get("payment", {})
    amount_usd = float(payment.get("amount_usd") or 0.0)
    payment_method = payment.get("method") or ""
    payment_status = payment.get("status") or ""

    gateway = _get_gateway_from_message(msg_obj)
    raw_payload_id = msg_obj.get("id_raw_payload")
    correlation_id = msg_obj.get("correlation_id")

    # DB operations
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                lead_id = await _ensure_lead(conn, cur, email, email_raw, first_name, last_name, phone, phone_raw, phone_valid, country)
                # use transaction_time_dt for orders.transaction_time
                order_id = await _ensure_order(conn, cur, lead_id, raw_payload_id, gateway, transaction_id, transaction_time_dt, product_id, product_name, product_niche, quantity, amount_usd, payment_method, payment_status)
                await _ensure_distribution_entries(cur, order_id)
            # commit to persist lead/order/distribution entries
            await conn.commit()

            # compute persisted timestamp and lag after commit
            persisted_at = datetime.now(timezone.utc).replace(tzinfo=None)
            # use transaction_time (the actual event time) to compute lag
            lag_seconds = int((persisted_at - transaction_time_dt).total_seconds())

            async with conn.cursor() as cur:
                await _insert_lead_event(cur, order_id, transaction_id, correlation_id, event, gateway_time, persisted_at, lag_seconds)
            await conn.commit()
    except Exception as exc:
        # wrap DB/publish errors as ProcessingError so retry logic can act on them
        raise ProcessingError(f"DB error: {exc}") from exc

    # publish distribution messages (best-effort but treated as critical here)
    try:
        await _publish_distribution_messages(publish_channel, order_id, transaction_id, payload)
    except Exception as exc:
        raise ProcessingError(f"publish error: {exc}") from exc

    logger.info("processed lead.received", extra={"transaction_id": transaction_id, "order_id": order_id, "lag_seconds": lag_seconds})


async def _process_with_retry(msg_obj: dict, pool: aiomysql.Pool, publish_channel: aio_pika.Channel) -> bool:
    """Process message with exponential backoff retries. Returns True on success.

    Retries on ProcessingError. On final failure, publishes to lead.dead.consumer_failed.
    """
    max_attempts = 3
    delays = [1, 4, 16]
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            await _process_once(msg_obj, pool, publish_channel)
            return True
        except ProcessingError as exc:
            last_exc = exc
            logger.warning("processing attempt %d/%d failed: %s", attempt, max_attempts, exc)
            if attempt < max_attempts:
                await asyncio.sleep(delays[attempt - 1])
                continue
            # final failure: publish to dead queue
            tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
            error_message = f"{exc} | traceback: {''.join(tb)}"
            try:
                await _publish_consumer_failed(publish_channel, msg_obj, error_message)
            except Exception:
                logger.exception("failed to publish consumer_failed after retries")
            logger.error("message processing failed after %d attempts", max_attempts)
            return False
        except Exception as exc:
            # unexpected exceptions treated as fatal for retries
            last_exc = exc
            logger.exception("unexpected error during processing (attempt %d)", attempt)
            if attempt < max_attempts:
                await asyncio.sleep(delays[attempt - 1])
                continue
            error_message = str(exc)
            try:
                await _publish_consumer_failed(publish_channel, msg_obj, error_message)
            except Exception:
                logger.exception("failed to publish consumer_failed after unexpected error")
            return False


def make_handler(pool: aiomysql.Pool, publish_channel: aio_pika.Channel):
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
                    await _process_with_retry(msg_obj, pool, publish_channel)
            else:
                await _process_with_retry(msg_obj, pool, publish_channel)
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
            await _process_with_retry(msg_obj, pool, publish_channel)

    return handler
