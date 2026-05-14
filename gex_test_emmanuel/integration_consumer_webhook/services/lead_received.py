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
import traceback
from datetime import UTC, datetime
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
        dt = dt.replace(tzinfo=UTC)
    # return naive UTC datetime suitable for MySQL DATETIME(6)
    return dt.astimezone(UTC).replace(tzinfo=None)


async def _call_sp_insert_lead(
    cur: aiomysql.Cursor,
    email: str,
    email_raw: str,
    first_name: str,
    last_name: str,
    phone: str | None,
    phone_raw: str | None,
    phone_valid: int,
    country: str | None,
    raw_payload_id: int | None,
    gateway: str,
    transaction_id: str,
    transaction_time: datetime,
    product_id: str,
    product_name: str,
    product_niche: str | None,
    quantity: int,
    amount_usd: float,
    payment_method: str,
    payment_status: str,
    correlation_id: str,
    event: str,
    gateway_time: datetime,
    persisted_at: datetime,
    lag_milliseconds: int,
) -> tuple[int, int, int | None]:
    """Call the sp_insert_lead stored procedure.

    Atomically inserts (or retrieves existing) rows in leads, orders, and
    lead_events within a single transaction managed by the stored procedure.

    Returns (lead_id, order_id, event_id).
    """
    await cur.callproc(
        "sp_insert_lead",
        (
            email, email_raw, first_name, last_name,
            phone, phone_raw, phone_valid, country,
            raw_payload_id, gateway, transaction_id, transaction_time,
            product_id, product_name, product_niche,
            quantity, amount_usd, payment_method, payment_status,
            correlation_id, event, gateway_time, persisted_at, lag_milliseconds,
            0, 0, 0,  # OUT: p_lead_id, p_order_id, p_event_id
        ),
    )
    await cur.execute(
        "SELECT @_sp_insert_lead_25 AS lead_id, @_sp_insert_lead_26 AS order_id, @_sp_insert_lead_27 AS event_id"
    )
    row = await cur.fetchone()
    # The stored procedure may return NULL for event_id in idempotent/no-op cases.
    # Treat that as "no new event row" instead of failing the whole processing.
    lead_id = int(row[0]) if row and row[0] is not None else None
    order_id = int(row[1]) if row and row[1] is not None else None
    event_id = int(row[2]) if row and row[2] is not None else None

    # lead_id and order_id are required for downstream processing; if missing, bubble up a clear error.
    if lead_id is None or order_id is None:
        raise ValueError("stored procedure did not return lead_id/order_id")

    return lead_id, order_id, event_id


async def _ensure_distribution_entries(cur: aiomysql.Cursor, order_id: int) -> None:
    for channel, _queue in CHANNELS:
        await cur.execute(
            "INSERT IGNORE INTO distribution_status (order_id, channel, status, created_at) VALUES (%s, %s, 'pending', NOW(6))",
            (order_id, channel),
        )


async def _publish_distribution_messages(
    publish_channel: aio_pika.Channel,
    order_id: int,
    transaction_id: str,
    payload_obj: Any,
    correlation_id: str | None,
) -> None:
    """Publish a copy of the message to each distribution channel.

    Guarantees that the nested payload includes `correlation_id`.
    """
    # ensure payload is a dict so we can inject correlation_id
    try:
        payload_for_publish = dict(payload_obj) if isinstance(payload_obj, dict) else json.loads(payload_obj)
    except Exception:
        # as a last resort, wrap original as-is
        payload_for_publish = payload_obj

    if isinstance(payload_for_publish, dict):
        # Always embed/override correlation_id to guarantee contract
        if correlation_id:
            payload_for_publish["correlation_id"] = correlation_id

    for chan, queue_name in CHANNELS:
        body = json.dumps(
            {
                "order_id": order_id,
                "transaction_id": transaction_id,
                "channel": chan,
                "payload": payload_for_publish,
            },
            default=str,
        ).encode("utf-8")
        await publish_channel.default_exchange.publish(aio_pika.Message(body=body), routing_key=queue_name)


async def _publish_consumer_failed(publish_channel: aio_pika.Channel, msg_obj: dict, error_message: str) -> None:
    # mirror shape used by other dead messages
    payload_field = msg_obj.get("payload")
    try:
        payload_obj = json.loads(payload_field) if isinstance(payload_field, str) else dict(payload_field)
    except Exception:
        payload_obj = payload_field

    # Guarantee correlation_id inside nested payload when possible
    if isinstance(payload_obj, dict):
        corr = msg_obj.get("correlation_id")
        if corr:
            payload_obj["correlation_id"] = corr

    dead = {
        "id_raw_payload": msg_obj.get("id_raw_payload"),
        "id_processed_webhook": msg_obj.get("id_processed_webhook"),
        "error_message": error_message,
        "gateway": msg_obj.get("gateway"),
        "received_at": msg_obj.get("received_at"),
        "payload": payload_obj,
    }
    try:
        body = json.dumps(dead, default=str).encode("utf-8")
        await publish_channel.default_exchange.publish(aio_pika.Message(body=body), routing_key="lead.dead.consumer_failed")
    except Exception:
        logger.exception("failed to publish lead.dead.consumer_failed")


async def _insert_dead_letter(
    pool: aiomysql.Pool,
    correlation_id: str | None,
    origin: str,
    raw_payload_id: int | None,
    payload: str,
    error_message: str,
) -> None:
    """Persist a dead-letter entry into the lead_dead_letter table.

    Failures are logged but never re-raised so as not to disrupt retry/shutdown flow.
    """
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO lead_dead_letter"
                    " (correlation_id, origin, raw_payload_id, payload, error_message, created_at)"
                    " VALUES (%s, %s, %s, %s, %s, NOW(6))",
                    (correlation_id, origin, raw_payload_id, payload, error_message),
                )
            await conn.commit()
    except Exception:
        logger.exception("failed to insert dead letter entry")


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
    except Exception:  # pragma: no cover - validation path
        logger.exception("invalid transaction_time format")
        return

    try:
        gateway_time = _parse_iso_datetime(received_at_raw)
    except Exception:  # pragma: no cover - validation path
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

    # compute persisted timestamp and lag (milliseconds) before DB call
    persisted_at = datetime.now(UTC).replace(tzinfo=None)
    # use transaction_time (the actual event time) to compute lag
    lag_milliseconds = int(max(0, round((persisted_at - transaction_time_dt).total_seconds() * 1000)))

    # DB operations — leads, orders and lead_events are inserted atomically
    # via the sp_insert_lead stored procedure
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                lead_id, order_id, _event_id = await _call_sp_insert_lead(
                    cur,
                    email, email_raw, first_name, last_name,
                    phone, phone_raw, phone_valid, country,
                    raw_payload_id, gateway, transaction_id, transaction_time_dt,
                    product_id, product_name, product_niche,
                    quantity, amount_usd, payment_method, payment_status,
                    correlation_id, event, gateway_time, persisted_at, lag_milliseconds,
                )
                await _ensure_distribution_entries(cur, order_id)
            await conn.commit()
    except Exception as exc:
        # wrap DB/publish errors as ProcessingError so retry logic can act on them
        raise ProcessingError(f"DB error: {exc}") from exc

    # publish distribution messages (best-effort but treated as critical here)
    try:
        await _publish_distribution_messages(
            publish_channel,
            order_id,
            transaction_id,
            payload,
            correlation_id,
        )
    except Exception as exc:
        raise ProcessingError(f"publish error: {exc}") from exc

    logger.info("processed lead.received", extra={"transaction_id": transaction_id, "order_id": order_id, "lag_milliseconds": lag_milliseconds})


async def _process_with_retry(msg_obj: dict, pool: aiomysql.Pool, publish_channel: aio_pika.Channel) -> bool:
    """Process message with exponential backoff retries. Returns True on success.

    Retries on ProcessingError. On final failure, publishes to lead.dead.consumer_failed
    and persists a row in lead_dead_letter.
    """
    max_attempts = 3
    delays = [1, 4, 16]
    for attempt in range(1, max_attempts + 1):
        try:
            await _process_once(msg_obj, pool, publish_channel)
            return True
        except ProcessingError as exc:
            logger.warning("processing attempt %d/%d failed: %s", attempt, max_attempts, exc)
            if attempt < max_attempts:
                await asyncio.sleep(delays[attempt - 1])
                continue
            # final failure: publish to dead queue and persist dead letter
            tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
            error_message = f"{exc} | traceback: {''.join(tb)}"
            try:
                await _publish_consumer_failed(publish_channel, msg_obj, error_message)
            except Exception:
                logger.exception("failed to publish consumer_failed after retries")
            await _insert_dead_letter(
                pool,
                correlation_id=msg_obj.get("correlation_id"),
                origin=QUEUE_NAME,
                raw_payload_id=msg_obj.get("id_raw_payload"),
                payload=json.dumps(msg_obj, default=str),
                error_message=error_message,
            )
            logger.error("message processing failed after %d attempts", max_attempts)
            return False
        except Exception as exc:
            # unexpected exceptions treated as fatal for retries
            logger.exception("unexpected error during processing (attempt %d)", attempt)
            if attempt < max_attempts:
                await asyncio.sleep(delays[attempt - 1])
                continue
            error_message = str(exc)
            try:
                await _publish_consumer_failed(publish_channel, msg_obj, error_message)
            except Exception:
                logger.exception("failed to publish consumer_failed after unexpected error")
            await _insert_dead_letter(
                pool,
                correlation_id=msg_obj.get("correlation_id"),
                origin=QUEUE_NAME,
                raw_payload_id=msg_obj.get("id_raw_payload"),
                payload=json.dumps(msg_obj, default=str),
                error_message=error_message,
            )
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
