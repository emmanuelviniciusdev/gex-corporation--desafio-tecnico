import base64
import json
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Literal

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from fastapi import APIRouter, Depends, Header, Request, Response, status
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.core.redis import get_redis
from app.db import LeadDeadLetter, ProcessedWebhook, RawPayload
from app.models.webhook import DuplicateResponse, EncryptedPayload, Payload
from app.utils.rabbitmq import publish_message_from_app

router = APIRouter()
logger = logging.getLogger(__name__)

IDEMPOTENCY_LOCK_TTL = 30
IDEMPOTENCY_KEY_PREFIX = "webhook:lock:"


def _load_grummer_key() -> bytes:
    """Return a 32‑byte AES‑256 key from settings.

    Accepts the following formats for convenience:
    - 64‑hex characters (e.g. "bd8f…f153"): interpreted as hex directly.
    - Base64 of the 32 raw key bytes.
    - Base64 of a 64‑char hex ASCII string (some integrations send this):
      we decode base64, detect hex ASCII, and convert to bytes.
    """
    raw = settings.grummer_aes256_key_base64
    if not raw:
        raise ValueError("missing grummer key")

    s = str(raw).strip()

    # 1) Direct HEX (64 hex characters → 32 bytes)
    if re.fullmatch(r"[0-9a-fA-F]{64}", s or ""):
        return bytes.fromhex(s)

    # 2) Base64 decode
    try:
        decoded = base64.b64decode(s)
    except Exception as exc:
        raise ValueError("invalid grummer key") from exc

    # 2a) If base64 decodes to 32 bytes, assume raw key bytes
    if len(decoded) == 32:
        return decoded

    # 2b) If base64 decodes to 64 ASCII hex chars, convert to bytes
    try:
        text = decoded.decode("ascii", errors="strict")
        if re.fullmatch(r"[0-9a-fA-F]{64}", text):
            return bytes.fromhex(text)
    except Exception:
        pass

    raise ValueError("invalid grummer key")


def _decrypt_grummer_payload(encrypted_payload: EncryptedPayload) -> dict:
    key = _load_grummer_key()
    iv = base64.b64decode(encrypted_payload.iv)
    ciphertext = base64.b64decode(encrypted_payload.ciphertext)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
    unpadder = PKCS7(128).unpadder()
    plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()
    return json.loads(plaintext.decode("utf-8"))


def _normalize_email(value: str) -> tuple[str, bool]:
    normalized = (value or "").strip().lower()
    is_valid = bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", normalized))
    return normalized, not is_valid


def _normalize_phone(value: str | None) -> tuple[str | None, bool]:
    if not value:
        return None, False
    has_plus = value.strip().startswith("+")
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) < 8:
        return value, True
    if has_plus:
        return f"+{digits}", False
    if len(digits) in (10, 11):
        return f"+1{digits}", False
    return f"+{digits}", False


def _normalize_payload(payload_data: dict) -> tuple[dict, bool, bool]:
    normalized = dict(payload_data)
    customer = dict(normalized.get("customer", {}))

    email, invalid_email = _normalize_email(str(customer.get("email", "")))
    customer["email"] = email

    phone, invalid_phone = _normalize_phone(customer.get("phone"))
    customer["phone"] = phone

    first_name = (customer.get("first_name") or "").strip()
    customer["first_name"] = first_name or "Customer"

    normalized["customer"] = customer
    return normalized, invalid_email, invalid_phone


async def _check_idempotency(transaction_id: str, event: str, correlation_id: str, db: AsyncSession) -> bool:
    redis_client = get_redis()
    lock_key = f"{IDEMPOTENCY_KEY_PREFIX}{transaction_id}:{event}"

    acquired = redis_client.set(lock_key, correlation_id, nx=True, ex=IDEMPOTENCY_LOCK_TTL)
    if not acquired:
        return True

    result = await db.execute(
        select(ProcessedWebhook).where(
            ProcessedWebhook.transaction_id == transaction_id, ProcessedWebhook.event == event
        )
    )
    existing = result.scalars().first()

    if existing:
        return True

    return False


async def _mark_processed(transaction_id: str, event: str, correlation_id: str, db: AsyncSession) -> int:
    processed = ProcessedWebhook(
        transaction_id=transaction_id,
        event=event,
        correlation_id=correlation_id,
        processed_at=datetime.now(UTC),
    )
    db.add(processed)
    await db.commit()
    try:
        await db.refresh(processed)
    except Exception:
        pass
    return int(processed.id)


async def _persist_raw_payload(
    correlation_id: str,
    gateway: str,
    received_at: datetime,
    headers: dict,
    original_body: str,
    decrypted_body: str | None,
    db: AsyncSession,
) -> int:
    raw_payload = RawPayload(
        correlation_id=correlation_id,
        gateway=gateway,
        received_at=received_at,
        headers=headers,
        original_body=original_body,
        decrypted_body=decrypted_body,
    )
    db.add(raw_payload)
    await db.commit()
    try:
        await db.refresh(raw_payload)
    except Exception:
        pass
    return int(raw_payload.id)


async def _insert_dead_letter(
    db: AsyncSession,
    *,
    correlation_id: str | None,
    origin: str,
    raw_payload_id: int | None,
    payload: str,
    error_message: str,
) -> None:
    """Insert a row into lead_dead_letter. Log-and-swallow failures.

    This mirrors the consumer-side behavior: dead-letter persistence must not
    disrupt the request flow.
    """
    try:
        dl = LeadDeadLetter(
            correlation_id=correlation_id,
            origin=origin,
            raw_payload_id=raw_payload_id,
            payload=payload,
            error_message=error_message,
            created_at=datetime.now(UTC),
        )
        db.add(dl)
        await db.commit()
    except Exception:
        logger.exception("failed to insert dead letter entry (webhook)")


@router.post("/webhooks/{gateway}", response_model=None)
async def receive_webhook(
    gateway: Literal["grummer", "lous"],
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_gr_encrypted: str | None = Header(default=None, alias="X-GR-Encrypted"),
):
    correlation_id = str(uuid.uuid4())
    received_at = datetime.now(UTC)
    validation_errors: list[dict] = []
    schema_valid = True
    decrypted_body: str | None = None
    decrypt_failed_reason: str | None = None
    schema_invalid_reason: str | None = None

    logger.info("Webhook received", extra={"correlation_id": correlation_id, "gateway": gateway})

    raw_body = await request.body()
    original_body = raw_body.decode("utf-8")
    headers = dict(request.headers)

    try:
        incoming_payload = json.loads(original_body) if original_body else {}
    except Exception as error:
        incoming_payload = {}
        schema_valid = False
        validation_errors.append({"message": str(error)})
        logger.warning(
            "Failed to parse JSON", extra={"correlation_id": correlation_id, "error": str(error)}
        )

    payload_data: dict = {}

    if gateway == "grummer":
        is_encrypted = str(x_gr_encrypted).lower() == "true"
        if not is_encrypted:
            schema_valid = False
            validation_errors.append(
                {"field": "X-GR-Encrypted", "message": "must be true for grummer"}
            )
            if isinstance(incoming_payload, dict):
                payload_data = incoming_payload
        else:
            try:
                encrypted_payload = EncryptedPayload.model_validate(incoming_payload)
                payload_data = _decrypt_grummer_payload(encrypted_payload)
                decrypted_body = json.dumps(payload_data)
            except ValidationError as error:
                schema_valid = False
                validation_errors.extend(error.errors())
                decrypt_failed_reason = json.dumps(error.errors(), default=str)
                payload_data = incoming_payload if isinstance(incoming_payload, dict) else {}
                logger.warning(
                    "Encrypted payload validation failed", extra={"correlation_id": correlation_id}
                )
            except Exception as error:
                schema_valid = False
                validation_errors.append({"message": str(error)})
                decrypt_failed_reason = str(error)
                payload_data = incoming_payload if isinstance(incoming_payload, dict) else {}
                logger.warning(
                    "Decryption failed",
                    extra={"correlation_id": correlation_id, "error": str(error)},
                )
    else:
        payload_data = incoming_payload if isinstance(incoming_payload, dict) else {}

    raw_id = await _persist_raw_payload(
        correlation_id, gateway, received_at, headers, original_body, decrypted_body, db
    )
    logger.info("Raw payload persisted", extra={"correlation_id": correlation_id, "raw_id": raw_id})

    if decrypt_failed_reason:
        # Build payload JSON ensuring correlation_id is included
        payload_dict: dict
        if decrypted_body is not None:
            try:
                payload_dict = json.loads(decrypted_body)
            except Exception:
                payload_dict = {"raw": decrypted_body}
        elif isinstance(payload_data, dict):
            payload_dict = dict(payload_data)
        else:
            try:
                payload_dict = json.loads(original_body)
            except Exception:
                payload_dict = {"raw": original_body}

        payload_dict["correlation_id"] = correlation_id
        payload_for_event = json.dumps(payload_dict, default=str)
        # Publish dead event
        try:
            await publish_message_from_app(
                request.app,
                "lead.dead.decrypt_failed",
                {
                    "id_raw_payload": raw_id,
                    "id_processed_webhook": None,
                    "error_message": decrypt_failed_reason,
                    "gateway": gateway,
                    "received_at": str(received_at),
                    "payload": payload_for_event,
                },
            )
            logger.info(
                "Published decrypt_failed to RabbitMQ",
                extra={"correlation_id": correlation_id},
            )
        except Exception:
            logger.exception("Failed to publish decrypt_failed message")
        # Also persist into lead_dead_letter (best-effort)
        await _insert_dead_letter(
            db,
            correlation_id=correlation_id,
            origin=f"webhook.{gateway}",
            raw_payload_id=raw_id,
            payload=payload_for_event or "",
            error_message=decrypt_failed_reason,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    normalized_payload, invalid_email, invalid_phone = _normalize_payload(payload_data)

    try:
        Payload.model_validate(normalized_payload)
    except ValidationError as error:
        schema_valid = False
        schema_invalid_reason = json.dumps(error.errors(), default=str)
        validation_errors.extend(error.errors())
        logger.warning("Payload schema validation failed", extra={"correlation_id": correlation_id})

    if not schema_valid:
        reason = schema_invalid_reason or json.dumps(validation_errors, default=str)
        # Ensure correlation_id is included in the payload JSON we publish
        payload_for_event = (
            json.dumps({**normalized_payload, "correlation_id": correlation_id}, default=str)
            if normalized_payload
            else original_body
        )
        # Publish dead event
        try:
            await publish_message_from_app(
                request.app,
                "lead.dead.schema_invalid",
                {
                    "id_raw_payload": raw_id,
                    "id_processed_webhook": None,
                    "error_message": reason,
                    "gateway": gateway,
                    "received_at": str(received_at),
                    "payload": payload_for_event,
                },
            )
            logger.info(
                "Published schema_invalid to RabbitMQ",
                extra={"correlation_id": correlation_id},
            )
        except Exception:
            logger.exception("Failed to publish schema_invalid message")
        # Also persist into lead_dead_letter (best-effort)
        await _insert_dead_letter(
            db,
            correlation_id=correlation_id,
            origin=f"webhook.{gateway}",
            raw_payload_id=raw_id,
            payload=payload_for_event or "",
            error_message=reason,
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    transaction_id = normalized_payload.get("transaction_id")
    event = normalized_payload.get("event")

    if transaction_id and event:
        is_duplicate = await _check_idempotency(transaction_id, event, correlation_id, db)
        if is_duplicate:
            logger.info(
                "Duplicate webhook detected",
                extra={
                    "correlation_id": correlation_id,
                    "transaction_id": transaction_id,
                    "event": event,
                },
            )
            return DuplicateResponse(status="duplicate", correlation_id=correlation_id)

        processed_id = await _mark_processed(transaction_id, event, correlation_id, db)
        logger.info(
            "Webhook marked as processed",
            extra={
                "correlation_id": correlation_id,
                "transaction_id": transaction_id,
                "event": event,
                "processed_id": processed_id,
            },
        )

        # Publish when order.approved and payment.status == approved
        if event == "order.approved":
            payment_status = None
            payment = normalized_payload.get("payment")
            if isinstance(payment, dict):
                payment_status = payment.get("status")
            if payment_status == "approved":
                try:
                    # Inject correlation_id into the inner payload that we publish
                    payload_for_event = (
                        json.dumps({**normalized_payload, "correlation_id": correlation_id}, default=str)
                        if normalized_payload
                        else original_body
                    )
                    await publish_message_from_app(
                        request.app,
                        "lead.received",
                        {
                            "correlation_id": correlation_id,
                            "id_raw_payload": raw_id,
                            "id_processed_webhook": processed_id,
                            "error_message": None,
                            "gateway": gateway,
                            "received_at": str(received_at),
                            "payload": payload_for_event,
                        },
                    )
                    logger.info("Published lead.received to RabbitMQ", extra={"correlation_id": correlation_id})
                except Exception:
                    logger.exception("Failed to publish lead.received message")

    logger.info(
        "Webhook processing completed",
        extra={
            "correlation_id": correlation_id,
            "schema_valid": schema_valid,
            "invalid_email": invalid_email,
            "invalid_phone": invalid_phone,
        },
    )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
