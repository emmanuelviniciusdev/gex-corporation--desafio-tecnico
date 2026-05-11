# Integration API — Endpoints and Models

This document describes the endpoints implemented under app/routers and the related models (Pydantic I/O models and SQLAlchemy database models).

## Table of Contents
- Overview
- Endpoints
  - GET /health
  - POST /webhooks/{gateway}
- Models
  - I/O models (Pydantic)
  - Database models (SQLAlchemy)
- Examples
- Notes & config

---

## Overview
The service exposes a minimal integration API used to receive gateway webhooks and a health endpoint. The webhook receiver supports two gateway names: `grummer` and `lous`. `grummer` may send encrypted payloads; decryption uses AES-256-CBC with PKCS7 padding. The service persists raw payloads and tracks processed webhooks to provide idempotency.

---

## Endpoints

### GET /health
- Method: GET
- Path: `/health`
- Description: Basic health check endpoint that returns service status and application version.
- Response model: `HealthResponse`

Example response:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

Mermaid sequence (simple):
```mermaid
sequenceDiagram
  Client->>API: GET /health
  API->>API: read settings.app_version
  API-->>Client: 200 OK (HealthResponse)
```

---

### POST /webhooks/{gateway}
- Method: POST
- Path: `/webhooks/{gateway}` where `gateway` is either `grummer` or `lous`.
- Headers:
  - `X-GR-Encrypted` (required and must be `true` when gateway is `grummer` and payload is encrypted)
- Request body:
  - For unencrypted payloads: a JSON matching the `Payload` model.
  - For encrypted `grummer` payloads: a JSON matching `EncryptedPayload` ({ "iv": "...", "ciphertext": "..." }) and header `X-GR-Encrypted: true`.
- Side-effects:
  - Raw request and (optionally) decrypted body are persisted to the `raw_payloads` table.
  - Payload is normalized (email/phone normalization rules) and validated against the `Payload` model.
  - Idempotency: a Redis lock (`webhook:lock:{transaction_id}:{event}`) is attempted (TTL = 30s). If lock already held or a processed record exists for (transaction_id, event) the request is treated as a duplicate.
  - If not duplicate, a `processed_webhooks` record is created.
- Responses:
  - 204 No Content — standard successful processing (even when schema validation failed, the endpoint still returns 204 after persisting raw payload).
  - 200 OK with body `DuplicateResponse` if the webhook is detected as duplicate (shape: `{ "status": "duplicate", "correlation_id": "..." }`).

Important: validation errors are collected and logged; they are stored in `validation_errors` internally but the endpoint does not return them to the client.

Mermaid sequence diagram (high-level):
```mermaid
sequenceDiagram
  participant C as Client
  participant A as API
  participant R as Redis
  participant DB as Database

  C->>A: POST /webhooks/{gateway} + body + headers
  A->>A: parse body
  alt gateway == grummer and X-GR-Encrypted == true
    A->>A: validate EncryptedPayload
    A->>A: decrypt AES-256-CBC -> decrypted JSON
  end
  A->>A: normalize email/phone, fill defaults
  A->>A: validate Payload (transaction_time must contain timezone)
  A->>R: SETNX webhook:lock:{transaction_id}:{event} (TTL=30s)
  alt duplicate found
    R-->>A: lock not acquired or DB has existing processed record
    A-->>C: 200 OK + DuplicateResponse
  else not duplicate
    A->>DB: insert ProcessedWebhook(transaction_id, event, correlation_id)
    A->>DB: insert RawPayload(correlation_id, gateway, headers, original_body, decrypted_body)
    A-->>C: 204 No Content
  end
```

---

## Models
All models live under `app/models` (Pydantic I/O models) and `app/db` (SQLAlchemy database models). The following lists fields and constraints.

### I/O (Pydantic) models

- HealthResponse
  - `status: str` — e.g., "ok"
  - `version: str` — application version from settings

- CustomerPayload
  - `email: str`
  - `first_name: str | None` (optional, default None)
  - `phone: str | None` (optional)
  - `last_name: str`
  - `country: str` — must be 2 characters (ISO-2)

- ProductPayload
  - `id: str`
  - `name: str`
  - `niche: str`

- PaymentPayload
  - `amount_usd: float`
  - `method: Literal["credit_card", "paypal", "pix"]`
  - `status: Literal["approved", "declined", "pending", "refunded"]`

- Payload
  - `transaction_id: str`
  - `transaction_time: datetime` — MUST include timezone; validator enforces tzinfo is present
  - `event: Literal["order.approved", "order.refunded", "order.declined"]`
  - `customer: CustomerPayload`
  - `product: ProductPayload`
  - `quantity: int`
  - `payment: PaymentPayload`

- EncryptedPayload
  - `iv: str` — base64-encoded IV
  - `ciphertext: str` — base64-encoded ciphertext

- WebhookResponse (internal model, not currently returned by the endpoint)
  - `correlation_id: str`
  - `gateway: Literal["grummer", "lous"]`
  - `schema_valid: bool`
  - `invalid_email: bool`
  - `invalid_phone: bool`
  - `normalized_payload: dict`
  - `validation_errors: list[dict]`

- DuplicateResponse
  - `status: Literal["duplicate"]`
  - `correlation_id: str`

Mermaid class diagram for the I/O models (simplified):
```mermaid
classDiagram
  class Payload {
    +str transaction_id
    +datetime transaction_time
    +str event
    +CustomerPayload customer
    +ProductPayload product
    +int quantity
    +PaymentPayload payment
  }
  class CustomerPayload {
    +str email
    +str first_name?
    +str phone?
    +str last_name
    +str country(2)
  }
  class ProductPayload {+str id\n+str name\n+str niche}
  class PaymentPayload {+float amount_usd\n+str method\n+str status}

  Payload --> CustomerPayload
  Payload --> ProductPayload
  Payload --> PaymentPayload
```

### Database (SQLAlchemy) models

- RawPayload (table `raw_payloads`)
  - `id: int` (PK)
  - `correlation_id: str` (36)
  - `gateway: str` (20)
  - `received_at: datetime(timezone=True)`
  - `headers: JSON` — request headers stored as JSON
  - `original_body: text` — the exact raw body received
  - `decrypted_body: text | None` — decrypted JSON (string) when applicable
  - `created_at: datetime(timezone=True)` — insertion time
  - Indexes: correlation_id, gateway, received_at

- ProcessedWebhook (table `processed_webhooks`)
  - `id: int` (PK)
  - `transaction_id: str` (255)
  - `event: str` (50)
  - `correlation_id: str` (36)
  - `processed_at: datetime(timezone=True)`
  - Unique index on (`transaction_id`, `event`)

Mermaid class diagram for DB models:
```mermaid
classDiagram
  class RawPayload {
    +int id
    +str correlation_id
    +str gateway
    +datetime received_at
    +JSON headers
    +text original_body
    +text decrypted_body?
    +datetime created_at
  }
  class ProcessedWebhook {
    +int id
    +str transaction_id
    +str event
    +str correlation_id
    +datetime processed_at
  }
```

---

## Examples

Health check:
```bash
curl -sS http://localhost:8000/health
```

Webhook (lous, plain JSON):
```bash
curl -X POST "http://localhost:8000/webhooks/lous" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id":"tx_123",
    "transaction_time":"2026-05-09T14:00:00+00:00",
    "event":"order.approved",
    "customer": {"email":"customer@example.com","first_name":"John","last_name":"Doe","phone":"+15551234567","country":"US"},
    "product": {"id":"prod_1","name":"Widget","niche":"gadgets"},
    "quantity":1,
    "payment": {"amount_usd":10.5,"method":"credit_card","status":"approved"}
  }'
```

Webhook (grummer, encrypted):
- Request: POST `/webhooks/grummer` with header `X-GR-Encrypted: true` and body matching `EncryptedPayload`.
- The server will attempt to base64-decode and AES-256-CBC decrypt the payload using `settings.grummer_aes256_key_base64`.

Duplicate response example (when duplicate detected):
```json
{ "status": "duplicate", "correlation_id": "<uuid>" }
```

---

## Notes & configuration
- Decryption: `settings.grummer_aes256_key_base64` must be set (base64-encoded 32 bytes key) for grummer AES decryption to work.
- Redis is required for the idempotency lock — `get_redis()` is used.
- Database is required to persist `raw_payloads` and `processed_webhooks`.
- The endpoint enforces that `transaction_time` includes timezone information; otherwise schema validation will fail.
- Idempotency lock TTL is 30 seconds and Redis lock key prefix is `webhook:lock:`.

## RabbitMQ integration
This service can publish events to RabbitMQ. When running via the provided docker-compose the repository includes a RabbitMQ service (management UI exposed on port 15672).

Configuration:
- Environment variable: `RABBITMQ_URL` (e.g. `amqp://guest:guest@rabbitmq:5672/`)
- FastAPI will create a RabbitPublisher instance on startup and connect lazily on first publish.

Published routing keys / queues:
- `lead.received` — published when payload is valid AND `event == "order.approved"` AND `payment.status == "approved"`.
- `lead.dead.decrypt_failed` — published when decryption or EncryptedPayload validation fails (contains an error message).
- `lead.dead.schema_invalid` — published when normalized payload fails schema validation (contains an error message).

Message schema (JSON):
```
{
  "id_raw_payload": int,
  "id_processed_webhook": int | null,
  "payload": string,                
  "error_message": string | null
}
```

Notes:
- Publishing is best-effort in this implementation: if RabbitMQ is not reachable or the publisher is not initialized messages are dropped and a warning is logged.
- For stronger delivery guarantees consider implementing an outbox pattern or transactional publishing.

---
