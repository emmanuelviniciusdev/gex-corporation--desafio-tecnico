### Integration API

FastAPI application that receives webhooks from external payment gateways, validates/normalizes payloads, enforces idempotency, persists raw/audit data, and publishes events to RabbitMQ for downstream processing.

#### Key responsibilities
- HTTP ingestion of webhooks at `POST /webhooks/{gateway}` for gateways: `grummer` and `lous`.
- Decryption support for `grummer` (AES‑256‑CBC with PKCS7) when header `X-GR-Encrypted: true` is present.
- Input normalization (email/phone) and Pydantic schema validation.
- Redis‑backed idempotency lock and `processed_webhooks` register to avoid double processing.
- Persistence of raw payloads and best‑effort dead‑letter rows in MySQL.
- Publishing of domain events to RabbitMQ:
  - `lead.dead.decrypt_failed`
  - `lead.dead.schema_invalid`
  - `lead.received` (only when `event == order.approved` and `payment.status == approved`)

#### High‑level architecture

```mermaid
flowchart LR
  subgraph External
    G1[Gateway: grummer]
    G2[Gateway: lous]
  end

  G1 & G2 -->|HTTP POST /webhooks/:gateway| API[Integration API]
  API -->|persist raw payload| DB[(MySQL)]
  API -->|idempotency check| R[(Redis)]
  API -->|publish events| MQ[(RabbitMQ)]

  MQ -->|lead.received| C1[integration_consumer_webhook]
  C1 -->|dist.*| MQ
  MQ -->|dist.sms / dist.email / dist.call_center / dist.whatsapp| C2[integration_consumer_channels]
  C2 -->|update distribution_status| DB

  API -.->|dead letters on decrypt/schema errors| DLQ[(lead_dead_letter in DB)]
```

#### Endpoints
- `GET /health` → returns `{status: "ok", version: <app_version>}`
- `POST /webhooks/{gateway}`
  - Path param `gateway`: `grummer` or `lous`
  - For `grummer`: header `X-GR-Encrypted: true` and body `{ iv, ciphertext }` (Base64). Decrypted JSON must match `Payload` schema.
  - For `lous`: unencrypted JSON body matching `Payload` schema.
  - Always persists a row in `raw_payloads` (with decrypted copy when applicable).
  - Normalizes `customer.email` (lowercase) and `customer.phone` (E.164‑ish, best‑effort) and validates schema.
  - Idempotency: combines Redis lock and `processed_webhooks` table to drop duplicates; responds with `{status: "duplicate", correlation_id}`.
  - Emits `lead.received` only if `event == "order.approved"` and `payment.status == "approved"`.
  - On decrypt or schema failure: publishes `lead.dead.*` event and inserts a row into `lead_dead_letter`.
  - Response: `204 No Content` (or 200 for duplicate payloads).

#### Environment variables (see `.env.example`)
- `APP_NAME` (default: Integration API)
- `APP_VERSION` (default: 0.1.0)
- `DEBUG` (bool)
- `DATABASE_URL` (e.g., `mysql+pymysql://root:root@mysql:3306/db_integration`)
- `REDIS_URL` (e.g., `redis://redis:6379/0`)
- `RABBITMQ_URL` (e.g., `amqp://guest:guest@rabbitmq:5672/`)
- `GRUMMER_AES256_KEY_BASE64` (AES‑256 key; accepts raw‑bytes Base64, Base64‑of‑hex, or 64‑hex string)

#### Data model (MySQL)
- `raw_payloads`: raw HTTP payloads + decrypted body (when available)
- `processed_webhooks`: idempotency ledger per `(transaction_id, event)`
- `leads`: normalized customer data (unique by email)
- `orders`: one per gateway transaction; unique by `(gateway, transaction_id)`
- `lead_events`: order‑scoped events with gateway time and persistence lag
- `distribution_status`: per‑order per‑channel delivery state
- `lead_dead_letter`: durable record of dead messages (origin + error + payload)

Core DDL and stored procedure are under `integration_api/sql/migration`:
- `000_create_databases.sql`, `001_create_tables.sql`, `002_create_stored_procedures.sql`, `003_add_audit_indexes.sql`.

#### Event contracts
- `lead.received`
  - Example body (outer envelope published by API):
    ```json
    {
      "correlation_id": "<uuid>",
      "id_raw_payload": 123,
      "id_processed_webhook": 456,
      "error_message": null,
      "gateway": "grummer",
      "received_at": "2026-01-01T12:00:00+00:00",
      "payload": { "transaction_id": "...", "event": "order.approved", "customer": {"email": "..."}, "payment": {"status": "approved"}, "correlation_id": "<uuid>" }
    }
    ```
- `lead.dead.decrypt_failed` / `lead.dead.schema_invalid` mirror the same shape, with `error_message` filled and nested `payload` preserving/including `correlation_id`.

#### Run locally

The easiest way is via Docker Compose from the project root:

```bash
docker compose up -d mysql redis rabbitmq
docker compose up -d integration-api integration-consumer-webhook integration-consumer-channels
# (optional) send sample traffic via the test runner once API is healthy
docker compose run --rm test-runner
```

Service endpoints once up:
- API: http://localhost:8080/health
- RabbitMQ Management UI: http://localhost:15672 (guest/guest)
- MySQL: localhost:3306 (user: root / pass: root by compose)
- Redis: localhost:6379

#### Sample requests

Unencrypted (lous):
```bash
curl -X POST http://localhost:8080/webhooks/lous \
  -H 'Content-Type: application/json' \
  -d '{
    "transaction_id": "tx-001",
    "transaction_time": "2026-01-01T12:00:00Z",
    "event": "order.approved",
    "customer": {"email": "USER@example.com", "first_name": "Ann", "last_name": "Lee", "phone": "+5511999999999", "country": "BR"},
    "product": {"id": "P1", "name": "Widget", "niche": "tools", "quantity": 1},
    "payment": {"amount_usd": 10.0, "method": "credit_card", "status": "approved"}
  }'
```

Encrypted (grummer): send `{iv, ciphertext}` JSON and header `X-GR-Encrypted: true`. The AES‑256 key format is controlled by `GRUMMER_AES256_KEY_BASE64`.

#### Notes and caveats
- Idempotency window is guarded by Redis (`webhook:lock:<transaction_id>:<event>` with short TTL) and permanently by the `processed_webhooks` table.
- Dead‑letter persistence aims to never break the request path; failures are logged and swallowed.
