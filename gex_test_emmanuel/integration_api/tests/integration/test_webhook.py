import json

from app.db import ProcessedWebhook, RawPayload
from tests.conftest import VALID_PAYLOAD, TestSession, encrypt_payload, mock_redis


class TestWebhookIntegrationLous:
    def test_valid_payload_returns_204(self, client):
        resp = client.post("/webhooks/lous", json=VALID_PAYLOAD)
        assert resp.status_code == 204
        assert resp.content == b""

    def test_duplicate_detection_returns_duplicate_response(self, client):
        resp1 = client.post("/webhooks/lous", json=VALID_PAYLOAD)
        assert resp1.status_code == 204

        resp2 = client.post("/webhooks/lous", json=VALID_PAYLOAD)
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["status"] == "duplicate"
        assert "correlation_id" in data

    def test_same_transaction_different_event_not_duplicate(self, client):
        resp1 = client.post("/webhooks/lous", json=VALID_PAYLOAD)
        assert resp1.status_code == 204

        payload2 = dict(VALID_PAYLOAD)
        payload2["event"] = "order.refunded"
        resp2 = client.post("/webhooks/lous", json=payload2)
        assert resp2.status_code == 204

        db = TestSession()
        count = db.query(ProcessedWebhook).filter(ProcessedWebhook.transaction_id == "123").count()
        db.close()
        assert count == 2

    def test_raw_payload_persisted(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD)
        db = TestSession()
        count = db.query(RawPayload).count()
        raw = db.query(RawPayload).first()
        db.close()
        assert count >= 1
        assert raw.gateway == "lous"
        assert raw.correlation_id is not None
        assert raw.decrypted_body is None

    def test_processed_webhook_persisted(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD)
        db = TestSession()
        processed = db.query(ProcessedWebhook).first()
        db.close()
        assert processed is not None
        assert processed.transaction_id == "123"
        assert processed.event == "order.approved"

    def test_invalid_json_returns_204(self, client):
        resp = client.post(
            "/webhooks/lous", content="not valid json", headers={"Content-Type": "application/json"}
        )
        assert resp.status_code == 204

    def test_invalid_gateway_returns_422(self, client):
        resp = client.post("/webhooks/invalid", json=VALID_PAYLOAD)
        assert resp.status_code == 422

    def test_empty_payload_returns_204(self, client):
        resp = client.post("/webhooks/lous", json={})
        assert resp.status_code == 204

    def test_missing_transaction_id_still_persists_raw(self, client):
        payload = dict(VALID_PAYLOAD)
        del payload["transaction_id"]
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204

        db = TestSession()
        count = db.query(RawPayload).count()
        db.close()
        assert count >= 1

    def test_invalid_email_still_returns_204(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "456"
        payload["customer"] = dict(VALID_PAYLOAD["customer"])
        payload["customer"]["email"] = "invalid-email"
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204

    def test_invalid_phone_still_returns_204(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "789"
        payload["customer"] = dict(VALID_PAYLOAD["customer"])
        payload["customer"]["phone"] = "123"
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204


class TestWebhookIntegrationGrummer:
    def test_encrypted_payload_returns_204(self, client):
        encrypted = encrypt_payload(json.dumps(VALID_PAYLOAD).encode())
        resp = client.post("/webhooks/grummer", json=encrypted, headers={"X-GR-Encrypted": "true"})
        assert resp.status_code == 204

    def test_missing_encrypted_header_returns_204(self, client):
        resp = client.post("/webhooks/grummer", json=VALID_PAYLOAD)
        assert resp.status_code == 204

    def test_encrypted_header_false_returns_204(self, client):
        resp = client.post(
            "/webhooks/grummer", json=VALID_PAYLOAD, headers={"X-GR-Encrypted": "false"}
        )
        assert resp.status_code == 204

    def test_decrypted_body_persisted(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "grummer-123"
        encrypted = encrypt_payload(json.dumps(payload).encode())
        client.post("/webhooks/grummer", json=encrypted, headers={"X-GR-Encrypted": "true"})

        db = TestSession()
        raw = db.query(RawPayload).filter(RawPayload.gateway == "grummer").first()
        db.close()

        assert raw is not None
        assert raw.decrypted_body is not None
        decrypted = json.loads(raw.decrypted_body)
        assert decrypted["transaction_id"] == "grummer-123"

    def test_invalid_encrypted_payload_returns_204(self, client):
        resp = client.post(
            "/webhooks/grummer",
            json={"iv": "invalid", "ciphertext": "invalid"},
            headers={"X-GR-Encrypted": "true"},
        )
        assert resp.status_code == 204

    def test_missing_iv_returns_204(self, client):
        resp = client.post(
            "/webhooks/grummer", json={"ciphertext": "dGVzdA=="}, headers={"X-GR-Encrypted": "true"}
        )
        assert resp.status_code == 204

    def test_missing_ciphertext_returns_204(self, client):
        resp = client.post(
            "/webhooks/grummer", json={"iv": "dGVzdA=="}, headers={"X-GR-Encrypted": "true"}
        )
        assert resp.status_code == 204

    def test_duplicate_encrypted_webhook(self, client):
        encrypted = encrypt_payload(json.dumps(VALID_PAYLOAD).encode())

        resp1 = client.post("/webhooks/grummer", json=encrypted, headers={"X-GR-Encrypted": "true"})
        assert resp1.status_code == 204

        resp2 = client.post("/webhooks/grummer", json=encrypted, headers={"X-GR-Encrypted": "true"})
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["status"] == "duplicate"


class TestIdempotency:
    def test_idempotency_key_format(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD)

        key = f"webhook:lock:{VALID_PAYLOAD['transaction_id']}:{VALID_PAYLOAD['event']}"
        assert mock_redis.exists(key)

    def test_idempotency_lock_expires(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD)

        key = f"webhook:lock:{VALID_PAYLOAD['transaction_id']}:{VALID_PAYLOAD['event']}"
        ttl = mock_redis.ttl(key)
        assert 0 < ttl <= 30

    def test_concurrent_requests_handled(self, client):
        payload1 = dict(VALID_PAYLOAD)
        payload1["transaction_id"] = "concurrent-1"

        payload2 = dict(VALID_PAYLOAD)
        payload2["transaction_id"] = "concurrent-2"

        resp1 = client.post("/webhooks/lous", json=payload1)
        resp2 = client.post("/webhooks/lous", json=payload2)

        assert resp1.status_code == 204
        assert resp2.status_code == 204

        db = TestSession()
        count = db.query(ProcessedWebhook).count()
        db.close()
        assert count == 2


class TestPayloadValidation:
    def test_invalid_country_code_length(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "invalid-country"
        payload["customer"] = dict(VALID_PAYLOAD["customer"])
        payload["customer"]["country"] = "USA"
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204

    def test_invalid_payment_method(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "invalid-method"
        payload["payment"] = dict(VALID_PAYLOAD["payment"])
        payload["payment"]["method"] = "bitcoin"
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204

    def test_invalid_event_type(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "invalid-event"
        payload["event"] = "order.unknown"
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204

    def test_missing_required_fields(self, client):
        payload = {"transaction_id": "missing-fields"}
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204

    def test_transaction_time_without_timezone(self, client):
        payload = dict(VALID_PAYLOAD)
        payload["transaction_id"] = "no-timezone"
        payload["transaction_time"] = "2024-01-01T12:00:00"
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204


class TestRawPayloadPersistence:
    def test_headers_persisted(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD, headers={"X-Custom-Header": "test-value"})

        db = TestSession()
        raw = db.query(RawPayload).first()
        db.close()

        assert raw is not None
        assert "x-custom-header" in raw.headers
        assert raw.headers["x-custom-header"] == "test-value"

    def test_original_body_persisted(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD)

        db = TestSession()
        raw = db.query(RawPayload).first()
        db.close()

        assert raw is not None
        original = json.loads(raw.original_body)
        assert original["transaction_id"] == "123"

    def test_received_at_persisted(self, client):
        client.post("/webhooks/lous", json=VALID_PAYLOAD)

        db = TestSession()
        raw = db.query(RawPayload).first()
        db.close()

        assert raw is not None
        assert raw.received_at is not None
