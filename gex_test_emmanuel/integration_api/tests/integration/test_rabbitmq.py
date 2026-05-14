import json
from unittest.mock import AsyncMock, patch

from tests.conftest import VALID_PAYLOAD


def test_publish_lead_received_on_order_approved(client):
    with patch("app.routers.webhook.publish_message_from_app", new=AsyncMock()) as mock_publish:
        resp = client.post("/webhooks/lous", json=VALID_PAYLOAD)
        assert resp.status_code == 204
        mock_publish.assert_called_once()
        called = mock_publish.call_args[0]
        # signature: (app, routing_key, message)
        assert called[1] == "lead.received"
        payload = called[2]
        assert payload["error_message"] is None
        assert isinstance(payload["id_raw_payload"], int)
        assert isinstance(payload["id_processed_webhook"], int)
        assert payload["gateway"] == "lous"
        assert "received_at" in payload
        assert isinstance(payload["received_at"], str)
        assert "payload" in payload
        assert isinstance(payload["payload"], str)
        decoded = json.loads(payload["payload"])
        assert decoded["transaction_id"] == "123"
        # All messages' payloads must include correlation_id
        assert "correlation_id" in decoded and isinstance(decoded["correlation_id"], str)
def test_no_publish_when_payment_not_approved(client):
    payload = dict(VALID_PAYLOAD)
    payload["payment"] = dict(payload["payment"])
    payload["payment"]["status"] = "declined"

    with patch("app.routers.webhook.publish_message_from_app", new=AsyncMock()) as mock_publish:
        resp = client.post("/webhooks/lous", json=payload)
        assert resp.status_code == 204
        mock_publish.assert_not_called()


def test_publish_dead_on_decrypt_failed(client):
    with patch("app.routers.webhook.publish_message_from_app", new=AsyncMock()) as mock_publish:
        resp = client.post(
            "/webhooks/grummer",
            json={"iv": "invalid", "ciphertext": "invalid"},
            headers={"X-GR-Encrypted": "true"},
        )
        assert resp.status_code == 204
        mock_publish.assert_called_once()
        called = mock_publish.call_args[0]
        assert called[1] == "lead.dead.decrypt_failed"
        payload = called[2]
        assert payload["id_raw_payload"] is not None
        assert payload["id_processed_webhook"] is None
        assert payload["error_message"] is not None
        assert payload["gateway"] == "grummer"
        assert "payload" in payload
        assert isinstance(payload["payload"], str)
        decoded = json.loads(payload["payload"])
        assert "correlation_id" in decoded and isinstance(decoded["correlation_id"], str)
        # decrypted or original payload should contain either iv (grummer) or transaction_id
        assert "iv" in decoded or "transaction_id" in decoded


def test_publish_dead_on_schema_invalid(client):
    with patch("app.routers.webhook.publish_message_from_app", new=AsyncMock()) as mock_publish:
        resp = client.post("/webhooks/lous", json={"transaction_id": "missing-fields"})
        assert resp.status_code == 204
        mock_publish.assert_called_once()
        called = mock_publish.call_args[0]
        assert called[1] == "lead.dead.schema_invalid"
        payload = called[2]
        assert payload["id_raw_payload"] is not None
        assert payload["id_processed_webhook"] is None
        assert payload["error_message"] is not None
        assert payload["gateway"] == "lous"
        assert "payload" in payload
        assert isinstance(payload["payload"], str)
        decoded = json.loads(payload["payload"])
        assert "transaction_id" in decoded
        assert "correlation_id" in decoded and isinstance(decoded["correlation_id"], str)
