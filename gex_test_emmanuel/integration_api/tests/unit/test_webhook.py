import base64
import json
from unittest.mock import patch

import pytest
from tests.conftest import VALID_PAYLOAD, encrypt_payload

from app.models.webhook import EncryptedPayload
from app.routers.webhook import (
    _decrypt_grummer_payload,
    _normalize_email,
    _normalize_payload,
    _normalize_phone,
)


class TestNormalizeEmail:
    def test_valid_email_normalized(self):
        email, invalid = _normalize_email("  TEST@EXAMPLE.COM  ")
        assert email == "test@example.com"
        assert invalid is False

    def test_invalid_email_no_at(self):
        email, invalid = _normalize_email("invalid-email")
        assert email == "invalid-email"
        assert invalid is True

    def test_invalid_email_no_domain(self):
        email, invalid = _normalize_email("test@")
        assert invalid is True

    def test_invalid_email_no_local(self):
        email, invalid = _normalize_email("@example.com")
        assert invalid is True

    def test_empty_email(self):
        email, invalid = _normalize_email("")
        assert email == ""
        assert invalid is True

    def test_email_with_spaces_only(self):
        email, invalid = _normalize_email("   ")
        assert email == ""
        assert invalid is True

    def test_valid_email_with_subdomain(self):
        email, invalid = _normalize_email("user@mail.example.com")
        assert email == "user@mail.example.com"
        assert invalid is False


class TestNormalizePhone:
    def test_none_phone(self):
        phone, invalid = _normalize_phone(None)
        assert phone is None
        assert invalid is False

    def test_empty_phone(self):
        phone, invalid = _normalize_phone("")
        assert phone is None
        assert invalid is False

    def test_phone_with_plus(self):
        phone, invalid = _normalize_phone("+1 (800) 555-1234")
        assert phone == "+18005551234"
        assert invalid is False

    def test_phone_10_digits(self):
        phone, invalid = _normalize_phone("8005551234")
        assert phone == "+18005551234"
        assert invalid is False

    def test_phone_11_digits(self):
        phone, invalid = _normalize_phone("18005551234")
        assert phone == "+118005551234"
        assert invalid is False

    def test_phone_with_formatting(self):
        phone, invalid = _normalize_phone("(800) 555-1234")
        assert phone == "+18005551234"
        assert invalid is False

    def test_short_phone_invalid(self):
        phone, invalid = _normalize_phone("1234")
        assert phone == "1234"
        assert invalid is True

    def test_international_phone(self):
        phone, invalid = _normalize_phone("+44 20 7946 0958")
        assert phone == "+442079460958"
        assert invalid is False


class TestNormalizePayload:
    def test_normalize_email_in_payload(self):
        payload = {"customer": {"email": "  TEST@EXAMPLE.COM  "}}
        normalized, invalid_email, invalid_phone = _normalize_payload(payload)
        assert normalized["customer"]["email"] == "test@example.com"
        assert invalid_email is False

    def test_normalize_phone_in_payload(self):
        payload = {"customer": {"email": "test@example.com", "phone": "(800) 555-1234"}}
        normalized, invalid_email, invalid_phone = _normalize_payload(payload)
        assert normalized["customer"]["phone"] == "+18005551234"
        assert invalid_phone is False

    def test_empty_first_name_defaults_to_customer(self):
        payload = {"customer": {"email": "test@example.com", "first_name": ""}}
        normalized, _, _ = _normalize_payload(payload)
        assert normalized["customer"]["first_name"] == "Customer"

    def test_whitespace_first_name_defaults_to_customer(self):
        payload = {"customer": {"email": "test@example.com", "first_name": "   "}}
        normalized, _, _ = _normalize_payload(payload)
        assert normalized["customer"]["first_name"] == "Customer"

    def test_none_first_name_defaults_to_customer(self):
        payload = {"customer": {"email": "test@example.com", "first_name": None}}
        normalized, _, _ = _normalize_payload(payload)
        assert normalized["customer"]["first_name"] == "Customer"

    def test_valid_first_name_preserved(self):
        payload = {"customer": {"email": "test@example.com", "first_name": "  John  "}}
        normalized, _, _ = _normalize_payload(payload)
        assert normalized["customer"]["first_name"] == "John"

    def test_missing_customer_creates_empty(self):
        payload = {}
        normalized, invalid_email, invalid_phone = _normalize_payload(payload)
        assert "customer" in normalized
        assert invalid_email is True

    def test_other_fields_preserved(self):
        payload = {"customer": {"email": "test@example.com"}, "extra_field": "value"}
        normalized, _, _ = _normalize_payload(payload)
        assert normalized["extra_field"] == "value"


class TestDecryptGrummerPayload:
    def test_decrypt_valid_payload(self):
        plaintext = json.dumps({"test": "data"}).encode()
        encrypted = encrypt_payload(plaintext)
        encrypted_payload = EncryptedPayload.model_validate(encrypted)
        result = _decrypt_grummer_payload(encrypted_payload)
        assert result == {"test": "data"}

    def test_decrypt_complex_payload(self):
        plaintext = json.dumps(VALID_PAYLOAD).encode()
        encrypted = encrypt_payload(plaintext)
        encrypted_payload = EncryptedPayload.model_validate(encrypted)
        result = _decrypt_grummer_payload(encrypted_payload)
        assert result == VALID_PAYLOAD

    def test_decrypt_with_missing_key(self):
        with patch("app.routers.webhook.settings") as mock_settings:
            mock_settings.grummer_aes256_key_base64 = None
            encrypted_payload = EncryptedPayload(iv="dGVzdA==", ciphertext="dGVzdA==")
            with pytest.raises(ValueError, match="missing grummer key"):
                _decrypt_grummer_payload(encrypted_payload)

    def test_decrypt_with_invalid_key_length(self):
        with patch("app.routers.webhook.settings") as mock_settings:
            mock_settings.grummer_aes256_key_base64 = base64.b64encode(b"short").decode()
            encrypted_payload = EncryptedPayload(iv="dGVzdA==", ciphertext="dGVzdA==")
            with pytest.raises(ValueError, match="invalid grummer key"):
                _decrypt_grummer_payload(encrypted_payload)
