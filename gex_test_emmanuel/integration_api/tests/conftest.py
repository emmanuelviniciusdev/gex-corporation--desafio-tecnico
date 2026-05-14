import base64
import os
import tempfile
from unittest.mock import patch

TEST_KEY = b"0123456789abcdef0123456789abcdef"
TEST_KEY_B64 = base64.b64encode(TEST_KEY).decode()

# Create a temporary sqlite file database so both async and sync engines can
# access the same database during tests.
_tmp_db = tempfile.NamedTemporaryFile(prefix="test_db_", suffix=".db", delete=False)
_db_path = _tmp_db.name
_tmp_db.close()

os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_db_path}"
os.environ["GRUMMER_AES256_KEY_BASE64"] = TEST_KEY_B64

import fakeredis  # noqa: E402
import pytest  # noqa: E402
from cryptography.hazmat.primitives.ciphers import (  # noqa: E402
    Cipher,
    algorithms,
    modes,
)
from cryptography.hazmat.primitives.padding import PKCS7  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from app.core.database import Base, async_session, get_db  # noqa: E402
from app.db import ProcessedWebhook, RawPayload  # noqa: E402
from app.main import app  # noqa: E402

# Create a synchronous engine bound to the same sqlite file so tests can run
# synchronous queries directly for assertions.
engine = create_engine(f"sqlite:///{_db_path}", connect_args={"check_same_thread": False})
Base.metadata.create_all(bind=engine)
TestSession = sessionmaker(bind=engine)

_current_mock_redis = None


async def override_get_db():
    async with async_session() as session:
        yield session


app.dependency_overrides[get_db] = override_get_db


def encrypt_payload(plaintext: bytes) -> dict:
    iv = os.urandom(16)
    padder = PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    cipher = Cipher(algorithms.AES(TEST_KEY), modes.CBC(iv))
    encryptor = cipher.encryptor()
    ct = encryptor.update(padded) + encryptor.finalize()
    return {"iv": base64.b64encode(iv).decode(), "ciphertext": base64.b64encode(ct).decode()}


VALID_PAYLOAD = {
    "transaction_id": "123",
    "transaction_time": "2024-01-01T12:00:00+00:00",
    "event": "order.approved",
    "customer": {
        "email": "  TEST@EXAMPLE.COM  ",
        "first_name": "",
        "phone": "(800) 555-1234",
        "last_name": "Doe",
        "country": "US",
    },
    "product": {"id": "p1", "name": "Product", "niche": "Tech", "quantity": 1},
    "payment": {"amount_usd": 99.99, "method": "credit_card", "status": "approved"},
}


def get_mock_redis():
    """Return the current mock redis instance.

    Pytest may import this conftest module under multiple module names
    (e.g. "conftest" and "tests.conftest"). Try to find the live
    instance from whichever module was used to run the client fixture.
    """
    if _current_mock_redis is not None:
        return _current_mock_redis

    import sys

    # Common module names pytest may use for conftest
    for mname in ("conftest", "tests.conftest"):
        mod = sys.modules.get(mname)
        if mod is None:
            continue
        mr = getattr(mod, "_current_mock_redis", None)
        if mr is not None:
            return mr
    return None


@pytest.fixture(scope="function")
def client():
    global _current_mock_redis

    db = TestSession()
    db.query(ProcessedWebhook).delete()
    db.query(RawPayload).delete()
    db.commit()
    db.close()

    _current_mock_redis = fakeredis.FakeStrictRedis(decode_responses=True)

    with patch("app.routers.webhook.get_redis", return_value=_current_mock_redis):
        yield TestClient(app)


@pytest.fixture
def db_session():
    db = TestSession()
    try:
        yield db
    finally:
        db.close()


class _MockRedisProxy:
    """A lightweight proxy object that forwards attribute access to the
    currently active fakeredis instance created by the `client` fixture.

    Tests import `mock_redis` at import time but the real fakeredis instance
    is only created when the `client` fixture runs. This proxy allows tests
    to call mock_redis.exists(...) etc. after the `client` fixture sets
    `_current_mock_redis`.
    """

    def __getattr__(self, name):
        mr = get_mock_redis()
        if mr is None:
            raise RuntimeError("mock_redis accessed before client fixture sets the redis instance")
        return getattr(mr, name)


# module-level proxy so tests can import mock_redis and use it directly
mock_redis = _MockRedisProxy()


# When tests import conftest via a package path (e.g. "tests.conftest") pytest
# may still register the module under the plain "conftest" name. Ensure both
# entries point to the same module object so fixtures and helpers share state.
import sys as _sys  # noqa: E402

_this = _sys.modules[__name__]
_sys.modules.setdefault("conftest", _this)
_sys.modules.setdefault("tests.conftest", _this)
