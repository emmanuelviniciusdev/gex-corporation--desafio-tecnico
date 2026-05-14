from datetime import UTC, datetime, timedelta
import json

import pytest

from services import sms


@pytest.mark.asyncio
async def test_handler_calls_mark_delivered(monkeypatch):
    called = {}

    async def fake_process_with_retry(msg_obj, session, publish_channel):
        called["processed"] = True
        return True, None

    async def fake_mark_delivered(pool, order_id, channel):
        called["delivered"] = (order_id, channel)

    monkeypatch.setattr(sms, "_process_with_retry", fake_process_with_retry)
    monkeypatch.setattr(sms, "_mark_delivered", fake_mark_delivered)

    handler = sms.make_handler(pool="pool", publish_channel=None)
    msg_obj = {"order_id": 42, "transaction_id": "tx", "channel": "SMS", "payload": {}}
    await handler(msg_obj)

    assert called.get("processed") is True
    assert called.get("delivered") == (42, "SMS")


@pytest.mark.asyncio
async def test_mark_delivered_updates_db_and_logs(caplog):
    class FakeCursor:
        def __init__(self, created_at):
            self._created_at = created_at
            self.executed = []

        async def execute(self, query, params=None):
            self.executed.append((query, params))

        async def fetchone(self):
            return (self._created_at,)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def __init__(self, cursor):
            self._cursor = cursor
            self.committed = False

        def cursor(self):
            return self._cursor

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            self.committed = True

    class FakeAcquire:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakePool:
        def __init__(self, conn):
            self._conn = conn

        def acquire(self):
            return FakeAcquire(self._conn)

    created_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=10)
    fake_cursor = FakeCursor(created_at)
    fake_conn = FakeConn(fake_cursor)
    fake_pool = FakePool(fake_conn)

    caplog.set_level("INFO")
    await sms._mark_delivered(fake_pool, 123, "SMS")

    # ensure SELECT executed
    assert any("SELECT created_at FROM distribution_status" in q for q, _ in fake_cursor.executed)

    # ensure UPDATE executed
    update_executed = [item for item in fake_cursor.executed if item[0].strip().upper().startswith("UPDATE DISTRIBUTION_STATUS")]
    assert update_executed, "UPDATE not executed"

    update_params = update_executed[-1][1]
    assert isinstance(update_params[0], int)
    assert update_params[1] == 123
    assert update_params[2] == "SMS"

    assert "marked distribution_status delivered" in caplog.text


@pytest.mark.asyncio
async def test_consumer_failed_publishes_payload_with_correlation_id(monkeypatch):
    # Force processing to fail so dead-letter is published
    async def always_fail_post(msg_obj, session):
        raise sms.ProcessingError("forced failure for test")

    published = {}

    class FakeExchange:
        async def publish(self, message, routing_key=""):
            published.setdefault("calls", []).append((message, routing_key))

    class FakeChannel:
        def __init__(self):
            self.default_exchange = FakeExchange()

    class FakeClientSession:
        def __init__(self, *_, **__):
            pass

        # used by _post_once via `async with session.post(...):`
        class _Resp:
            def __init__(self):
                self.status = 200
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc, tb):
                return False
            async def text(self):
                return ""

        def post(self, *args, **kwargs):
            return self._Resp()

    monkeypatch.setattr(sms, "_post_once", always_fail_post)
    monkeypatch.setattr(sms.aiohttp, "ClientSession", FakeClientSession)

    handler = sms.make_handler(pool=None, publish_channel=FakeChannel())
    msg_obj = {
        "order_id": 7,
        "transaction_id": "tx-7",
        "channel": "SMS",
        "received_at": datetime.now(UTC).isoformat(),
        "payload": {"transaction_id": "tx-7", "correlation_id": "corr-123"},
    }

    await handler(msg_obj)

    # ensure a dead-letter was published and it carries correlation_id inside nested payload
    assert published.get("calls"), "no publish calls captured"
    # Only one dead-letter expected
    message, routing_key = published["calls"][0]
    body = getattr(message, "body", b"")
    obj = json.loads(body.decode("utf-8")) if isinstance(body, (bytes, bytearray)) else json.loads(body)
    assert obj.get("payload", {}).get("correlation_id") == "corr-123"


@pytest.mark.asyncio
async def test_process_with_retry_success_does_not_log_unexpected_error(caplog):
    class FakeClientSession:
        class _Resp:
            def __init__(self):
                self.status = 200
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc, tb):
                return False
            async def text(self):
                return ""

        def post(self, *args, **kwargs):
            return self._Resp()

    caplog.set_level("INFO")
    msg_obj = {"order_id": 1, "transaction_id": "tx1", "channel": "SMS", "payload": {}}

    ok, err = await sms._process_with_retry(msg_obj, FakeClientSession(), publish_channel=None)

    assert ok is True
    assert err is None
    assert "unexpected error during posting" not in caplog.text


@pytest.mark.asyncio
async def test_handler_marks_failed_on_processing_failure(monkeypatch):
    called = {}

    async def fake_process_with_retry(msg_obj, session, publish_channel):
        return False, "boom"

    async def fake_mark_failed(pool, order_id, channel, error_message):
        called["failed"] = (order_id, channel, error_message)

    monkeypatch.setattr(sms, "_process_with_retry", fake_process_with_retry)
    monkeypatch.setattr(sms, "_mark_failed", fake_mark_failed)

    handler = sms.make_handler(pool="pool", publish_channel=None)
    msg_obj = {"order_id": 9, "transaction_id": "tx", "channel": "SMS", "payload": {}}
    await handler(msg_obj)

    assert called.get("failed") == (9, "SMS", "boom")


@pytest.mark.asyncio
async def test_mark_failed_updates_db_and_logs(caplog):
    class FakeCursor:
        def __init__(self):
            self.executed = []

        async def execute(self, query, params=None):
            self.executed.append((query, params))

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def __init__(self, cursor):
            self._cursor = cursor
            self.committed = False

        def cursor(self):
            return self._cursor

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def commit(self):
            self.committed = True

    class FakeAcquire:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakePool:
        def __init__(self, conn):
            self._conn = conn

        def acquire(self):
            return FakeAcquire(self._conn)

    fake_cursor = FakeCursor()
    fake_conn = FakeConn(fake_cursor)
    fake_pool = FakePool(fake_conn)

    caplog.set_level("INFO")
    await sms._mark_failed(fake_pool, 321, "SMS", "some error happened")

    update_executed = [item for item in fake_cursor.executed if item[0].strip().upper().startswith("UPDATE DISTRIBUTION_STATUS")]
    assert update_executed, "UPDATE not executed for failed"
    query, params = update_executed[-1]
    # ensure we are also updating the millisecond lag column on failure
    assert "lag_db_channel_milliseconds" in query
    assert params[0] == "some error happened"
    assert params[1] == 321
    assert params[2] == "SMS"
    assert "marked distribution_status failed" in caplog.text
