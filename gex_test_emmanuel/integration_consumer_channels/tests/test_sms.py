from datetime import UTC, datetime, timedelta

import pytest

from services import sms


@pytest.mark.asyncio
async def test_handler_calls_mark_delivered(monkeypatch):
    called = {}

    async def fake_process_with_retry(msg_obj, session, publish_channel):
        called["processed"] = True
        return True

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

        async def cursor(self):
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
