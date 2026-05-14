"""Unit tests for services/lead_received.py.

All DB and messaging interactions are mocked so no real infrastructure is needed.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

# Ensure the package directory is importable (conftest.py already handles sys.path)
from services.lead_received import (
    ProcessingError,
    _call_sp_insert_lead,
    _insert_dead_letter,
    _parse_iso_datetime,
    _process_once,
    _process_with_retry,
    make_handler,
)

# ---------------------------------------------------------------------------
# _parse_iso_datetime
# ---------------------------------------------------------------------------


def test_parse_iso_datetime_utc_z():
    dt = _parse_iso_datetime("2024-01-15T10:30:00Z")
    assert dt.tzinfo is None
    assert dt == datetime(2024, 1, 15, 10, 30, 0)


def test_parse_iso_datetime_offset():
    dt = _parse_iso_datetime("2024-01-15T12:30:00+02:00")
    assert dt.tzinfo is None
    assert dt == datetime(2024, 1, 15, 10, 30, 0)


def test_parse_iso_datetime_naive():
    dt = _parse_iso_datetime("2024-01-15T10:30:00")
    assert dt.tzinfo is None
    assert dt == datetime(2024, 1, 15, 10, 30, 0)


def test_parse_iso_datetime_none_raises():
    with pytest.raises((ValueError, AttributeError)):
        _parse_iso_datetime(None)


# ---------------------------------------------------------------------------
# _call_sp_insert_lead
# ---------------------------------------------------------------------------


def _make_mock_cursor(sp_out_row):
    """Return an AsyncMock cursor whose fetchone returns sp_out_row."""
    cur = AsyncMock()
    cur.callproc = AsyncMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value=sp_out_row)
    return cur


def test_call_sp_insert_lead_returns_ids():
    cur = _make_mock_cursor((10, 20, 30))

    async def _run():
        return await _call_sp_insert_lead(
            cur,
            "test@example.com", "Test@example.com",
            "John", "Doe",
            "+1555000", "+1555000", 1, "US",
            None, "webhook", "TX-001",
            datetime(2024, 1, 15, 10, 0, 0),
            "PROD-1", "Product One", "health",
            1, 99.99, "credit_card", "approved",
            "corr-abc", "lead.received",
            datetime(2024, 1, 15, 10, 0, 0),
            datetime(2024, 1, 15, 10, 0, 5),
            5,
        )

    lead_id, order_id, event_id = asyncio.run(_run())
    assert lead_id == 10
    assert order_id == 20
    assert event_id == 30

    # Ensure callproc was called with the stored procedure name
    cur.callproc.assert_awaited_once()
    args = cur.callproc.call_args[0]
    assert args[0] == "sp_insert_lead"

    # Ensure OUT parameter variables were SELECTed
    cur.execute.assert_awaited_once()
    select_sql = cur.execute.call_args[0][0]
    assert "@_sp_insert_lead_25" in select_sql
    assert "@_sp_insert_lead_26" in select_sql
    assert "@_sp_insert_lead_27" in select_sql


def test_call_sp_insert_lead_allows_null_event_id():
    cur = _make_mock_cursor((11, 22, None))

    async def _run():
        return await _call_sp_insert_lead(
            cur,
            "test2@example.com",
            "Test2@example.com",
            "Alice",
            "Doe",
            None,
            None,
            0,
            "US",
            None,
            "webhook",
            "TX-002",
            datetime(2024, 1, 15, 11, 0, 0),
            "PROD-2",
            "Product Two",
            None,
            2,
            49.99,
            "paypal",
            "approved",
            "corr-def",
            "lead.received",
            datetime(2024, 1, 15, 11, 0, 0),
            datetime(2024, 1, 15, 11, 0, 5),
            5,
        )

    lead_id, order_id, event_id = asyncio.run(_run())
    assert lead_id == 11
    assert order_id == 22
    assert event_id is None


def test_call_sp_insert_lead_missing_required_ids_raises():
    # Simulate an unexpected NULL from the stored procedure for required IDs
    cur = _make_mock_cursor((None, 33, 44))

    async def _run():
        return await _call_sp_insert_lead(
            cur,
            "test3@example.com",
            "Test3@example.com",
            "Bob",
            "Smith",
            None,
            None,
            0,
            "US",
            None,
            "webhook",
            "TX-003",
            datetime(2024, 1, 15, 12, 0, 0),
            "PROD-3",
            "Product Three",
            None,
            1,
            19.99,
            "credit_card",
            "approved",
            "corr-ghi",
            "lead.received",
            datetime(2024, 1, 15, 12, 0, 0),
            datetime(2024, 1, 15, 12, 0, 5),
            5,
        )

    with pytest.raises(ValueError):
        asyncio.run(_run())


# ---------------------------------------------------------------------------
# _process_once — happy path
# ---------------------------------------------------------------------------

_BASE_MSG = {
    "gateway": "webhook",
    "received_at": "2024-01-15T10:00:00Z",
    "correlation_id": "corr-xyz",
    "id_raw_payload": 7,
    "payload": {
        "transaction_id": "TX-999",
        "transaction_time": "2024-01-15T09:59:55Z",
        "event": "lead.received",
        "customer": {
            "email": "alice@example.com",
            "first_name": "Alice",
            "last_name": "Smith",
            "phone": "+15550001",
            "country": "US",
        },
        "product": {
            "id": "P-1",
            "name": "Widget",
            "niche": "tech",
        },
        "quantity": 2,
        "payment": {
            "amount_usd": 49.99,
            "method": "pix",
            "status": "approved",
        },
    },
}


def _make_pool_mock(sp_out_row=(1, 2, 3)):
    """Build a mock aiomysql pool whose cursor returns sp_out_row."""
    cur = AsyncMock()
    cur.callproc = AsyncMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value=sp_out_row)
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=False)

    conn = AsyncMock()
    conn.cursor = MagicMock(return_value=cur)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)

    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=conn)

    return pool, conn, cur


def _make_publish_channel_mock():
    exchange = AsyncMock()
    exchange.publish = AsyncMock()
    channel = AsyncMock()
    channel.default_exchange = exchange
    return channel


def test_process_once_happy_path():
    pool, conn, cur = _make_pool_mock(sp_out_row=(1, 2, 3))
    publish_channel = _make_publish_channel_mock()

    async def _run():
        await _process_once(_BASE_MSG, pool, publish_channel)

    asyncio.run(_run())

    # sp was called
    cur.callproc.assert_awaited_once()
    sp_name = cur.callproc.call_args[0][0]
    assert sp_name == "sp_insert_lead"

    # distribution messages were published (4 channels)
    assert publish_channel.default_exchange.publish.await_count == 4

    # ensure each published message payload includes correlation_id embedded
    corr = _BASE_MSG["correlation_id"]
    for call in publish_channel.default_exchange.publish.await_args_list:
        # first positional arg is the aio_pika.Message
        msg = call.args[0]
        body = getattr(msg, "body", b"")
        if isinstance(body, (bytes, bytearray)):
            obj = json.loads(body.decode("utf-8"))
        else:
            obj = json.loads(body)
        assert "payload" in obj
        assert isinstance(obj["payload"], dict)
        assert obj["payload"].get("correlation_id") == corr


def test_process_once_missing_transaction_id_returns_without_error():
    msg = {**_BASE_MSG, "payload": {**_BASE_MSG["payload"], "transaction_id": ""}}
    pool, _, _ = _make_pool_mock()
    publish_channel = _make_publish_channel_mock()

    async def _run():
        await _process_once(msg, pool, publish_channel)

    asyncio.run(_run())
    # No exception raised — invalid payload is silently dropped
    publish_channel.default_exchange.publish.assert_not_awaited()


def test_process_once_missing_received_at_returns_without_error():
    msg = {**_BASE_MSG}
    msg = dict(msg)
    del msg["received_at"]
    pool, _, _ = _make_pool_mock()
    publish_channel = _make_publish_channel_mock()

    async def _run():
        await _process_once(msg, pool, publish_channel)

    asyncio.run(_run())
    publish_channel.default_exchange.publish.assert_not_awaited()


def test_process_once_invalid_payload_type_returns_without_error():
    msg = {**_BASE_MSG, "payload": 12345}
    pool, _, _ = _make_pool_mock()
    publish_channel = _make_publish_channel_mock()

    async def _run():
        await _process_once(msg, pool, publish_channel)

    asyncio.run(_run())
    publish_channel.default_exchange.publish.assert_not_awaited()


def test_process_once_db_error_raises_processing_error():
    pool, conn, cur = _make_pool_mock()
    cur.callproc = AsyncMock(side_effect=Exception("DB down"))
    publish_channel = _make_publish_channel_mock()

    async def _run():
        await _process_once(_BASE_MSG, pool, publish_channel)

    with pytest.raises(ProcessingError, match="DB error"):
        asyncio.run(_run())


def test_process_once_payload_as_json_string():
    """payload field can be a JSON string instead of a dict."""
    msg = {**_BASE_MSG, "payload": json.dumps(_BASE_MSG["payload"])}
    pool, conn, cur = _make_pool_mock(sp_out_row=(1, 2, 3))
    publish_channel = _make_publish_channel_mock()

    async def _run():
        await _process_once(msg, pool, publish_channel)

    asyncio.run(_run())
    cur.callproc.assert_awaited_once()


# ---------------------------------------------------------------------------
# make_handler — smoke test
# ---------------------------------------------------------------------------


def test_make_handler_processes_bytes_message():
    pool, conn, cur = _make_pool_mock(sp_out_row=(1, 2, 3))
    publish_channel = _make_publish_channel_mock()

    handler = make_handler(pool, publish_channel)

    raw_bytes = json.dumps(_BASE_MSG).encode("utf-8")

    async def _run():
        await handler(raw_bytes)

    asyncio.run(_run())
    cur.callproc.assert_awaited_once()


def test_make_handler_processes_dict_message():
    pool, conn, cur = _make_pool_mock(sp_out_row=(10, 20, 30))
    publish_channel = _make_publish_channel_mock()

    handler = make_handler(pool, publish_channel)

    async def _run():
        await handler(_BASE_MSG)

    asyncio.run(_run())
    cur.callproc.assert_awaited_once()


# ---------------------------------------------------------------------------
# _insert_dead_letter
# ---------------------------------------------------------------------------


def test_insert_dead_letter_executes_insert():
    """_insert_dead_letter must INSERT a row via the pool cursor and commit."""
    dl_cur = AsyncMock()
    dl_cur.execute = AsyncMock()
    dl_cur.__aenter__ = AsyncMock(return_value=dl_cur)
    dl_cur.__aexit__ = AsyncMock(return_value=False)

    dl_conn = AsyncMock()
    dl_conn.cursor = MagicMock(return_value=dl_cur)
    dl_conn.commit = AsyncMock()
    dl_conn.__aenter__ = AsyncMock(return_value=dl_conn)
    dl_conn.__aexit__ = AsyncMock(return_value=False)

    dl_pool = AsyncMock()
    dl_pool.acquire = MagicMock(return_value=dl_conn)

    async def _run():
        await _insert_dead_letter(
            dl_pool,
            correlation_id="corr-dead-1",
            origin="lead.received",
            raw_payload_id=42,
            payload='{"test": true}',
            error_message="something went wrong",
        )

    asyncio.run(_run())

    dl_cur.execute.assert_awaited_once()
    sql, params = dl_cur.execute.call_args[0]
    assert "lead_dead_letter" in sql
    assert params[0] == "corr-dead-1"        # correlation_id
    assert params[1] == "lead.received"      # origin
    assert params[2] == 42                   # raw_payload_id
    assert params[3] == '{"test": true}'     # payload
    assert params[4] == "something went wrong"  # error_message

    dl_conn.commit.assert_awaited_once()


def test_insert_dead_letter_db_error_is_swallowed():
    """_insert_dead_letter must not propagate DB errors (log only)."""
    dl_cur = AsyncMock()
    dl_cur.execute = AsyncMock(side_effect=Exception("DB exploded"))
    dl_cur.__aenter__ = AsyncMock(return_value=dl_cur)
    dl_cur.__aexit__ = AsyncMock(return_value=False)

    dl_conn = AsyncMock()
    dl_conn.cursor = MagicMock(return_value=dl_cur)
    dl_conn.commit = AsyncMock()
    dl_conn.__aenter__ = AsyncMock(return_value=dl_conn)
    dl_conn.__aexit__ = AsyncMock(return_value=False)

    dl_pool = AsyncMock()
    dl_pool.acquire = MagicMock(return_value=dl_conn)

    async def _run():
        # must not raise even when DB fails
        await _insert_dead_letter(
            dl_pool,
            correlation_id=None,
            origin="lead.received",
            raw_payload_id=None,
            payload="{}",
            error_message="oops",
        )

    asyncio.run(_run())  # no exception expected


# ---------------------------------------------------------------------------
# _process_with_retry — dead letter persistence
# ---------------------------------------------------------------------------


def _make_pool_mock_with_dl_cursor(sp_out_row=(1, 2, 3)):
    """Pool mock that supports two cursor() calls: one for SP, one for dead-letter INSERT."""
    sp_cur = AsyncMock()
    sp_cur.callproc = AsyncMock()
    sp_cur.execute = AsyncMock()
    sp_cur.fetchone = AsyncMock(return_value=sp_out_row)
    sp_cur.__aenter__ = AsyncMock(return_value=sp_cur)
    sp_cur.__aexit__ = AsyncMock(return_value=False)

    dl_cur = AsyncMock()
    dl_cur.execute = AsyncMock()
    dl_cur.__aenter__ = AsyncMock(return_value=dl_cur)
    dl_cur.__aexit__ = AsyncMock(return_value=False)

    # Two separate connection contexts for SP conn and DL conn
    sp_conn = AsyncMock()
    sp_conn.cursor = MagicMock(return_value=sp_cur)
    sp_conn.commit = AsyncMock()
    sp_conn.__aenter__ = AsyncMock(return_value=sp_conn)
    sp_conn.__aexit__ = AsyncMock(return_value=False)

    dl_conn = AsyncMock()
    dl_conn.cursor = MagicMock(return_value=dl_cur)
    dl_conn.commit = AsyncMock()
    dl_conn.__aenter__ = AsyncMock(return_value=dl_conn)
    dl_conn.__aexit__ = AsyncMock(return_value=False)

    # pool.acquire() alternates: first call → sp_conn, subsequent → dl_conn
    call_count = [0]

    class _AcquireCtx:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *_):
            return False

    def _acquire():
        call_count[0] += 1
        return _AcquireCtx(sp_conn if call_count[0] == 1 else dl_conn)

    pool = MagicMock()
    pool.acquire = _acquire

    return pool, sp_conn, sp_cur, dl_conn, dl_cur


def test_process_with_retry_inserts_dead_letter_on_processing_error():
    """On final ProcessingError failure, a dead-letter row must be inserted."""
    # Build a dedicated dead-letter cursor/connection tracked separately
    dl_cur = AsyncMock()
    dl_cur.execute = AsyncMock()
    dl_cur.__aenter__ = AsyncMock(return_value=dl_cur)
    dl_cur.__aexit__ = AsyncMock(return_value=False)

    dl_conn = AsyncMock()
    dl_conn.cursor = MagicMock(return_value=dl_cur)
    dl_conn.commit = AsyncMock()
    dl_conn.__aenter__ = AsyncMock(return_value=dl_conn)
    dl_conn.__aexit__ = AsyncMock(return_value=False)

    # Failing SP cursor (every acquire for _process_once raises ProcessingError)
    fail_cur = AsyncMock()
    fail_cur.callproc = AsyncMock(side_effect=Exception("DB failure"))
    fail_cur.__aenter__ = AsyncMock(return_value=fail_cur)
    fail_cur.__aexit__ = AsyncMock(return_value=False)

    fail_conn = AsyncMock()
    fail_conn.cursor = MagicMock(return_value=fail_cur)
    fail_conn.commit = AsyncMock()
    fail_conn.__aenter__ = AsyncMock(return_value=fail_conn)
    fail_conn.__aexit__ = AsyncMock(return_value=False)

    # pool.acquire(): first 3 calls (SP retries) → fail_conn; 4th call (dead-letter) → dl_conn
    call_count = [0]

    class _AcquireCtx:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *_):
            return False

    def _acquire():
        call_count[0] += 1
        return _AcquireCtx(fail_conn if call_count[0] <= 3 else dl_conn)

    pool = MagicMock()
    pool.acquire = _acquire

    publish_channel = _make_publish_channel_mock()

    async def _run():
        import unittest.mock as mock
        with mock.patch("asyncio.sleep", new=AsyncMock()):
            return await _process_with_retry(_BASE_MSG, pool, publish_channel)

    result = asyncio.run(_run())
    assert result is False

    # dead-letter INSERT must have been called
    dl_cur.execute.assert_awaited()
    sql, params = dl_cur.execute.call_args[0]
    assert "lead_dead_letter" in sql
    # origin must be the queue name
    assert params[1] == "lead.received"
    # correlation_id from _BASE_MSG
    assert params[0] == "corr-xyz"
    # raw_payload_id from _BASE_MSG
    assert params[2] == 7
    # payload must be valid JSON containing the message
    payload_obj = json.loads(params[3])
    assert payload_obj.get("correlation_id") == "corr-xyz"


def test_process_with_retry_no_dead_letter_on_success():
    """On success, no dead-letter row should be inserted."""
    pool, conn, cur = _make_pool_mock(sp_out_row=(1, 2, 3))
    publish_channel = _make_publish_channel_mock()

    async def _run():
        return await _process_with_retry(_BASE_MSG, pool, publish_channel)

    result = asyncio.run(_run())
    assert result is True

    # Only one cursor usage: the SP call; dead-letter INSERT uses a separate acquire
    # We verify by checking that only SP-related SQL was executed (no lead_dead_letter)
    executed_sqls = [call[0][0] for call in cur.execute.call_args_list]
    assert not any("lead_dead_letter" in sql for sql in executed_sqls)
