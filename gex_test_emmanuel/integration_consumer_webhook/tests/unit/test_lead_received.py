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
    _parse_iso_datetime,
    _process_once,
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
