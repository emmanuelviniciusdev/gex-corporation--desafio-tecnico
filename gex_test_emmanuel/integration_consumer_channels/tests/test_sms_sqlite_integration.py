from datetime import UTC, datetime, timedelta

import pytest

try:
    import aiosqlite  # type: ignore
except Exception:  # pragma: no cover - environment dependent
    aiosqlite = None  # type: ignore
    pytestmark = pytest.mark.skip(reason="aiosqlite not installed")

from services import sms


class SqlitePoolAdapter:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def acquire(self):
        return AcquireCM(self.db_path)


class AcquireCM:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    async def __aenter__(self):
        self.conn = await aiosqlite.connect(self.db_path)
        return ConnAdapter(self.conn)

    async def __aexit__(self, exc_type, exc, tb):
        await self.conn.close()


class ConnAdapter:
    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    def cursor(self):
        return CursorCM(self._conn)

    async def commit(self):
        await self._conn.commit()


class CursorCM:
    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn
        self._cur = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._cur:
            await self._cur.close()

    async def execute(self, query: str, params=None):
        params = () if params is None else params
        has_now = "NOW(6)" in query
        adapted_query = query.replace("NOW(6)", "?").replace("%s", "?")
        if has_now:
            now_val = datetime.now(UTC).isoformat()
            adapted_params = (now_val,) + tuple(params)
        else:
            adapted_params = tuple(params)
        self._cur = await self._conn.execute(adapted_query, adapted_params)

    async def fetchone(self):
        row = await self._cur.fetchone()
        if row is None:
            return None
        val = row[0]
        if isinstance(val, str):
            try:
                val_dt = datetime.fromisoformat(val)
            except Exception:
                try:
                    val_dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
                except Exception:
                    try:
                        val_dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        val_dt = val
            return (val_dt,)
        return row


@pytest.mark.asyncio
async def test_mark_delivered_with_sqlite(tmp_path):
    db_path = str(tmp_path / "test.db")

    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """
            CREATE TABLE distribution_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                channel TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                delivered_at TEXT NULL,
                lag_db_channel_milliseconds INTEGER NULL,
                error_message TEXT NULL
            )
            """
        )
        created_at = datetime.now(UTC) - timedelta(seconds=10)
        await db.execute(
            "INSERT INTO distribution_status (order_id, channel, status, created_at) VALUES (?, ?, 'pending', ?)",
            (123, "SMS", created_at.isoformat()),
        )
        await db.commit()

    pool = SqlitePoolAdapter(db_path)

    # call production helper which expects an aiomysql-style pool
    await sms._mark_delivered(pool, 123, "SMS")

    # assert DB updated
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute(
            "SELECT status, delivered_at, lag_db_channel_milliseconds FROM distribution_status WHERE order_id=? AND channel=?",
            (123, "SMS"),
        )
        row = await cur.fetchone()
        assert row[0] == "delivered"
        assert row[1] is not None
        # parse delivered_at and assert lag approximately 10s
        delivered_at = row[1]
        if isinstance(delivered_at, str):
            delivered_dt = datetime.fromisoformat(delivered_at)
        else:
            delivered_dt = delivered_at
        assert (delivered_dt - created_at).total_seconds() >= 9
