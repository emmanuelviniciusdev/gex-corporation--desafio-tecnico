import logging
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings

logger = logging.getLogger(__name__)


def _make_async_url(url: str) -> str:
    # Convert sqlite URLs to use aiosqlite if necessary
    if url.startswith("sqlite+"):
        return url
    if url.startswith("sqlite:"):
        return url.replace("sqlite:", "sqlite+aiosqlite:", 1)
    # Leave other URLs as-is (expecting async-capable driver in production)
    # Common MySQL sync driver 'mysql+pymysql' should be 'mysql+aiomysql' for async
    if "mysql+pymysql" in url:
        async_url = url.replace("mysql+pymysql", "mysql+aiomysql")
        logger.warning("Converted MySQL sync driver to async driver: %s -> %s", url, async_url)
        return async_url
    return url


ASYNC_DATABASE_URL = _make_async_url(settings.database_url)

async_engine: AsyncEngine = create_async_engine(ASYNC_DATABASE_URL, future=True, pool_pre_ping=True)
async_session: async_sessionmaker[AsyncSession] = async_sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession]:
    async with async_session() as session:
        yield session
