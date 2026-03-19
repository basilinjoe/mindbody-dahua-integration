from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# ── Async engine (FastAPI routes + Prefect tasks) ────────────────────────────
async_engine = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None


def init_async_db(database_url: str) -> async_sessionmaker[AsyncSession]:
    """
    Initialise the async engine.

    database_url should use an async driver, e.g.:
        postgresql+asyncpg://user:pass@host/db

    If a sync URL is given (starts with postgresql+psycopg2 or postgresql://),
    it is auto-converted to the async equivalent so callers can share
    the same DATABASE_URL env var.
    """
    global async_engine, AsyncSessionLocal
    async_url = _to_async_url(database_url)
    async_engine = create_async_engine(async_url, echo=False)
    AsyncSessionLocal = async_sessionmaker(
        async_engine, expire_on_commit=False, class_=AsyncSession
    )
    return AsyncSessionLocal


def _to_async_url(url: str) -> str:
    """Convert a sync PostgreSQL URL to its async equivalent."""
    if url.startswith("postgresql+psycopg2://"):
        return url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url  # already async or unknown


def _get_async_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return AsyncSessionLocal, auto-initialising from DATABASE_URL env var if needed."""
    global AsyncSessionLocal
    if AsyncSessionLocal is None:
        import os

        database_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost/sync")
        init_async_db(database_url)
    return AsyncSessionLocal  # type: ignore[return-value]


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Async dependency for FastAPI routes."""
    async with _get_async_session_factory()() as session:
        yield session
