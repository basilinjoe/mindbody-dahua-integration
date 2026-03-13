from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

# ── Sync engine (FastAPI routes + tests) ───────────────────────────────────────
engine = None
SessionLocal: sessionmaker[Session] | None = None

# ── Async engine (Prefect tasks) ───────────────────────────────────────────────
async_engine = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None


class Base(DeclarativeBase):
    pass


def init_db(database_url: str) -> sessionmaker[Session]:
    """Initialise the synchronous engine. Called by FastAPI lifespan."""
    global engine, SessionLocal
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    engine = create_engine(database_url, connect_args=connect_args)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal


def init_async_db(database_url: str) -> async_sessionmaker[AsyncSession]:
    """
    Initialise the async engine for use by Prefect tasks.

    database_url should use an async driver, e.g.:
        postgresql+asyncpg://user:pass@host/db

    If a sync URL is given (starts with postgresql+psycopg2 or sqlite),
    it is auto-converted to the async equivalent so callers can share
    the same DATABASE_URL env var.
    """
    global async_engine, AsyncSessionLocal
    async_url = _to_async_url(database_url)
    connect_args: dict = {}
    if async_url.startswith("sqlite+aiosqlite"):
        connect_args["check_same_thread"] = False
    async_engine = create_async_engine(async_url, echo=False, connect_args=connect_args)
    AsyncSessionLocal = async_sessionmaker(
        async_engine, expire_on_commit=False, class_=AsyncSession
    )
    return AsyncSessionLocal


def _to_async_url(url: str) -> str:
    """Convert a sync DB URL to its async equivalent."""
    if url.startswith("postgresql+psycopg2://"):
        return url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
    if url.startswith("sqlite+pysqlite:///"):
        return url.replace("sqlite+pysqlite:///", "sqlite+aiosqlite:///", 1)
    return url  # already async or unknown


def get_db():
    """Sync dependency for FastAPI routes."""
    if SessionLocal is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Async dependency / context manager for Prefect tasks."""
    if AsyncSessionLocal is None:
        raise RuntimeError("Async database not initialised. Call init_async_db() first.")
    async with AsyncSessionLocal() as session:
        yield session
