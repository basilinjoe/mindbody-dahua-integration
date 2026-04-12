from __future__ import annotations

import logging
from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

_logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


# ── Async engine (FastAPI routes + Prefect tasks) ────────────────────────────
async_engine = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None
_timestamps_migrated = False

# Columns that need to be TIMESTAMPTZ (was TIMESTAMP WITHOUT TIME ZONE).
_TIMESTAMP_COLUMNS = [
    ("dahua_devices", "last_seen_at"),
    ("dahua_devices", "created_at"),
    ("dahua_sync_queue", "created_at"),
    ("dahua_sync_queue", "processed_at"),
    ("admin_users", "created_at"),
    ("export_jobs", "created_at"),
    ("export_jobs", "started_at"),
    ("export_jobs", "finished_at"),
    ("mindbody_clients", "last_fetched_at"),
    ("mindbody_memberships", "last_synced_at"),
]


async def ensure_timestamps_tz() -> None:
    """ALTER existing TIMESTAMP columns to TIMESTAMPTZ. Idempotent, safe to call multiple times."""
    global _timestamps_migrated
    if _timestamps_migrated or async_engine is None:
        return
    if "asyncpg" not in str(async_engine.url):
        _timestamps_migrated = True
        return
    async with async_engine.begin() as conn:
        for table, column in _TIMESTAMP_COLUMNS:
            try:
                await conn.execute(
                    text(
                        f"ALTER TABLE {table} "  # noqa: S608
                        f"ALTER COLUMN {column} TYPE TIMESTAMPTZ "
                        f"USING {column} AT TIME ZONE 'UTC'"
                    )
                )
            except Exception:
                pass  # Column may not exist yet or already be TIMESTAMPTZ

        # Add flow_type column to dahua_sync_queue (idempotent)
        try:
            await conn.execute(
                text(
                    "ALTER TABLE dahua_sync_queue "
                    "ADD COLUMN IF NOT EXISTS flow_type VARCHAR(16) NOT NULL DEFAULT 'full'"
                )
            )
        except Exception:
            pass  # Column already exists or table not created yet

    _timestamps_migrated = True
    _logger.info("Ensured all timestamp columns are timezone-aware")


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

        database_url = os.environ.get(
            "DATABASE_URL", "postgresql://postgres:postgres@localhost/sync"
        )
        init_async_db(database_url)
    return AsyncSessionLocal  # type: ignore[return-value]


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Async dependency for FastAPI routes."""
    async with _get_async_session_factory()() as session:
        yield session
