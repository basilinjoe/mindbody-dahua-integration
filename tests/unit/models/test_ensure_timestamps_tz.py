from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import app.models.database as db_mod


@pytest.mark.asyncio
async def test_ensure_timestamps_tz_skips_when_already_migrated():
    """No-op if _timestamps_migrated is already True."""
    original = db_mod._timestamps_migrated
    try:
        db_mod._timestamps_migrated = True
        # Should return immediately without touching engine
        await db_mod.ensure_timestamps_tz()
    finally:
        db_mod._timestamps_migrated = original


@pytest.mark.asyncio
async def test_ensure_timestamps_tz_skips_when_engine_is_none():
    """No-op if async_engine is None."""
    original_flag = db_mod._timestamps_migrated
    original_engine = db_mod.async_engine
    try:
        db_mod._timestamps_migrated = False
        db_mod.async_engine = None
        await db_mod.ensure_timestamps_tz()
        assert not db_mod._timestamps_migrated  # stays False — didn't run
    finally:
        db_mod._timestamps_migrated = original_flag
        db_mod.async_engine = original_engine


@pytest.mark.asyncio
async def test_ensure_timestamps_tz_skips_non_asyncpg_engine():
    """Sets flag but skips ALTER for non-asyncpg (e.g. SQLite) engines."""
    original_flag = db_mod._timestamps_migrated
    original_engine = db_mod.async_engine
    try:
        db_mod._timestamps_migrated = False
        fake_engine = MagicMock()
        fake_engine.url = "sqlite+aiosqlite:///:memory:"
        db_mod.async_engine = fake_engine
        await db_mod.ensure_timestamps_tz()
        assert db_mod._timestamps_migrated is True
    finally:
        db_mod._timestamps_migrated = original_flag
        db_mod.async_engine = original_engine


@pytest.mark.asyncio
async def test_ensure_timestamps_tz_runs_alter_for_asyncpg():
    """Executes ALTER statements for asyncpg engines."""
    original_flag = db_mod._timestamps_migrated
    original_engine = db_mod.async_engine
    try:
        db_mod._timestamps_migrated = False

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        fake_engine = MagicMock()
        fake_engine.url = "postgresql+asyncpg://user:pass@host/db"
        fake_engine.begin.return_value = mock_cm

        db_mod.async_engine = fake_engine

        await db_mod.ensure_timestamps_tz()

        assert db_mod._timestamps_migrated is True
        # Should have called execute for each timestamp column
        assert mock_conn.execute.call_count == len(db_mod._TIMESTAMP_COLUMNS)
    finally:
        db_mod._timestamps_migrated = original_flag
        db_mod.async_engine = original_engine


@pytest.mark.asyncio
async def test_ensure_timestamps_tz_handles_alter_errors():
    """ALTER errors are silently caught (column may already be TIMESTAMPTZ)."""
    original_flag = db_mod._timestamps_migrated
    original_engine = db_mod.async_engine
    try:
        db_mod._timestamps_migrated = False

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(side_effect=Exception("already timestamptz"))

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        fake_engine = MagicMock()
        fake_engine.url = "postgresql+asyncpg://user:pass@host/db"
        fake_engine.begin.return_value = mock_cm

        db_mod.async_engine = fake_engine

        # Should not raise
        await db_mod.ensure_timestamps_tz()
        assert db_mod._timestamps_migrated is True
    finally:
        db_mod._timestamps_migrated = original_flag
        db_mod.async_engine = original_engine


def test_get_async_session_factory_auto_init():
    """_get_async_session_factory auto-initialises from DATABASE_URL when AsyncSessionLocal is None."""
    original = db_mod.AsyncSessionLocal
    original_engine = db_mod.async_engine
    try:
        db_mod.AsyncSessionLocal = None
        db_mod.async_engine = None

        with patch.dict("os.environ", {"DATABASE_URL": "sqlite+aiosqlite:///:memory:"}):
            factory = db_mod._get_async_session_factory()

        assert factory is not None
        assert db_mod.AsyncSessionLocal is not None
    finally:
        db_mod.AsyncSessionLocal = original
        db_mod.async_engine = original_engine
