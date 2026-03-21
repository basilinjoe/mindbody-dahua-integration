from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_get_async_db_exported_from_deps():
    """get_async_db must be importable from app.api.deps."""
    from app.api.deps import get_async_db

    assert callable(get_async_db)


@pytest.mark.asyncio
async def test_get_async_db_yields_session():
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.api.deps import get_async_db

    mock_session = AsyncMock(spec=AsyncSession)
    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_ctx)

    with patch("app.models.database.AsyncSessionLocal", mock_factory):
        gen = get_async_db()
        session = await gen.__anext__()
        assert session is mock_session
