from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.queue import load_pending
from app.services.export_jobs import list_all, create, get, update


@pytest.mark.asyncio
async def test_load_pending_empty_run():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_pending(mock_db, "no-such-run")
    assert result == []


@pytest.mark.asyncio
async def test_export_jobs_list_all_empty():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert result == []


@pytest.mark.asyncio
async def test_export_jobs_update_noop():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    from app.models.export_job import ExportStatus
    await update(mock_db, 9999, status=ExportStatus.failed)
    mock_db.commit.assert_not_called()
