from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def fake_job():
    from app.models.export_job import ExportJob, ExportStatus

    j = MagicMock(spec=ExportJob)
    j.id = 1
    j.status = ExportStatus.pending
    return j


@pytest.mark.asyncio
async def test_create_returns_job(mock_db):
    from app.models.export_job import ExportStatus
    from app.services.export_jobs import create

    mock_db.add = MagicMock()
    mock_db.commit = AsyncMock()
    mock_db.refresh = AsyncMock()

    job = await create(mock_db)
    mock_db.add.assert_called_once()
    mock_db.commit.assert_called_once()
    mock_db.refresh.assert_called_once()
    assert job.status == ExportStatus.pending


@pytest.mark.asyncio
async def test_get_returns_job(mock_db, fake_job):
    from app.services.export_jobs import get

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_job
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get(mock_db, 1)
    assert result is fake_job


@pytest.mark.asyncio
async def test_get_returns_none_when_not_found(mock_db):
    from app.services.export_jobs import get

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get(mock_db, 999)
    assert result is None


@pytest.mark.asyncio
async def test_list_all_returns_jobs(mock_db, fake_job):
    from app.services.export_jobs import list_all

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_job]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert result == [fake_job]


@pytest.mark.asyncio
async def test_update_commits_changes(mock_db, fake_job):
    from app.models.export_job import ExportStatus
    from app.services.export_jobs import update

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_job
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update(mock_db, 1, status=ExportStatus.running)
    assert fake_job.status == ExportStatus.running
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_noop_when_not_found(mock_db):
    from app.models.export_job import ExportStatus
    from app.services.export_jobs import update

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update(mock_db, 999, status=ExportStatus.running)
    mock_db.commit.assert_not_called()
