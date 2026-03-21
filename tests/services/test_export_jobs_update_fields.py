from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_db():
    return AsyncMock()


@pytest.fixture
def fake_job():
    from app.models.export_job import ExportJob, ExportStatus

    j = MagicMock(spec=ExportJob)
    j.id = 1
    j.status = ExportStatus.pending
    j.error_msg = None
    j.zip_path = None
    j.file_name = None
    j.started_at = None
    j.finished_at = None
    return j


@pytest.mark.asyncio
async def test_update_all_fields(mock_db, fake_job):
    from app.models.export_job import ExportStatus
    from app.services.export_jobs import update

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_job
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    now = datetime.now(UTC)
    await update(
        mock_db,
        1,
        status=ExportStatus.complete,
        error_msg="some error",
        zip_path="/tmp/export.zip",
        file_name="export.zip",
        started_at=now,
        finished_at=now,
    )
    assert fake_job.status == ExportStatus.complete
    assert fake_job.error_msg == "some error"
    assert fake_job.zip_path == "/tmp/export.zip"
    assert fake_job.file_name == "export.zip"
    assert fake_job.started_at == now
    assert fake_job.finished_at == now
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_partial_fields(mock_db, fake_job):
    from app.models.export_job import ExportStatus
    from app.services.export_jobs import update

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_job
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update(mock_db, 1, error_msg="fail reason")
    assert fake_job.error_msg == "fail reason"
    # status should remain unchanged since we didn't pass it
    assert fake_job.status == ExportStatus.pending
