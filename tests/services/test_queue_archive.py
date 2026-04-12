from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.services.queue_archive import _serialize_queue_item, archive_previous_runs


def _make_queue_item(**overrides) -> MagicMock:
    defaults = {
        "id": 1,
        "run_id": "old-run",
        "flow_type": "full",
        "device_id": 1,
        "mindbody_client_id": "101",
        "action": "enroll",
        "status": "success",
        "member_snapshot": '{"Id": "101"}',
        "dahua_user_id": "101",
        "enrollment_id": None,
        "error_message": None,
        "created_at": datetime(2026, 3, 22, 10, 0, 0),
        "processed_at": datetime(2026, 3, 22, 10, 1, 0),
    }
    defaults.update(overrides)
    item = MagicMock(spec=DahuaSyncQueue)
    for k, v in defaults.items():
        setattr(item, k, v)
    return item


# ── _serialize_queue_item ─────────────────────────────────────────────────


def test_serialize_queue_item_full():
    item = _make_queue_item()
    result = _serialize_queue_item(item)

    assert result["id"] == 1
    assert result["run_id"] == "old-run"
    assert result["action"] == "enroll"
    assert result["created_at"] == "2026-03-22T10:00:00"
    assert result["processed_at"] == "2026-03-22T10:01:00"


def test_serialize_queue_item_null_datetimes():
    item = _make_queue_item(created_at=None, processed_at=None)
    result = _serialize_queue_item(item)

    assert result["created_at"] is None
    assert result["processed_at"] is None


# ── archive_previous_runs ────────────────────────────────────────────────


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_archive_no_previous_runs(mock_db, tmp_path):
    """Empty table → returns 0, no files written."""
    mock_distinct = MagicMock()
    mock_distinct.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_distinct)

    with patch("app.services.queue_archive.ARCHIVE_DIR", tmp_path):
        count = await archive_previous_runs(mock_db, "current-run")

    assert count == 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_archive_single_previous_run(mock_db, tmp_path):
    """One old run_id → writes JSON file, deletes rows, returns count."""
    item = _make_queue_item(id=1, run_id="old-run-1")

    mock_distinct = MagicMock()
    mock_distinct.scalars.return_value.all.return_value = ["old-run-1"]

    mock_rows = MagicMock()
    mock_rows.scalars.return_value.all.return_value = [item]

    mock_delete = MagicMock()

    mock_db.execute = AsyncMock(side_effect=[mock_distinct, mock_rows, mock_delete])
    mock_db.commit = AsyncMock()

    with patch("app.services.queue_archive.ARCHIVE_DIR", tmp_path):
        count = await archive_previous_runs(mock_db, "current-run")

    assert count == 1
    mock_db.commit.assert_called_once()

    files = list(tmp_path.iterdir())
    assert len(files) == 1
    assert files[0].name == "sync_queue_old-run-1.json"

    data = json.loads(files[0].read_text())
    assert data["run_id"] == "old-run-1"
    assert data["item_count"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["action"] == "enroll"


@pytest.mark.asyncio
async def test_archive_multiple_previous_runs(mock_db, tmp_path):
    """Two old run_ids → two separate JSON files."""
    item_a = _make_queue_item(id=1, run_id="run-a")
    item_b = _make_queue_item(id=2, run_id="run-b", action="deactivate")

    mock_distinct = MagicMock()
    mock_distinct.scalars.return_value.all.return_value = ["run-a", "run-b"]

    mock_rows_a = MagicMock()
    mock_rows_a.scalars.return_value.all.return_value = [item_a]
    mock_delete_a = MagicMock()

    mock_rows_b = MagicMock()
    mock_rows_b.scalars.return_value.all.return_value = [item_b]
    mock_delete_b = MagicMock()

    mock_db.execute = AsyncMock(
        side_effect=[mock_distinct, mock_rows_a, mock_delete_a, mock_rows_b, mock_delete_b]
    )
    mock_db.commit = AsyncMock()

    with patch("app.services.queue_archive.ARCHIVE_DIR", tmp_path):
        count = await archive_previous_runs(mock_db, "current-run")

    assert count == 2
    files = sorted(f.name for f in tmp_path.iterdir())
    assert files == ["sync_queue_run-a.json", "sync_queue_run-b.json"]


@pytest.mark.asyncio
async def test_archive_skips_current_run(mock_db, tmp_path):
    """Only old run_ids are archived; current run_id is excluded by the query."""
    mock_distinct = MagicMock()
    mock_distinct.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_distinct)

    with patch("app.services.queue_archive.ARCHIVE_DIR", tmp_path):
        count = await archive_previous_runs(mock_db, "current-run")

    assert count == 0


@pytest.mark.asyncio
async def test_archive_file_write_failure_skips_deletion(mock_db, tmp_path):
    """If file write fails, rows for that run_id are NOT deleted."""
    item = _make_queue_item(id=1, run_id="old-run")

    mock_distinct = MagicMock()
    mock_distinct.scalars.return_value.all.return_value = ["old-run"]

    mock_rows = MagicMock()
    mock_rows.scalars.return_value.all.return_value = [item]

    mock_db.execute = AsyncMock(side_effect=[mock_distinct, mock_rows])
    mock_db.commit = AsyncMock()

    # Make the archive dir read-only by pointing to a non-writable path
    with patch("app.services.queue_archive.ARCHIVE_DIR", tmp_path):
        with patch("pathlib.Path.write_text", side_effect=OSError("disk full")):
            count = await archive_previous_runs(mock_db, "current-run")

    assert count == 0
    # execute called twice: distinct query + rows query (no delete because write failed)
    assert mock_db.execute.call_count == 2


# ── Thin task wrapper ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_archive_previous_sync_queue_task_delegates():
    import app.sync.tasks as tasks_mod
    from app.services import queue_archive as queue_archive_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            queue_archive_svc,
            "archive_previous_runs",
            new_callable=AsyncMock,
            return_value=5,
        ) as mock_archive:
            result = await tasks_mod.archive_previous_sync_queue.fn("run-1")
            assert result == 5
            mock_archive.assert_called_once_with(mock_db, "run-1", flow_type=None)
