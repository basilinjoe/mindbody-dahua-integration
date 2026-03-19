from __future__ import annotations

import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_write_batch_adds_rows(mock_db):
    from app.services.queue import write_batch

    items = [
        {"mindbody_client_id": "101", "device_id": 1, "action": "enroll",
         "status": "pending", "dahua_user_id": None, "member_snapshot": None},
    ]
    mock_db.add_all = MagicMock()
    mock_db.commit = AsyncMock()

    count = await write_batch(mock_db, "run-1", items)
    assert count == 1
    mock_db.add_all.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_write_batch_empty_items(mock_db):
    from app.services.queue import write_batch

    mock_db.add_all = MagicMock()
    mock_db.commit = AsyncMock()

    count = await write_batch(mock_db, "run-1", [])
    assert count == 0
    mock_db.add_all.assert_not_called()


@pytest.mark.asyncio
async def test_load_pending_returns_pending_rows(mock_db):
    from app.services.queue import load_pending
    from app.models.dahua_sync_queue import DahuaSyncQueue

    fake_item = MagicMock(spec=DahuaSyncQueue)
    fake_item.status = "pending"
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_item]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_pending(mock_db, "run-1")
    assert result == [fake_item]


@pytest.mark.asyncio
async def test_mark_item_updates_status(mock_db):
    from app.services.queue import mark_item
    from app.models.dahua_sync_queue import DahuaSyncQueue

    fake_item = MagicMock(spec=DahuaSyncQueue)
    fake_item.status = "pending"
    fake_item.error_message = None
    fake_item.executed_at = None

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_item
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await mark_item(mock_db, 42, "success")
    assert fake_item.status == "success"
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_mark_item_noop_when_not_found(mock_db):
    from app.services.queue import mark_item

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await mark_item(mock_db, 999, "success")
    mock_db.commit.assert_not_called()
