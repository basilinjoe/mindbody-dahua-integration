from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    db = AsyncMock(spec=AsyncSession)
    return db


@pytest.mark.asyncio
async def test_upsert_batch_returns_count(mock_db):
    from app.services.members import upsert_batch

    members = [
        {
            "Id": "101",
            "UniqueId": "u101",
            "FirstName": "Alice",
            "LastName": "Smith",
            "Email": "alice@example.com",
            "MobilePhone": None,
            "HomePhone": None,
            "WorkPhone": None,
            "Status": "Active",
            "Active": True,
            "BirthDate": None,
            "Gender": "Female",
            "CreationDate": None,
            "LastModifiedDateTime": None,
        }
    ]

    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, members)
    assert count == 1
    mock_db.execute.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_batch_deduplicates(mock_db):
    from app.services.members import upsert_batch

    members = [
        {
            "Id": "101",
            "UniqueId": "u101",
            "FirstName": "Alice",
            "LastName": "Smith",
            "Email": None,
            "MobilePhone": None,
            "HomePhone": None,
            "WorkPhone": None,
            "Status": "Active",
            "Active": True,
            "BirthDate": None,
            "Gender": None,
            "CreationDate": None,
            "LastModifiedDateTime": None,
        },
        {
            "Id": "101",
            "UniqueId": "u101",
            "FirstName": "Alice",
            "LastName": "Smith",
            "Email": None,
            "MobilePhone": None,
            "HomePhone": None,
            "WorkPhone": None,
            "Status": "Active",
            "Active": True,
            "BirthDate": None,
            "Gender": None,
            "CreationDate": None,
            "LastModifiedDateTime": None,
        },
    ]

    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, members)
    assert count == 1


@pytest.mark.asyncio
async def test_upsert_batch_empty_list(mock_db):
    from app.services.members import upsert_batch

    count = await upsert_batch(mock_db, [])
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_active_returns_scalars(mock_db):
    from app.models.mindbody_client import MindBodyClient
    from app.services.members import load_active

    fake_client = MagicMock(spec=MindBodyClient)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_client]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_active(mock_db)
    assert result == [fake_client]
    mock_db.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_last_fetched_at_returns_none_when_empty(mock_db):
    from app.services.members import get_last_fetched_at

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_last_fetched_at(mock_db)
    assert result is None


@pytest.mark.asyncio
async def test_get_last_fetched_at_returns_datetime(mock_db):
    from app.services.members import get_last_fetched_at

    expected = datetime(2025, 1, 1, tzinfo=UTC)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = expected
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_last_fetched_at(mock_db)
    assert result == expected


@pytest.mark.asyncio
async def test_load_active_by_ids_returns_matching(mock_db):
    from app.models.mindbody_client import MindBodyClient
    from app.services.members import load_active_by_ids

    fake = MagicMock(spec=MindBodyClient)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_active_by_ids(mock_db, ["101"])
    assert result == [fake]
    mock_db.execute.assert_called_once()


@pytest.mark.asyncio
async def test_load_active_by_ids_empty_list(mock_db):
    from app.services.members import load_active_by_ids

    result = await load_active_by_ids(mock_db, [])
    assert result == []
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_update_last_fetched_at_updates_rows(mock_db):
    from app.services.members import update_last_fetched_at

    mock_result = MagicMock()
    mock_result.rowcount = 3
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    ts = datetime(2026, 4, 12, 10, 0, 0, tzinfo=UTC)
    count = await update_last_fetched_at(mock_db, ["101", "102", "103"], ts)
    assert count == 3
    mock_db.execute.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_last_fetched_at_empty_ids(mock_db):
    from app.services.members import update_last_fetched_at

    ts = datetime(2026, 4, 12, 10, 0, 0, tzinfo=UTC)
    count = await update_last_fetched_at(mock_db, [], ts)
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_batch_uses_fetched_at(mock_db):
    """When fetched_at is provided, it should be used instead of now()."""
    from app.services.members import upsert_batch

    custom_ts = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    members = [
        {
            "Id": "101",
            "UniqueId": "u101",
            "FirstName": "A",
            "LastName": "B",
            "Email": None,
            "MobilePhone": None,
            "HomePhone": None,
            "WorkPhone": None,
            "Status": "Active",
            "Active": True,
            "BirthDate": None,
            "Gender": None,
            "CreationDate": None,
            "LastModifiedDateTime": None,
        }
    ]

    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, members, fetched_at=custom_ts)
    assert count == 1
