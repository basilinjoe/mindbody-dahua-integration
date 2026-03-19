from __future__ import annotations

import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_upsert_batch_returns_count(mock_db):
    from app.services.memberships import upsert_batch

    memberships_by_client = {
        "101": [
            {
                "Id": "contract-1",
                "Name": "Monthly",
                "Status": "Active",
                "StartDate": "2024-01-01",
                "ExpirationDate": None,
            }
        ]
    }
    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, memberships_by_client)
    assert count == 1
    mock_db.execute.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_batch_skips_empty_contract_id(mock_db):
    from app.services.memberships import upsert_batch

    memberships_by_client = {
        "101": [{"Id": "", "Name": "Bad", "Status": "Active", "StartDate": None, "ExpirationDate": None}]
    }
    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, memberships_by_client)
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_batch_empty_dict(mock_db):
    from app.services.memberships import upsert_batch

    count = await upsert_batch(mock_db, {})
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_windows_returns_dict(mock_db):
    from app.services.memberships import load_windows

    fake_row = MagicMock()
    fake_row.mindbody_client_id = "101"
    fake_row.valid_start = "2024-01-01"
    fake_row.valid_end = None

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([fake_row]))
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_windows(mock_db, ["101"])
    assert "101" in result
    assert result["101"]["valid_start"] == "2024-01-01"
    assert result["101"]["valid_end"] is None


@pytest.mark.asyncio
async def test_load_windows_empty_client_ids(mock_db):
    from app.services.memberships import load_windows

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_windows(mock_db, [])
    assert result == {}
