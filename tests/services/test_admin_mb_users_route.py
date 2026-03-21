from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.members import get_last_fetched_at, upsert_batch


@pytest.mark.asyncio
async def test_get_last_fetched_at_returns_none_on_empty_table():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_last_fetched_at(mock_db)
    assert result is None


@pytest.mark.asyncio
async def test_upsert_batch_handles_no_members():
    mock_db = AsyncMock(spec=AsyncSession)
    count = await upsert_batch(mock_db, [])
    assert count == 0
