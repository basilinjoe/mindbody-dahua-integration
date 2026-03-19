from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.devices import list_all, get_by_id, update_status


@pytest.mark.asyncio
async def test_list_all_integrates_with_devices_service():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_get_by_id_returns_none_for_missing():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_id(mock_db, 9999)
    assert result is None


@pytest.mark.asyncio
async def test_update_status_sets_offline():
    from app.models.device import DahuaDevice
    mock_db = AsyncMock(spec=AsyncSession)
    fake_device = MagicMock(spec=DahuaDevice)
    fake_device.status = "online"
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "offline")
    assert fake_device.status == "offline"
