from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def fake_device():
    from app.models.device import DahuaDevice
    d = MagicMock(spec=DahuaDevice)
    d.id = 1
    d.name = "Gate A"
    d.gate_type = "all"
    d.is_enabled = True
    d.status = "online"
    return d


@pytest.mark.asyncio
async def test_list_all_returns_enabled_devices(mock_db, fake_device):
    from app.services.devices import list_all

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_device]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert result == [fake_device]
    mock_db.execute.assert_called_once()


@pytest.mark.asyncio
async def test_list_by_gate_type_returns_ids(mock_db):
    from app.services.devices import list_by_gate_type

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [1, 2]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_by_gate_type(mock_db, "male")
    assert result == [1, 2]


@pytest.mark.asyncio
async def test_get_by_id_returns_device(mock_db, fake_device):
    from app.services.devices import get_by_id

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_id(mock_db, 1)
    assert result is fake_device


@pytest.mark.asyncio
async def test_get_by_id_returns_none_when_not_found(mock_db):
    from app.services.devices import get_by_id

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_id(mock_db, 999)
    assert result is None


@pytest.mark.asyncio
async def test_update_status_commits(mock_db, fake_device):
    from app.services.devices import update_status

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "offline")
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_status_noop_when_not_found(mock_db):
    from app.services.devices import update_status

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 999, "offline")
    mock_db.commit.assert_not_called()
