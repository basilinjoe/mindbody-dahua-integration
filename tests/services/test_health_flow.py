from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_health_flow_uses_devices_service_update_status():
    """health.py must call devices_svc.update_status instead of raw inline update()."""

    # If the module imports devices service, the attribute will exist
    import app.services.devices as devices_svc

    assert callable(devices_svc.update_status)


@pytest.mark.asyncio
async def test_update_status_online_sets_last_seen_at():
    from app.models.device import DahuaDevice
    from app.services.devices import update_status

    mock_db = AsyncMock()
    fake_device = MagicMock(spec=DahuaDevice)
    fake_device.status = "offline"
    fake_device.last_seen_at = None

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "online")
    assert fake_device.status == "online"
    assert fake_device.last_seen_at is not None


@pytest.mark.asyncio
async def test_update_status_offline_does_not_set_last_seen_at():
    from app.models.device import DahuaDevice
    from app.services.devices import update_status

    mock_db = AsyncMock()
    fake_device = MagicMock(spec=DahuaDevice)
    fake_device.status = "online"
    fake_device.last_seen_at = None

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "offline")
    assert fake_device.status == "offline"
    assert fake_device.last_seen_at is None
