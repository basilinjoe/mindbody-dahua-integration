from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_get_stats_returns_dict(mock_db):
    from app.services.dashboard import get_stats

    mock_db.execute = AsyncMock(
        side_effect=[
            MagicMock(scalar=MagicMock(return_value=100)),  # total_members
            MagicMock(scalar=MagicMock(return_value=80)),  # active_members
            MagicMock(scalar=MagicMock(return_value=60)),  # active_with_membership
            MagicMock(scalar=MagicMock(return_value=3)),  # total_devices
            MagicMock(scalar=MagicMock(return_value=2)),  # online_devices
            MagicMock(scalar=MagicMock(return_value=5)),  # pending_queue
            MagicMock(scalar=MagicMock(return_value=1)),  # failed_queue
            MagicMock(scalar=MagicMock(return_value=0)),  # failed_24h
        ]
    )

    stats = await get_stats(mock_db)
    assert stats["total_members"] == 100
    assert stats["active_members"] == 80
    assert stats["active_with_membership"] == 60
    assert stats["total_devices"] == 3
    assert stats["online_devices"] == 2
    assert stats["pending_queue"] == 5
    assert stats["failed_queue"] == 1
    assert stats["failed_24h"] == 0


@pytest.mark.asyncio
async def test_get_stats_defaults_none_to_zero(mock_db):
    from app.services.dashboard import get_stats

    mock_db.execute = AsyncMock(
        side_effect=[
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
        ]
    )

    stats = await get_stats(mock_db)
    assert all(v == 0 for v in stats.values())


@pytest.mark.asyncio
async def test_get_recent_queue_returns_tuples(mock_db):
    from app.services.dashboard import get_recent_queue

    fake_row = (MagicMock(), "Device A")
    mock_result = MagicMock()
    mock_result.all.return_value = [fake_row]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_recent_queue(mock_db)
    assert result == [fake_row]


@pytest.mark.asyncio
async def test_get_mb_breakdown_returns_expected_keys(mock_db):
    from app.services.dashboard import get_mb_breakdown

    # gender query
    gender_result = MagicMock()
    gender_result.__iter__ = MagicMock(return_value=iter([(None, 5), ("Male", 20), ("Female", 10)]))
    # active_sub_count query
    sub_result = MagicMock(scalar=MagicMock(return_value=15))

    mock_db.execute = AsyncMock(side_effect=[gender_result, sub_result])

    result = await get_mb_breakdown(mock_db)
    assert result["male_count"] == 20
    assert result["female_count"] == 10
    assert result["active_sub_count"] == 15
    assert result["no_sub_count"] == 20  # 35 total - 15 active
    assert "male_pct" in result
    assert "female_pct" in result
    assert "active_sub_pct" in result
    assert "no_sub_pct" in result


@pytest.mark.asyncio
async def test_get_device_rows_returns_dicts(mock_db):
    from app.services.dashboard import get_device_rows

    fake_device = MagicMock()
    fake_device.id = 1
    devices_result = MagicMock()
    devices_result.scalars.return_value.all.return_value = [fake_device]
    pending_result = MagicMock(scalar=MagicMock(return_value=3))
    failed_result = MagicMock(scalar=MagicMock(return_value=1))

    mock_db.execute = AsyncMock(side_effect=[devices_result, pending_result, failed_result])

    result = await get_device_rows(mock_db)
    assert len(result) == 1
    assert result[0]["device"] is fake_device
    assert result[0]["pending"] == 3
    assert result[0]["failed_24h"] == 1
