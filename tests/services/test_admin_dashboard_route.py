from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_dashboard_uses_service():
    """Dashboard route module must import dashboard service."""
    import app.admin.dashboard as dash_mod

    assert hasattr(dash_mod, "dashboard_svc")


@pytest.mark.asyncio
async def test_get_stats_callable():
    from app.services.dashboard import get_stats

    mock_db = AsyncMock(spec=AsyncSession)
    mock_db.execute = AsyncMock(
        side_effect=[
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
            MagicMock(scalar=MagicMock(return_value=0)),
        ]
    )
    result = await get_stats(mock_db)
    assert isinstance(result, dict)
    assert "total_members" in result
