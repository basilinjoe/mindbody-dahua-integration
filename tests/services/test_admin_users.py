from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_get_by_username_returns_user(mock_db):
    from app.models.admin_user import AdminUser
    from app.services.admin_users import get_by_username

    fake_user = MagicMock(spec=AdminUser)
    fake_user.username = "admin"
    fake_user.is_active = True

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_user
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_username(mock_db, "admin")
    assert result is fake_user


@pytest.mark.asyncio
async def test_get_by_username_returns_none_when_not_found(mock_db):
    from app.services.admin_users import get_by_username

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_username(mock_db, "nobody")
    assert result is None


@pytest.mark.asyncio
async def test_get_by_username_filters_active(mock_db):
    """Query must include is_active in the WHERE clause."""
    from app.services.admin_users import get_by_username

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    await get_by_username(mock_db, "admin")
    call_args = mock_db.execute.call_args[0][0]
    compiled = str(call_args.compile(compile_kwargs={"literal_binds": True}))
    assert "is_active" in compiled
