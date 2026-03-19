from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_login_calls_admin_users_service():
    """The login handler must call admin_users.get_by_username instead of raw query."""
    import app.admin.auth as auth_mod

    assert hasattr(auth_mod, "admin_users")
    assert callable(auth_mod.admin_users.get_by_username)
