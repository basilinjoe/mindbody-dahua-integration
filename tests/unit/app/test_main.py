from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.main import _seed_admin, _seed_devices, create_app
from app.models.admin_user import AdminUser
from app.models.device import DahuaDevice


def test_create_app_registers_stateful_routes_and_static() -> None:
    app = create_app()

    route_paths = {route.path for route in app.routes}
    assert "/health" in route_paths
    assert "/admin/login" in route_paths
    assert any(getattr(route, "path", None) == "/static" for route in app.routes)


@pytest.mark.asyncio
async def test_seed_admin_creates_single_admin_user() -> None:
    from app.config import Settings

    settings = Settings(
        mindbody_api_key="k",
        mindbody_site_id="s",
        mindbody_api_base_url="https://x",
        mindbody_username="u",
        mindbody_password="p",
        mindbody_webhook_signature_key="wk",
        dahua_devices="",
        dahua_default_host="",
        dahua_default_password="",
        database_url="postgresql://localhost/test",
        admin_username="admin",
        admin_password="changeme",
        secret_key="x",
    )

    mock_db = AsyncMock()
    mock_db.add = MagicMock()
    # First call: no admin exists → seed
    mock_result_empty = MagicMock()
    mock_result_empty.scalar_one_or_none.return_value = None
    # Second call: admin exists → skip
    mock_result_exists = MagicMock()
    mock_result_exists.scalar_one_or_none.return_value = MagicMock(spec=AdminUser)

    mock_db.execute = AsyncMock(side_effect=[mock_result_empty, mock_result_exists])
    mock_db.commit = AsyncMock()

    await _seed_admin(mock_db, settings)
    mock_db.add.assert_called_once()
    mock_db.commit.assert_called_once()

    # Second call should not add another user
    await _seed_admin(mock_db, settings)
    # add still called only once total
    mock_db.add.assert_called_once()


@pytest.mark.asyncio
async def test_seed_devices_supports_json_array() -> None:
    from app.config import Settings

    settings = Settings(
        mindbody_api_key="k",
        mindbody_site_id="s",
        mindbody_api_base_url="https://x",
        mindbody_username="u",
        mindbody_password="p",
        mindbody_webhook_signature_key="wk",
        dahua_default_host="",
        dahua_default_password="",
        database_url="postgresql://localhost/test",
        admin_username="admin",
        admin_password="changeme",
        secret_key="x",
        dahua_devices=json.dumps(
            [
                {"name": "Main Gate", "host": "10.0.0.10", "password": "a"},
                {"name": "Side Gate", "host": "10.0.0.11", "password": "b", "door_ids": "0,1"},
            ]
        ),
    )

    mock_db = AsyncMock()
    mock_db.add = MagicMock()
    # Both devices are new (not in DB)
    not_found = MagicMock()
    not_found.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=not_found)
    mock_db.commit = AsyncMock()

    await _seed_devices(mock_db, settings)

    assert mock_db.add.call_count == 2
    added_devices = [call.args[0] for call in mock_db.add.call_args_list]
    names = [d.name for d in added_devices]
    assert "Main Gate" in names
    assert "Side Gate" in names


@pytest.mark.asyncio
async def test_seed_devices_falls_back_to_default_host() -> None:
    from app.config import Settings

    settings = Settings(
        mindbody_api_key="k",
        mindbody_site_id="s",
        mindbody_api_base_url="https://x",
        mindbody_username="u",
        mindbody_password="p",
        mindbody_webhook_signature_key="wk",
        dahua_devices="",
        dahua_default_host="10.0.0.20",
        dahua_default_password="pw",
        database_url="postgresql://localhost/test",
        admin_username="admin",
        admin_password="changeme",
        secret_key="x",
    )

    mock_db = AsyncMock()
    mock_db.add = MagicMock()
    not_found = MagicMock()
    not_found.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=not_found)
    mock_db.commit = AsyncMock()

    await _seed_devices(mock_db, settings)

    assert mock_db.add.call_count == 1
    added = mock_db.add.call_args.args[0]
    assert isinstance(added, DahuaDevice)
    assert added.host == "10.0.0.20"
    assert added.name == "Default Device"
