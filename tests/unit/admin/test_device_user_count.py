from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from tests.helpers.factories import make_device


@pytest.mark.network
def test_user_count_returns_count_for_online_device(logged_in_client, db_session) -> None:
    device = make_device(name="Gate A", host="10.0.0.1", status="online", is_enabled=True)
    db_session.add(device)
    db_session.commit()

    mock_users = [{"UserId": str(i)} for i in range(42)]
    with patch("app.admin.devices.DahuaClient") as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.get_all_users = AsyncMock(return_value=mock_users)
        instance.close = AsyncMock()

        resp = logged_in_client.get(f"/admin/devices/{device.id}/user-count")

    assert resp.status_code == 200
    assert "42" in resp.text


def test_user_count_returns_dash_for_offline_device(logged_in_client, db_session) -> None:
    device = make_device(name="Gate B", host="10.0.0.2", status="offline", is_enabled=True)
    db_session.add(device)
    db_session.commit()

    resp = logged_in_client.get(f"/admin/devices/{device.id}/user-count")

    assert resp.status_code == 200
    assert "—" in resp.text


def test_user_count_returns_dash_when_device_not_found(logged_in_client, db_session) -> None:
    resp = logged_in_client.get("/admin/devices/9999/user-count")

    assert resp.status_code == 200
    assert "—" in resp.text


@pytest.mark.network
def test_user_count_returns_dash_when_client_raises(logged_in_client, db_session) -> None:
    device = make_device(name="Gate C", host="10.0.0.3", status="online", is_enabled=True)
    db_session.add(device)
    db_session.commit()

    with patch("app.admin.devices.DahuaClient") as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.get_all_users = AsyncMock(side_effect=Exception("timeout"))
        instance.close = AsyncMock()

        resp = logged_in_client.get(f"/admin/devices/{device.id}/user-count")

    assert resp.status_code == 200
    assert "—" in resp.text
