from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.admin.csrf import generate_csrf_token
from app.admin.sync_queue import _execute_push, _row_to_dict
from app.models.dahua_sync_queue import DahuaSyncQueue

# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


def test_row_to_dict_with_client_and_device():
    item = MagicMock(spec=DahuaSyncQueue)
    item.id = 1
    item.run_id = "run-1"
    item.mindbody_client_id = "100"
    item.device_id = 10
    item.action = "enroll"
    item.status = "pending"
    item.error_message = None
    item.created_at = datetime(2025, 1, 1, tzinfo=UTC)
    item.processed_at = None

    client = MagicMock()
    client.full_name = "Jane Doe"
    device = MagicMock()
    device.name = "Gate-A"

    result = _row_to_dict(item, client, device)
    assert result["client_name"] == "Jane Doe"
    assert result["device_name"] == "Gate-A"
    assert result["action"] == "enroll"


def test_row_to_dict_without_client_or_device():
    item = MagicMock(spec=DahuaSyncQueue)
    item.id = 2
    item.run_id = "run-2"
    item.mindbody_client_id = "200"
    item.device_id = 20
    item.action = "deactivate"
    item.status = "failed"
    item.error_message = "timeout"
    item.created_at = None
    item.processed_at = None

    result = _row_to_dict(item, None, None)
    assert result["client_name"] == "\u2014"
    assert result["device_name"] == "20"


# ---------------------------------------------------------------------------
# Unit tests for _execute_push
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_push_device_not_found():
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 999
    db = AsyncMock()

    with patch("app.admin.sync_queue.devices_svc") as mock_svc:
        mock_svc.get_by_id = AsyncMock(return_value=None)
        success, err = await _execute_push(item, db)

    assert success is False
    assert "not found" in err


@pytest.mark.asyncio
async def test_execute_push_deactivate_success():
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "deactivate"
    item.dahua_user_id = "123"
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()
    mock_client.update_user_status = AsyncMock(return_value=True)

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is True
    assert err is None
    mock_client.update_user_status.assert_called_once_with("123", card_status=4)
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_execute_push_reactivate_success():
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "reactivate"
    item.dahua_user_id = "456"
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()
    mock_client.update_user_status = AsyncMock(return_value=True)

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is True
    mock_client.update_user_status.assert_called_once_with("456", card_status=0)


@pytest.mark.asyncio
async def test_execute_push_enroll_success():
    snapshot = json.dumps(
        {"Id": "555", "FirstName": "Jane", "LastName": "Doe", "valid_start": "s", "valid_end": "e"}
    )
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "enroll"
    item.member_snapshot = snapshot
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()
    mock_client.add_user = AsyncMock(return_value=True)

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is True
    call_kwargs = mock_client.add_user.call_args[1]
    assert call_kwargs["card_name"] == "Jane Doe"
    assert call_kwargs["user_id"] == "555"


@pytest.mark.asyncio
async def test_execute_push_update_success():
    snapshot = json.dumps(
        {"card_name": "Jane Doe", "valid_start": "2025-01-01", "valid_end": "2025-12-31"}
    )
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "update"
    item.dahua_user_id = "789"
    item.member_snapshot = snapshot
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()
    mock_client.update_user = AsyncMock(return_value=True)

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is True
    mock_client.update_user.assert_called_once_with(
        "789", card_name="Jane Doe", valid_start="2025-01-01", valid_end="2025-12-31"
    )


@pytest.mark.asyncio
async def test_execute_push_device_returns_failure():
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "deactivate"
    item.dahua_user_id = "123"
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()
    mock_client.update_user_status = AsyncMock(return_value=False)

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is False
    assert err == "Device returned failure"


@pytest.mark.asyncio
async def test_execute_push_exception():
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "deactivate"
    item.dahua_user_id = "123"
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()
    mock_client.update_user_status = AsyncMock(side_effect=ConnectionError("offline"))

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is False
    assert "offline" in err
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_execute_push_unknown_action():
    item = MagicMock(spec=DahuaSyncQueue)
    item.device_id = 1
    item.action = "delete"
    db = AsyncMock()

    device = MagicMock(host="h", port=80, username="u", password="p", door_ids="0")
    mock_client = AsyncMock()

    with (
        patch("app.admin.sync_queue.devices_svc") as mock_svc,
        patch("app.admin.sync_queue.DahuaClient", return_value=mock_client),
    ):
        mock_svc.get_by_id = AsyncMock(return_value=device)
        success, err = await _execute_push(item, db)

    assert success is False
    assert "Unknown action" in err


# ---------------------------------------------------------------------------
# CSV export route tests
# ---------------------------------------------------------------------------


def test_export_csv_empty(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/sync-queue/export.csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "sync_queue.csv" in resp.headers.get("content-disposition", "")
    lines = resp.text.strip().split("\n")
    assert len(lines) == 1  # header only
    assert "id" in lines[0]
    assert "action" in lines[0]


def test_export_csv_with_filter_params(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue/export.csv",
        params={"action": "enroll", "status": "pending"},
    )
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")


# ---------------------------------------------------------------------------
# Push route tests
# ---------------------------------------------------------------------------


def test_push_item_not_found(logged_in_client: TestClient, settings) -> None:
    csrf = generate_csrf_token(settings.secret_key)
    resp = logged_in_client.post(
        "/admin/sync-queue/9999/push",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert resp.status_code == 404


def test_push_success_redirect(logged_in_client: TestClient, app, settings) -> None:
    """Push a pending item — mock DB lookup and push execution."""
    csrf = generate_csrf_token(settings.secret_key)

    fake_item = MagicMock(spec=DahuaSyncQueue)
    fake_item.id = 1
    fake_item.status = "pending"
    fake_item.action = "deactivate"
    fake_item.device_id = 10
    fake_item.mindbody_client_id = "100"

    fake_result = MagicMock()
    fake_result.scalar_one_or_none.return_value = fake_item

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=fake_result)

    from app.models.database import get_async_db

    async def _override():
        yield mock_session

    app.dependency_overrides[get_async_db] = _override

    with (
        patch("app.admin.sync_queue._execute_push", new_callable=AsyncMock) as mock_push,
        patch("app.admin.sync_queue.queue_svc") as mock_q,
    ):
        mock_push.return_value = (True, None)
        mock_q.mark_item = AsyncMock()

        resp = logged_in_client.post(
            "/admin/sync-queue/1/push",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )

    app.dependency_overrides.pop(get_async_db, None)

    assert resp.status_code == 303
    assert "/admin/sync-queue" in resp.headers.get("location", "")
    mock_push.assert_called_once()
    mock_q.mark_item.assert_called_once()


def test_push_already_succeeded(logged_in_client: TestClient, app, settings) -> None:
    csrf = generate_csrf_token(settings.secret_key)

    fake_item = MagicMock(spec=DahuaSyncQueue)
    fake_item.status = "success"

    fake_result = MagicMock()
    fake_result.scalar_one_or_none.return_value = fake_item

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=fake_result)

    from app.models.database import get_async_db

    async def _override():
        yield mock_session

    app.dependency_overrides[get_async_db] = _override

    resp = logged_in_client.post(
        "/admin/sync-queue/2/push",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )

    app.dependency_overrides.pop(get_async_db, None)

    assert resp.status_code == 400
