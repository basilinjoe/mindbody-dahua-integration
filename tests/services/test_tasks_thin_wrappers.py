from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_upsert_mindbody_users_batch_delegates_to_members_service():
    import app.sync.tasks as tasks_mod
    from app.services import members as members_svc

    members_data = [
        {
            "Id": "101",
            "UniqueId": "u1",
            "FirstName": "Alice",
            "LastName": "S",
            "Email": None,
            "MobilePhone": None,
            "HomePhone": None,
            "WorkPhone": None,
            "Status": "Active",
            "Active": True,
            "BirthDate": None,
            "Gender": None,
            "CreationDate": None,
            "LastModifiedDateTime": None,
        }
    ]

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            members_svc, "upsert_batch", new_callable=AsyncMock, return_value=1
        ) as mock_upsert:
            result = await tasks_mod.upsert_mindbody_users_batch.fn(members_data)
            assert result == 1
            mock_upsert.assert_called_once_with(mock_db, members_data, fetched_at=None)


@pytest.mark.asyncio
async def test_upsert_mindbody_memberships_batch_delegates_to_memberships_service():
    import app.sync.tasks as tasks_mod
    from app.services import memberships as memberships_svc

    data = {
        "101": [
            {
                "Id": "c1",
                "Name": "Monthly",
                "Status": "Active",
                "StartDate": None,
                "ExpirationDate": None,
            }
        ]
    }

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            memberships_svc, "upsert_batch", new_callable=AsyncMock, return_value=1
        ) as mock_up:
            result = await tasks_mod.upsert_mindbody_memberships_batch.fn(data)
            assert result == 1
            mock_up.assert_called_once_with(mock_db, data)


@pytest.mark.asyncio
async def test_load_device_ids_by_gate_type_delegates_to_devices_service():
    import app.sync.tasks as tasks_mod
    from app.services import devices as devices_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            devices_svc, "list_by_gate_type", new_callable=AsyncMock, return_value=[1, 2]
        ) as mock_list:
            result = await tasks_mod.load_device_ids_by_gate_type.fn("male")
            assert result == [1, 2]
            mock_list.assert_called_once_with(mock_db, "male")


@pytest.mark.asyncio
async def test_load_active_members_from_db_delegates_to_members_service():
    import app.sync.tasks as tasks_mod
    from app.services import members as members_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            members_svc, "load_active", new_callable=AsyncMock, return_value=[]
        ) as mock_la:
            result = await tasks_mod.load_active_members_from_db.fn()
            assert result == []
            mock_la.assert_called_once_with(mock_db)


@pytest.mark.asyncio
async def test_load_membership_windows_delegates_to_memberships_service():
    import app.sync.tasks as tasks_mod
    from app.services import memberships as memberships_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            memberships_svc, "load_windows", new_callable=AsyncMock, return_value={}
        ) as mock_lw:
            result = await tasks_mod.load_membership_windows.fn(["101"])
            assert result == {}
            mock_lw.assert_called_once_with(mock_db, ["101"])


@pytest.mark.asyncio
async def test_write_sync_queue_batch_delegates_to_queue_service():
    import app.sync.tasks as tasks_mod
    from app.services import queue as queue_svc

    items = [
        {
            "mindbody_client_id": "101",
            "device_id": 1,
            "action": "enroll",
            "status": "pending",
            "dahua_user_id": None,
            "member_snapshot": None,
        }
    ]

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            queue_svc, "write_batch", new_callable=AsyncMock, return_value=1
        ) as mock_wb:
            result = await tasks_mod.write_sync_queue_batch.fn("run-1", items)
            assert result == 1
            mock_wb.assert_called_once_with(mock_db, "run-1", items, flow_type="full")


@pytest.mark.asyncio
async def test_load_pending_queue_items_delegates_to_queue_service():
    import app.sync.tasks as tasks_mod
    from app.services import queue as queue_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            queue_svc, "load_pending", new_callable=AsyncMock, return_value=[]
        ) as mock_lp:
            result = await tasks_mod.load_pending_queue_items.fn("run-1")
            assert result == []
            mock_lp.assert_called_once_with(mock_db, "run-1")


@pytest.mark.asyncio
async def test_mark_queue_item_delegates_to_queue_service():
    import app.sync.tasks as tasks_mod
    from app.services import queue as queue_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(queue_svc, "mark_item", new_callable=AsyncMock) as mock_mi:
            await tasks_mod.mark_queue_item.fn(42, "success", "no error")
            mock_mi.assert_called_once_with(mock_db, 42, "success", "no error")


@pytest.mark.asyncio
async def test_advance_watermark_delegates_to_members_service():
    from datetime import UTC, datetime

    import app.sync.tasks as tasks_mod
    from app.services import members as members_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    ts = datetime(2026, 4, 12, 10, 0, 0, tzinfo=UTC)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            members_svc, "update_last_fetched_at", new_callable=AsyncMock, return_value=2
        ) as mock_update:
            result = await tasks_mod.advance_watermark.fn(["101", "102"], ts)
            assert result == 2
            mock_update.assert_called_once_with(mock_db, ["101", "102"], ts)


@pytest.mark.asyncio
async def test_load_all_devices_delegates_to_devices_service():
    import app.sync.tasks as tasks_mod
    from app.services import devices as devices_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(
            devices_svc, "list_all", new_callable=AsyncMock, return_value=[]
        ) as mock_la:
            result = await tasks_mod.load_all_devices.fn()
            assert result == []
            mock_la.assert_called_once_with(mock_db)
