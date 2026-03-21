from __future__ import annotations

import pytest

from app.sync.flows import integration as integration_mod


class _DummyLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass

    def error(self, *args, **kwargs) -> None:
        pass


@pytest.mark.asyncio
async def test_sync_integration_flow_no_members(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flow returns early when no members fetched."""

    async def fake_fetch_members(**kwargs):
        return []

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)

    # Should not raise — returns early
    await integration_mod.sync_integration_flow.fn(sync_type="test")


@pytest.mark.asyncio
async def test_sync_integration_flow_no_active_members(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flow returns early when all members are inactive."""
    members = [{"Id": "100", "Active": False, "FirstName": "A", "LastName": "B"}]

    async def fake_fetch_members(**kwargs):
        return members

    async def fake_upsert_mindbody_users_batch(m):
        return len(m)

    async def fake_fetch_all_memberships(ids):
        return {}

    async def fake_upsert_mindbody_memberships_batch(m):
        return 0

    async def fake_load_active_members_from_db():
        return []

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_users_batch", fake_upsert_mindbody_users_batch
    )
    monkeypatch.setattr(integration_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_memberships_batch", fake_upsert_mindbody_memberships_batch
    )
    monkeypatch.setattr(
        integration_mod, "load_active_members_from_db", fake_load_active_members_from_db
    )

    await integration_mod.sync_integration_flow.fn(sync_type="test")


@pytest.mark.asyncio
async def test_sync_integration_flow_no_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flow returns early when no devices are configured."""
    members = [{"Id": "100", "Active": True, "FirstName": "A", "LastName": "B", "Gender": "Male"}]

    async def fake_fetch_members(**kwargs):
        return members

    async def fake_upsert_mindbody_users_batch(m):
        return len(m)

    async def fake_fetch_all_memberships(ids):
        return {}

    async def fake_upsert_mindbody_memberships_batch(m):
        return 0

    async def fake_load_active_members_from_db():
        return [{"Id": "100", "Gender": "Male", "FirstName": "A", "LastName": "B"}]

    async def fake_load_device_ids_by_gate_type(gt):
        return []

    async def fake_load_membership_windows(ids):
        return {}

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_users_batch", fake_upsert_mindbody_users_batch
    )
    monkeypatch.setattr(integration_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_memberships_batch", fake_upsert_mindbody_memberships_batch
    )
    monkeypatch.setattr(
        integration_mod, "load_active_members_from_db", fake_load_active_members_from_db
    )
    monkeypatch.setattr(
        integration_mod, "load_device_ids_by_gate_type", fake_load_device_ids_by_gate_type
    )
    monkeypatch.setattr(integration_mod, "load_membership_windows", fake_load_membership_windows)

    await integration_mod.sync_integration_flow.fn(sync_type="test")


@pytest.mark.asyncio
async def test_sync_integration_flow_full_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Full flow run: fetch → plan → push."""
    members = [
        {"Id": "100", "Active": True, "FirstName": "Alice", "LastName": "A", "Gender": "Female"},
        {"Id": "200", "Active": True, "FirstName": "Bob", "LastName": "B", "Gender": "Male"},
    ]
    written_items: list = []

    async def fake_fetch_members(**kwargs):
        return members

    async def fake_upsert_mindbody_users_batch(m):
        return len(m)

    async def fake_fetch_all_memberships(ids):
        return {}

    async def fake_upsert_mindbody_memberships_batch(m):
        return 0

    async def fake_load_active_members_from_db():
        return [
            {"Id": "100", "Gender": "Female", "FirstName": "Alice", "LastName": "A"},
            {"Id": "200", "Gender": "Male", "FirstName": "Bob", "LastName": "B"},
        ]

    async def fake_load_device_ids_by_gate_type(gt):
        if gt == "male":
            return [1]
        return [2]

    async def fake_load_membership_windows(ids):
        return {
            "100": ("2026-01-01T00:00:00Z", "2026-12-31T23:59:59Z"),
            "200": ("2026-01-01T00:00:00Z", "2026-12-31T23:59:59Z"),
        }

    async def fake_fetch_dahua_users_for_device(device_id):
        return []

    async def fake_write_sync_queue_batch(run_id, items):
        written_items.extend(items)
        return len(items)

    async def fake_run_dahua_push(run_id, logger):
        return {"enrolled": 2, "deactivated": 0, "reactivated": 0, "window_updated": 0, "failed": 0}

    async def fake_create_table_artifact(**kwargs):
        pass

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_users_batch", fake_upsert_mindbody_users_batch
    )
    monkeypatch.setattr(integration_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_memberships_batch", fake_upsert_mindbody_memberships_batch
    )
    monkeypatch.setattr(
        integration_mod, "load_active_members_from_db", fake_load_active_members_from_db
    )
    monkeypatch.setattr(
        integration_mod, "load_device_ids_by_gate_type", fake_load_device_ids_by_gate_type
    )
    monkeypatch.setattr(integration_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(
        integration_mod, "fetch_dahua_users_for_device", fake_fetch_dahua_users_for_device
    )
    monkeypatch.setattr(integration_mod, "write_sync_queue_batch", fake_write_sync_queue_batch)
    monkeypatch.setattr(integration_mod, "run_dahua_push", fake_run_dahua_push)
    monkeypatch.setattr(integration_mod, "create_table_artifact", fake_create_table_artifact)

    await integration_mod.sync_integration_flow.fn(sync_type="test")

    assert len(written_items) == 2
    actions = {item["action"] for item in written_items}
    assert actions == {"enroll"}


@pytest.mark.asyncio
async def test_sync_integration_flow_dedup_members(monkeypatch: pytest.MonkeyPatch) -> None:
    """Duplicate member IDs in API response are deduplicated (active preferred)."""
    members = [
        {"Id": "100", "Active": False, "FirstName": "Old", "LastName": "A"},
        {"Id": "100", "Active": True, "FirstName": "New", "LastName": "A"},
    ]

    upserted: list = []

    async def fake_fetch_members(**kwargs):
        return members

    async def fake_upsert_mindbody_users_batch(m):
        upserted.extend(m)
        return len(m)

    async def fake_fetch_all_memberships(ids):
        return {}

    async def fake_upsert_mindbody_memberships_batch(m):
        return 0

    async def fake_load_active_members_from_db():
        return []

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_users_batch", fake_upsert_mindbody_users_batch
    )
    monkeypatch.setattr(integration_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_memberships_batch", fake_upsert_mindbody_memberships_batch
    )
    monkeypatch.setattr(
        integration_mod, "load_active_members_from_db", fake_load_active_members_from_db
    )

    await integration_mod.sync_integration_flow.fn(sync_type="test")

    # After dedup, only the active member should remain
    assert len(upserted) == 1
    assert upserted[0]["Active"] is True


@pytest.mark.asyncio
async def test_sync_integration_flow_ungendered_members(monkeypatch: pytest.MonkeyPatch) -> None:
    """Members with no gender route to all gates."""
    members = [{"Id": "100", "Active": True, "FirstName": "A", "LastName": "B"}]
    written_items: list = []

    async def fake_fetch_members(**kwargs):
        return members

    async def fake_upsert_mindbody_users_batch(m):
        return len(m)

    async def fake_fetch_all_memberships(ids):
        return {}

    async def fake_upsert_mindbody_memberships_batch(m):
        return 0

    async def fake_load_active_members_from_db():
        return [{"Id": "100", "Gender": None, "FirstName": "A", "LastName": "B"}]

    async def fake_load_device_ids_by_gate_type(gt):
        if gt == "male":
            return [1]
        return [2]

    async def fake_load_membership_windows(ids):
        return {"100": ("2026-01-01T00:00:00Z", "2026-12-31T23:59:59Z")}

    async def fake_fetch_dahua_users_for_device(device_id):
        return []

    async def fake_write_sync_queue_batch(run_id, items):
        written_items.extend(items)
        return len(items)

    async def fake_run_dahua_push(run_id, logger):
        return {"enrolled": 2, "deactivated": 0, "reactivated": 0, "window_updated": 0, "failed": 0}

    async def fake_create_table_artifact(**kwargs):
        pass

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_users_batch", fake_upsert_mindbody_users_batch
    )
    monkeypatch.setattr(integration_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_memberships_batch", fake_upsert_mindbody_memberships_batch
    )
    monkeypatch.setattr(
        integration_mod, "load_active_members_from_db", fake_load_active_members_from_db
    )
    monkeypatch.setattr(
        integration_mod, "load_device_ids_by_gate_type", fake_load_device_ids_by_gate_type
    )
    monkeypatch.setattr(integration_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(
        integration_mod, "fetch_dahua_users_for_device", fake_fetch_dahua_users_for_device
    )
    monkeypatch.setattr(integration_mod, "write_sync_queue_batch", fake_write_sync_queue_batch)
    monkeypatch.setattr(integration_mod, "run_dahua_push", fake_run_dahua_push)
    monkeypatch.setattr(integration_mod, "create_table_artifact", fake_create_table_artifact)

    await integration_mod.sync_integration_flow.fn(sync_type="test")

    # Ungendered member should be enrolled on both male (device 1) and female (device 2)
    assert len(written_items) == 2
    device_ids = {item["device_id"] for item in written_items}
    assert device_ids == {1, 2}


@pytest.mark.asyncio
async def test_sync_integration_flow_device_fetch_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Flow handles device fetch errors gracefully."""
    members = [{"Id": "100", "Active": True, "FirstName": "A", "LastName": "B", "Gender": "Male"}]

    async def fake_fetch_members(**kwargs):
        return members

    async def fake_upsert_mindbody_users_batch(m):
        return len(m)

    async def fake_fetch_all_memberships(ids):
        return {}

    async def fake_upsert_mindbody_memberships_batch(m):
        return 0

    async def fake_load_active_members_from_db():
        return [{"Id": "100", "Gender": "Male", "FirstName": "A", "LastName": "B"}]

    async def fake_load_device_ids_by_gate_type(gt):
        if gt == "male":
            return [1]
        return []

    async def fake_load_membership_windows(ids):
        return {"100": ("2026-01-01T00:00:00Z", "2026-12-31T23:59:59Z")}

    async def fake_fetch_dahua_users_for_device(device_id):
        raise ConnectionError("device unreachable")

    async def fake_write_sync_queue_batch(run_id, items):
        return len(items)

    async def fake_run_dahua_push(run_id, logger):
        return {"enrolled": 1, "deactivated": 0, "reactivated": 0, "window_updated": 0, "failed": 0}

    async def fake_create_table_artifact(**kwargs):
        pass

    monkeypatch.setattr(integration_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(integration_mod, "flow_run", type("FR", (), {"id": "test-run-id"})())
    monkeypatch.setattr(integration_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_users_batch", fake_upsert_mindbody_users_batch
    )
    monkeypatch.setattr(integration_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        integration_mod, "upsert_mindbody_memberships_batch", fake_upsert_mindbody_memberships_batch
    )
    monkeypatch.setattr(
        integration_mod, "load_active_members_from_db", fake_load_active_members_from_db
    )
    monkeypatch.setattr(
        integration_mod, "load_device_ids_by_gate_type", fake_load_device_ids_by_gate_type
    )
    monkeypatch.setattr(integration_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(
        integration_mod, "fetch_dahua_users_for_device", fake_fetch_dahua_users_for_device
    )
    monkeypatch.setattr(integration_mod, "write_sync_queue_batch", fake_write_sync_queue_batch)
    monkeypatch.setattr(integration_mod, "run_dahua_push", fake_run_dahua_push)
    monkeypatch.setattr(integration_mod, "create_table_artifact", fake_create_table_artifact)

    # Should not raise even if device fetch fails
    await integration_mod.sync_integration_flow.fn(sync_type="test")
