from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import app.sync.flows.incremental as incremental_mod


class _DummyLogger:
    def info(self, *_args, **_kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    def warning(self, *_args, **_kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    def error(self, *_args, **_kwargs) -> None:  # noqa: ANN002, ANN003
        return None


# ---------------------------------------------------------------------------
# _plan_incremental_operations — unit tests
# ---------------------------------------------------------------------------


def test_plan_incremental_enroll_reactivate_update() -> None:
    """
    Given:
      - active member 101 (on device, active, window changed) → update
      - active member 102 (on device, FROZEN)                 → reactivate
      - active member 104 (NOT on device)                     → enroll
      - Dahua user  103 (on device, NOT in active set)        → NO deactivation
    """
    active_member_ids = {"101", "102", "104"}
    member_map = {
        "101": {
            "Id": "101",
            "FirstName": "Alex",
            "LastName": "One",
            "Gender": "male",
            "Email": "101@x.com",
        },
        "102": {
            "Id": "102",
            "FirstName": "Blair",
            "LastName": "Two",
            "Gender": "male",
            "Email": "102@x.com",
        },
        "104": {
            "Id": "104",
            "FirstName": "Casey",
            "LastName": "Four",
            "Gender": "male",
            "Email": "104@x.com",
        },
    }
    dahua_users = [
        {
            "UserID": "101",
            "CardName": "Alex One",
            "CardStatus": "0",
            "ValidDateStart": "20260101 000000",
            "ValidDateEnd": "20260331 235959",
        },
        {"UserID": "102", "CardStatus": "4", "ValidDateStart": "", "ValidDateEnd": ""},
        {"UserID": "103", "CardStatus": "0", "ValidDateStart": "", "ValidDateEnd": ""},
    ]
    membership_windows = {
        "101": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
        "102": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
        "104": {"valid_start": "2026-02-01T00:00:00Z", "valid_end": "2026-11-30T23:59:59Z"},
    }

    items = incremental_mod._plan_incremental_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
    )

    by_action: dict[str, list[dict]] = {}
    for item in items:
        by_action.setdefault(item["action"], []).append(item)

    # Should produce enroll, reactivate, update — but NO deactivation
    assert "deactivate" not in by_action, "Incremental plan must not produce deactivation"
    assert set(by_action.keys()) == {"enroll", "reactivate", "update"}

    # enroll: member 104 not on device
    enroll = by_action["enroll"][0]
    assert enroll["device_id"] == 7
    assert enroll["mindbody_client_id"] == "104"
    snapshot = json.loads(enroll["member_snapshot"])
    assert snapshot["valid_start"] == "20260201 000000"
    assert snapshot["valid_end"] == "20261130 235959"

    # reactivate: member 102 frozen on device
    reactivate = by_action["reactivate"][0]
    assert reactivate["dahua_user_id"] == "102"
    assert reactivate["mindbody_client_id"] == "102"

    # update: member 101 window changed + member 102 window updated after reactivation
    update_ids = {u["mindbody_client_id"] for u in by_action["update"]}
    assert "101" in update_ids, "Member 101 should get a window update"
    assert "102" in update_ids, "Member 102 should get a window update after reactivation"


def test_plan_incremental_no_deactivation_for_absent_members() -> None:
    """Users on device but not in active set should NOT be deactivated."""
    items = incremental_mod._plan_incremental_operations(
        device_id=7,
        active_member_ids=set(),
        member_map={},
        dahua_users=[
            {"UserID": "999", "CardStatus": "0", "ValidDateStart": "", "ValidDateEnd": ""}
        ],
        membership_windows={},
    )
    assert items == [], f"Expected no operations, got: {items}"


def test_plan_incremental_no_op_when_in_sync() -> None:
    """Active member already enrolled with correct window → no operations."""
    items = incremental_mod._plan_incremental_operations(
        device_id=7,
        active_member_ids={"101"},
        member_map={
            "101": {"Id": "101", "FirstName": "A", "LastName": "B", "Gender": "male", "Email": None}
        },
        dahua_users=[
            {
                "UserID": "101",
                "CardName": "A B",
                "CardStatus": "0",
                "ValidDateStart": "20260101 000000",
                "ValidDateEnd": "20261231 235959",
            }
        ],
        membership_windows={
            "101": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"}
        },
    )
    assert items == [], f"Expected no operations but got: {items}"


def test_plan_incremental_normalized_id() -> None:
    """MindBody ID '00123' should match device UserID '123'."""
    items = incremental_mod._plan_incremental_operations(
        device_id=7,
        active_member_ids={"00123"},
        member_map={
            "00123": {
                "Id": "00123",
                "FirstName": "A",
                "LastName": "B",
                "Gender": "male",
                "Email": None,
            }
        },
        dahua_users=[
            {
                "UserID": "123",
                "CardName": "A B",
                "CardStatus": "0",
                "ValidDateStart": "20260101 000000",
                "ValidDateEnd": "20261231 235959",
            }
        ],
        membership_windows={
            "00123": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"}
        },
    )
    assert all(i["action"] != "enroll" for i in items), "Should not enroll already-present member"


# ---------------------------------------------------------------------------
# sync_incremental_flow — integration-style tests with monkeypatching
# ---------------------------------------------------------------------------


@pytest.fixture()
def _patch_flow(monkeypatch: pytest.MonkeyPatch):
    """Shared fixture to disable Prefect decorators and DB calls."""
    monkeypatch.setattr(incremental_mod, "get_run_logger", lambda: _DummyLogger())
    monkeypatch.setattr(incremental_mod, "create_table_artifact", _noop_coro)

    async def noop_ensure():
        pass

    async def noop_archive(run_id, flow_type=None):
        return 0

    async def noop_advance(client_ids, timestamp):
        return len(client_ids)

    monkeypatch.setattr(incremental_mod, "ensure_timestamps_tz", noop_ensure)
    monkeypatch.setattr(incremental_mod, "archive_previous_sync_queue", noop_archive)
    monkeypatch.setattr(incremental_mod, "advance_watermark", noop_advance)

    # Patch flow_run.id
    monkeypatch.setattr(incremental_mod.flow_run, "id", "test-run-123")


async def _noop_coro(**_kwargs):
    return None


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_no_watermark_returns_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When load_last_fetched_at returns None, the flow should return early."""
    fetch_called = False

    async def fake_load_last_fetched_at():
        return None

    async def fake_fetch_members(modified_since=None):
        nonlocal fetch_called
        fetch_called = True
        return []

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")

    assert not fetch_called, "fetch_members should not be called when no watermark exists"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_no_changes_returns_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When fetch_members returns empty, the flow should return early."""
    watermark = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)

    async def fake_load_last_fetched_at():
        return watermark

    async def fake_fetch_members(modified_since=None):
        assert modified_since == watermark - incremental_mod._WATERMARK_MARGIN
        return []

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_no_active_among_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Changed members exist but none are active — returns early after upsert."""
    watermark = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)

    async def fake_load_last_fetched_at():
        return watermark

    async def fake_fetch_members(modified_since=None):
        return [{"Id": "101", "Active": False, "FirstName": "A", "LastName": "B"}]

    upsert_called = False
    membership_fetch_called = False

    async def fake_upsert(members, fetched_at=None):
        nonlocal upsert_called
        upsert_called = True
        return len(members)

    async def fake_fetch_all_memberships(client_ids):
        nonlocal membership_fetch_called
        membership_fetch_called = True
        return {}

    async def fake_upsert_memberships(memberships_by_client):
        return 0

    async def fake_load_active_by_ids(client_ids):
        return []  # none active

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(incremental_mod, "upsert_mindbody_users_batch", fake_upsert)
    monkeypatch.setattr(incremental_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        incremental_mod, "upsert_mindbody_memberships_batch", fake_upsert_memberships
    )
    monkeypatch.setattr(incremental_mod, "load_active_members_by_ids", fake_load_active_by_ids)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")

    assert upsert_called, "Should still upsert changed members even if none are active"
    assert not membership_fetch_called, "Should skip membership API fetch when no active members"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_enroll_new_active_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """New active member should be enrolled on devices."""
    watermark = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    written_items: list[dict] = []

    async def fake_load_last_fetched_at():
        return watermark

    async def fake_fetch_members(modified_since=None):
        return [
            {
                "Id": "201",
                "Active": True,
                "FirstName": "New",
                "LastName": "Member",
                "Gender": "Male",
            }
        ]

    async def fake_upsert(members, fetched_at=None):
        return len(members)

    async def fake_fetch_all_memberships(client_ids):
        return {"201": [{"Id": "m1", "Name": "Monthly", "Status": "Active"}]}

    async def fake_upsert_memberships(memberships_by_client):
        return 1

    async def fake_load_active_by_ids(client_ids):
        return [
            SimpleNamespace(
                mindbody_id="201",
                first_name="New",
                last_name="Member",
                gender="Male",
                email=None,
            )
        ]

    async def fake_load_device_ids(gate_type):
        return [10] if gate_type == "male" else []

    async def fake_load_membership_windows(client_ids):
        return {"201": {"valid_start": "2026-04-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"}}

    async def fake_fetch_dahua_users(device_id):
        return []  # empty device

    async def fake_write_queue(run_id, items, flow_type="full"):
        written_items.extend(items)
        return len(items)

    async def fake_push(run_id, flow_logger):
        return {"enrolled": 1, "reactivated": 0, "updated": 0, "failed": 0}

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(incremental_mod, "upsert_mindbody_users_batch", fake_upsert)
    monkeypatch.setattr(incremental_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        incremental_mod, "upsert_mindbody_memberships_batch", fake_upsert_memberships
    )
    monkeypatch.setattr(incremental_mod, "load_active_members_by_ids", fake_load_active_by_ids)
    monkeypatch.setattr(incremental_mod, "load_device_ids_by_gate_type", fake_load_device_ids)
    monkeypatch.setattr(incremental_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(incremental_mod, "fetch_dahua_users_for_device", fake_fetch_dahua_users)
    monkeypatch.setattr(incremental_mod, "write_sync_queue_batch", fake_write_queue)
    monkeypatch.setattr(incremental_mod, "run_dahua_push", fake_push)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")

    assert len(written_items) == 1
    assert written_items[0]["action"] == "enroll"
    assert written_items[0]["mindbody_client_id"] == "201"
    assert written_items[0]["device_id"] == 10


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_gender_routing_ungendered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ungendered member should be routed to both male and female gates."""
    watermark = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    written_items: list[dict] = []

    async def fake_load_last_fetched_at():
        return watermark

    async def fake_fetch_members(modified_since=None):
        return [
            {
                "Id": "301",
                "Active": True,
                "FirstName": "Ungendered",
                "LastName": "Person",
                "Gender": None,
            }
        ]

    async def fake_upsert(members, fetched_at=None):
        return len(members)

    async def fake_fetch_all_memberships(client_ids):
        return {"301": [{"Id": "m1", "Name": "Monthly", "Status": "Active"}]}

    async def fake_upsert_memberships(memberships_by_client):
        return 1

    async def fake_load_active_by_ids(client_ids):
        return [
            SimpleNamespace(
                mindbody_id="301",
                first_name="Ungendered",
                last_name="Person",
                gender=None,
                email=None,
            )
        ]

    async def fake_load_device_ids(gate_type):
        if gate_type == "male":
            return [10]
        if gate_type == "female":
            return [20]
        return []

    async def fake_load_membership_windows(client_ids):
        return {"301": {"valid_start": "2026-04-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"}}

    async def fake_fetch_dahua_users(device_id):
        return []

    async def fake_write_queue(run_id, items, flow_type="full"):
        written_items.extend(items)
        return len(items)

    async def fake_push(run_id, flow_logger):
        return {"enrolled": 2, "reactivated": 0, "updated": 0, "failed": 0}

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(incremental_mod, "upsert_mindbody_users_batch", fake_upsert)
    monkeypatch.setattr(incremental_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        incremental_mod, "upsert_mindbody_memberships_batch", fake_upsert_memberships
    )
    monkeypatch.setattr(incremental_mod, "load_active_members_by_ids", fake_load_active_by_ids)
    monkeypatch.setattr(incremental_mod, "load_device_ids_by_gate_type", fake_load_device_ids)
    monkeypatch.setattr(incremental_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(incremental_mod, "fetch_dahua_users_for_device", fake_fetch_dahua_users)
    monkeypatch.setattr(incremental_mod, "write_sync_queue_batch", fake_write_queue)
    monkeypatch.setattr(incremental_mod, "run_dahua_push", fake_push)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")

    # Should produce enroll for both male device 10 and female device 20
    assert len(written_items) == 2
    device_ids = {item["device_id"] for item in written_items}
    assert device_ids == {10, 20}
    assert all(item["action"] == "enroll" for item in written_items)
    assert all(item["mindbody_client_id"] == "301" for item in written_items)


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_dedup_prefers_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate member IDs should be deduped, preferring active over inactive."""
    watermark = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    written_items: list[dict] = []

    async def fake_load_last_fetched_at():
        return watermark

    async def fake_fetch_members(modified_since=None):
        return [
            {"Id": "401", "Active": False, "FirstName": "Old", "LastName": "X", "Gender": "Male"},
            {"Id": "401", "Active": True, "FirstName": "New", "LastName": "X", "Gender": "Male"},
        ]

    async def fake_upsert(members, fetched_at=None):
        return len(members)

    async def fake_fetch_all_memberships(client_ids):
        return {"401": [{"Id": "m1", "Name": "Monthly", "Status": "Active"}]}

    async def fake_upsert_memberships(memberships_by_client):
        return 1

    async def fake_load_active_by_ids(client_ids):
        return [
            SimpleNamespace(
                mindbody_id="401",
                first_name="New",
                last_name="X",
                gender="Male",
                email=None,
            )
        ]

    async def fake_load_device_ids(gate_type):
        return [10] if gate_type == "male" else []

    async def fake_load_membership_windows(client_ids):
        return {"401": {"valid_start": "2026-04-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"}}

    async def fake_fetch_dahua_users(device_id):
        return []

    async def fake_write_queue(run_id, items, flow_type="full"):
        written_items.extend(items)
        return len(items)

    async def fake_push(run_id, flow_logger):
        return {"enrolled": 1, "reactivated": 0, "updated": 0, "failed": 0}

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(incremental_mod, "upsert_mindbody_users_batch", fake_upsert)
    monkeypatch.setattr(incremental_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        incremental_mod, "upsert_mindbody_memberships_batch", fake_upsert_memberships
    )
    monkeypatch.setattr(incremental_mod, "load_active_members_by_ids", fake_load_active_by_ids)
    monkeypatch.setattr(incremental_mod, "load_device_ids_by_gate_type", fake_load_device_ids)
    monkeypatch.setattr(incremental_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(incremental_mod, "fetch_dahua_users_for_device", fake_fetch_dahua_users)
    monkeypatch.setattr(incremental_mod, "write_sync_queue_batch", fake_write_queue)
    monkeypatch.setattr(incremental_mod, "run_dahua_push", fake_push)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")

    # Only one enroll despite duplicate API response
    assert len(written_items) == 1
    assert written_items[0]["mindbody_client_id"] == "401"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_patch_flow")
async def test_incremental_flow_no_devices_returns_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no devices are enabled, the flow should return early after classification."""
    watermark = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)

    async def fake_load_last_fetched_at():
        return watermark

    async def fake_fetch_members(modified_since=None):
        return [{"Id": "501", "Active": True, "FirstName": "A", "LastName": "B", "Gender": "Male"}]

    async def fake_upsert(members, fetched_at=None):
        return len(members)

    async def fake_fetch_all_memberships(client_ids):
        return {"501": [{"Id": "m1", "Name": "Monthly", "Status": "Active"}]}

    async def fake_upsert_memberships(memberships_by_client):
        return 1

    async def fake_load_active_by_ids(client_ids):
        return [
            SimpleNamespace(
                mindbody_id="501", first_name="A", last_name="B", gender="Male", email=None
            )
        ]

    async def fake_load_device_ids(gate_type):
        return []  # no devices

    async def fake_load_membership_windows(client_ids):
        return {"501": {"valid_start": None, "valid_end": None}}

    write_called = False

    async def fake_write_queue(run_id, items, flow_type="full"):
        nonlocal write_called
        write_called = True
        return 0

    monkeypatch.setattr(incremental_mod, "load_last_fetched_at", fake_load_last_fetched_at)
    monkeypatch.setattr(incremental_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(incremental_mod, "upsert_mindbody_users_batch", fake_upsert)
    monkeypatch.setattr(incremental_mod, "fetch_all_memberships", fake_fetch_all_memberships)
    monkeypatch.setattr(
        incremental_mod, "upsert_mindbody_memberships_batch", fake_upsert_memberships
    )
    monkeypatch.setattr(incremental_mod, "load_active_members_by_ids", fake_load_active_by_ids)
    monkeypatch.setattr(incremental_mod, "load_device_ids_by_gate_type", fake_load_device_ids)
    monkeypatch.setattr(incremental_mod, "load_membership_windows", fake_load_membership_windows)
    monkeypatch.setattr(incremental_mod, "write_sync_queue_batch", fake_write_queue)

    await incremental_mod.sync_incremental_flow.fn(sync_type="test")

    assert not write_called, "Should not write queue when no devices available"
