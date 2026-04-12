from __future__ import annotations

import json

import app.sync.flows.integration as integration_mod


def test_plan_device_operations_creates_all_action_types() -> None:
    """
    Given:
      - active member 101 (already on device, active, window unchanged)  → update or no-op
      - active member 102 (already on device, FROZEN)                    → reactivate
      - active member 104 (NOT on device yet)                            → enroll
      - Dahua user  103 (on device, active, NOT in active set)           → deactivate

    Expected: one item of each action type.
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
    # Dahua device state:
    #   101 → CardStatus=0 (active), ValidDateEnd="2026-03-31 23:59:59"
    #   102 → CardStatus=4 (frozen)
    #   103 → CardStatus=0 (active) — no longer in active member set
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
        "101": {
            "valid_start": "2026-01-01T00:00:00Z",
            "valid_end": "2026-12-31T23:59:59Z",
        },  # expiry changed → update
        "102": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
        "104": {"valid_start": "2026-02-01T00:00:00Z", "valid_end": "2026-11-30T23:59:59Z"},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
        known_mindbody_ids={"101", "102", "103", "104"},
    )

    by_action: dict[str, list[dict]] = {}
    for item in items:
        by_action.setdefault(item["action"], []).append(item)
    assert set(by_action.keys()) == {"enroll", "deactivate", "reactivate", "update"}, (
        f"Expected all 4 action types, got: {set(by_action.keys())}"
    )

    # enroll: member 104 not on device
    enroll = by_action["enroll"][0]
    assert enroll["device_id"] == 7
    assert enroll["mindbody_client_id"] == "104"
    snapshot = json.loads(enroll["member_snapshot"])
    assert snapshot["valid_start"] == "20260201 000000"
    assert snapshot["valid_end"] == "20261130 235959"

    # deactivate: user 103 on device but not in active set
    deactivate = by_action["deactivate"][0]
    assert deactivate["dahua_user_id"] == "103"
    assert deactivate["mindbody_client_id"] == "103"

    # reactivate: member 102 frozen on device but has active membership
    reactivate = by_action["reactivate"][0]
    assert reactivate["dahua_user_id"] == "102"
    assert reactivate["mindbody_client_id"] == "102"

    # update: member 101 window changed + member 102 window updated after reactivation
    update_ids = {u["mindbody_client_id"] for u in by_action["update"]}
    assert "101" in update_ids, "Member 101 should get a window update"
    assert "102" in update_ids, "Member 102 should get a window update after reactivation"

    # Verify member 101's window update snapshot
    update_101 = [u for u in by_action["update"] if u["mindbody_client_id"] == "101"][0]
    snapshot = json.loads(update_101["member_snapshot"])
    assert snapshot["card_name"] is None  # name unchanged
    assert snapshot["valid_end"] == "20261231 235959"


def test_plan_recognises_existing_user_with_normalized_id() -> None:
    """MindBody ID '00123' should match device UserID '123' (normalized by _make_dahua_user_id)."""
    active_member_ids = {"00123"}
    member_map = {
        "00123": {
            "Id": "00123",
            "FirstName": "A",
            "LastName": "B",
            "Gender": "male",
            "Email": None,
        },
    }
    dahua_users = [
        {
            "UserID": "123",  # device stores normalized form
            "CardName": "A B",
            "CardStatus": "0",
            "ValidDateStart": "20260101 000000",
            "ValidDateEnd": "20261231 235959",
        },
    ]
    membership_windows = {
        "00123": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
        known_mindbody_ids={"00123"},
    )
    # Should NOT try to enroll — the member already exists on the device
    assert all(i["action"] != "enroll" for i in items), (
        f"Should not enroll already-present member, got: {items}"
    )
    # Should NOT deactivate — the member is active
    assert all(i["action"] != "deactivate" for i in items), (
        f"Should not deactivate active member, got: {items}"
    )


def test_plan_device_operations_returns_empty_when_no_active_members() -> None:
    """No active members and empty Dahua device → no operations."""
    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=set(),
        member_map={},
        dahua_users=[],
        membership_windows={},
        known_mindbody_ids=set(),
    )
    assert items == []


def test_plan_device_operations_no_op_when_already_in_sync() -> None:
    """Active member already enrolled with correct window → no operations."""
    active_member_ids = {"101"}
    member_map = {
        "101": {
            "Id": "101",
            "FirstName": "A",
            "LastName": "B",
            "Gender": "male",
            "PhotoUrl": None,
            "Email": None,
        },
    }
    dahua_users = [
        {
            "UserID": "101",
            "CardName": "A B",
            "CardStatus": "0",
            "ValidDateStart": "20260101 000000",
            "ValidDateEnd": "20261231 235959",
        },
    ]
    membership_windows = {
        "101": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
        known_mindbody_ids={"101"},
    )
    assert items == [], f"Expected no operations but got: {items}"


def test_plan_device_operations_skips_already_frozen_users() -> None:
    """Users already frozen on device (CardStatus=4) not in active set → skip deactivate."""
    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=set(),
        member_map={},
        dahua_users=[{"UserID": "999", "CardStatus": "4", "ValidDateEnd": ""}],
        membership_windows={},
        known_mindbody_ids={"999"},
    )
    # Should NOT generate a deactivate — already frozen
    assert all(i["action"] != "deactivate" for i in items)


def test_plan_skips_manually_managed_device_users() -> None:
    """Users on device that are NOT in known_mindbody_ids should never be deactivated."""
    active_member_ids = {"101"}
    member_map = {
        "101": {
            "Id": "101",
            "FirstName": "A",
            "LastName": "B",
            "Gender": "male",
            "Email": None,
        },
    }
    dahua_users = [
        # Active MindBody member
        {
            "UserID": "101",
            "CardName": "A B",
            "CardStatus": "0",
            "ValidDateStart": "20260101 000000",
            "ValidDateEnd": "20261231 235959",
        },
        # Manually-added user (not in MindBody at all)
        {"UserID": "GUARD01", "CardName": "Security Guard", "CardStatus": "0"},
        # Lapsed MindBody member (in known set but not active)
        {"UserID": "103", "CardName": "Old Member", "CardStatus": "0"},
    ]
    membership_windows = {
        "101": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
        known_mindbody_ids={"101", "103"},  # GUARD01 is NOT in this set
    )

    deactivated_ids = [i["mindbody_client_id"] for i in items if i["action"] == "deactivate"]
    assert "103" in deactivated_ids, "Lapsed MindBody member should be deactivated"
    assert "GUARD01" not in deactivated_ids, "Manually-managed user must NOT be deactivated"


def test_plan_skips_all_when_only_manual_users_on_device() -> None:
    """Device has only manually-managed users → zero deactivate actions."""
    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=set(),
        member_map={},
        dahua_users=[
            {"UserID": "GUARD01", "CardStatus": "0"},
            {"UserID": "STAFF02", "CardStatus": "0"},
        ],
        membership_windows={},
        known_mindbody_ids=set(),  # neither user is a MindBody member
    )
    assert all(i["action"] != "deactivate" for i in items), (
        f"No deactivations expected for manual-only users, got: {items}"
    )
