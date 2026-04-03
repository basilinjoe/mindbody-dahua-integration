from __future__ import annotations

import json

import app.sync.flows.integration as integration_mod


def test_plan_device_operations_creates_all_action_types() -> None:
    """
    Given:
      - active member 101 (already on device, active, window unchanged)  → update_window or no-op
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
    #   101 → CardStatus=0 (active), ValidDateEnd="20260331 235959"
    #   102 → CardStatus=4 (frozen)
    #   103 → CardStatus=0 (active) — no longer in active member set
    dahua_users = [
        {
            "UserID": "101",
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
        },  # expiry changed → update_window
        "102": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
        "104": {"valid_start": "2026-02-01T00:00:00Z", "valid_end": "2026-11-30T23:59:59Z"},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
    )

    by_action = {item["action"]: item for item in items}
    assert set(by_action.keys()) == {"enroll", "deactivate", "reactivate", "update_window"}, (
        f"Expected all 4 action types, got: {set(by_action.keys())}"
    )

    # enroll: member 104 not on device
    enroll = by_action["enroll"]
    assert enroll["device_id"] == 7
    assert enroll["mindbody_client_id"] == "104"
    snapshot = json.loads(enroll["member_snapshot"])
    assert snapshot["valid_start"] == "20260201 000000"
    assert snapshot["valid_end"] == "20261130 235959"

    # deactivate: user 103 on device but not in active set
    deactivate = by_action["deactivate"]
    assert deactivate["dahua_user_id"] == "103"
    assert deactivate["mindbody_client_id"] == "103"

    # reactivate: member 102 frozen on device but has active membership
    reactivate = by_action["reactivate"]
    assert reactivate["dahua_user_id"] == "102"
    assert reactivate["mindbody_client_id"] == "102"

    # update_window: member 101 active but expiry changed (was 2026-03-31, now 2026-12-31)
    update = by_action["update_window"]
    assert update["dahua_user_id"] == "101"
    assert update["mindbody_client_id"] == "101"
    window = json.loads(update["member_snapshot"])
    assert window["valid_end"] == "20261231 235959"


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
    )
    # Should NOT generate a deactivate — already frozen
    assert all(i["action"] != "deactivate" for i in items)
