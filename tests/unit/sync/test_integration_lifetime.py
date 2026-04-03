"""Tests for lifetime membership (null expiration) and null gender edge cases."""

from __future__ import annotations

import json

import app.sync.flows.integration as integration_mod


def test_plan_updates_window_for_lifetime_membership() -> None:
    """Lifetime membership (no expiration_date) with a start date should still update window."""
    active_member_ids = {"200"}
    member_map = {
        "200": {"Id": "200", "FirstName": "A", "LastName": "B", "Gender": "male", "Email": None},
    }
    dahua_users = [
        {
            "UserID": "200",
            "CardStatus": "0",
            "ValidDateStart": "",
            "ValidDateEnd": "",
        },
    ]
    # Lifetime membership: start_date present, expiration_date is None
    membership_windows = {
        "200": {"valid_start": "2026-01-01T00:00:00Z", "valid_end": None},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
    )

    assert len(items) == 1
    assert items[0]["action"] == "update_window"
    window = json.loads(items[0]["member_snapshot"])
    assert window["valid_start"] == "2026-01-01 00:00:00"
    assert window["valid_end"] is None


def test_plan_no_op_for_lifetime_no_dates() -> None:
    """Lifetime membership with both dates None and empty device dates → no update needed."""
    active_member_ids = {"300"}
    member_map = {
        "300": {"Id": "300", "FirstName": "C", "LastName": "D", "Gender": "female", "Email": None},
    }
    dahua_users = [
        {"UserID": "300", "CardStatus": "0", "ValidDateStart": "", "ValidDateEnd": ""},
    ]
    membership_windows = {
        "300": {"valid_start": None, "valid_end": None},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
    )
    assert items == [], f"Expected no operations but got: {items}"


def test_plan_updates_when_only_start_changed() -> None:
    """Start date changed but end date same → should update window."""
    active_member_ids = {"400"}
    member_map = {
        "400": {"Id": "400", "FirstName": "E", "LastName": "F", "Gender": "male", "Email": None},
    }
    dahua_users = [
        {
            "UserID": "400",
            "CardStatus": "0",
            "ValidDateStart": "2026-01-01 00:00:00",
            "ValidDateEnd": "2026-12-31 23:59:59",
        },
    ]
    # Start date changed, end date same
    membership_windows = {
        "400": {"valid_start": "2026-03-01T00:00:00Z", "valid_end": "2026-12-31T23:59:59Z"},
    }

    items = integration_mod._plan_device_operations(
        device_id=7,
        active_member_ids=active_member_ids,
        member_map=member_map,
        dahua_users=dahua_users,
        membership_windows=membership_windows,
    )

    assert len(items) == 1
    assert items[0]["action"] == "update_window"
    window = json.loads(items[0]["member_snapshot"])
    assert window["valid_start"] == "2026-03-01 00:00:00"
