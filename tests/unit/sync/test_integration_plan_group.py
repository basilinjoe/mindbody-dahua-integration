from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from app.sync.flows import integration as integration_mod


@dataclass
class _Enrollment:
    id: int
    dahua_user_id: str
    is_active: bool
    valid_end: str | None


@pytest.mark.asyncio
async def test_plan_group_creates_all_operation_types(monkeypatch: pytest.MonkeyPatch) -> None:
    group_members = [
        {"Id": "101", "FirstName": "Alex", "LastName": "One", "Gender": "male", "PhotoUrl": "https://img/101", "Email": "101@example.com"},
        {"Id": "102", "FirstName": "Blair", "LastName": "Two", "Gender": "male", "PhotoUrl": "https://img/102", "Email": "102@example.com"},
        {"Id": "104", "FirstName": "Casey", "LastName": "Four", "Gender": "male", "PhotoUrl": "https://img/104", "Email": "104@example.com"},
    ]
    active_ids = {"101", "102", "104"}
    device_ids = [7]

    enrollments = {
        "101": _Enrollment(id=501, dahua_user_id="101", is_active=True, valid_end="2026-03-31 23:59:59"),
        "102": _Enrollment(id=502, dahua_user_id="102", is_active=False, valid_end="2026-12-31 23:59:59"),
        "103": _Enrollment(id=503, dahua_user_id="103", is_active=True, valid_end="2026-10-10 23:59:59"),
    }
    membership_windows = {
        "101": ("2026-01-01T00:00:00Z", "2026-12-31T23:59:59Z"),
        "102": ("2026-01-01T00:00:00Z", "2026-12-31T23:59:59Z"),
        "104": ("2026-02-01T00:00:00Z", "2026-11-30T23:59:59Z"),
    }

    async def fake_load_enrollments_for_device(device_id: int):  # noqa: ANN202
        assert device_id == 7
        return enrollments

    async def fake_load_membership_windows(client_ids: list[str]):  # noqa: ANN202
        assert set(client_ids) == {"101", "102", "104"}
        return membership_windows

    monkeypatch.setattr(integration_mod, "load_enrollments_for_device", fake_load_enrollments_for_device)
    monkeypatch.setattr(integration_mod, "load_membership_windows", fake_load_membership_windows)

    logger = MagicMock()
    items = await integration_mod._plan_group(group_members, device_ids, active_ids, logger)

    by_action = {item["action"]: item for item in items}
    assert set(by_action.keys()) == {"enroll", "deactivate", "reactivate", "update_window"}

    enroll_item = by_action["enroll"]
    enroll_snapshot = json.loads(enroll_item["member_snapshot"])
    assert enroll_item["device_id"] == 7
    assert enroll_item["mindbody_client_id"] == "104"
    assert enroll_snapshot["valid_start"] == "2026-02-01 00:00:00"
    assert enroll_snapshot["valid_end"] == "2026-11-30 23:59:59"

    deactivate_item = by_action["deactivate"]
    assert deactivate_item["dahua_user_id"] == "103"
    assert deactivate_item["enrollment_id"] == 503

    reactivate_item = by_action["reactivate"]
    assert reactivate_item["dahua_user_id"] == "102"
    assert reactivate_item["enrollment_id"] == 502

    update_item = by_action["update_window"]
    update_snapshot = json.loads(update_item["member_snapshot"])
    assert update_item["dahua_user_id"] == "101"
    assert update_item["enrollment_id"] == 501
    assert update_snapshot["valid_start"] == "2026-01-01 00:00:00"
    assert update_snapshot["valid_end"] == "2026-12-31 23:59:59"


@pytest.mark.asyncio
async def test_plan_group_returns_empty_when_no_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    async def should_not_run(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("loader should not run when device_ids is empty")

    monkeypatch.setattr(integration_mod, "load_enrollments_for_device", should_not_run)
    monkeypatch.setattr(integration_mod, "load_membership_windows", should_not_run)

    items = await integration_mod._plan_group(
        group_members=[{"Id": "1"}],
        device_ids=[],
        active_ids={"1"},
        flow_logger=MagicMock(),
    )

    assert items == []
