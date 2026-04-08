from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.sync.flows import dahua_push as dahua_push_mod


class _DummyLogger:
    def info(self, *_args, **_kwargs) -> None:  # noqa: ANN002, ANN003
        return None


@pytest.mark.asyncio
async def test_sync_dahua_push_flow_handles_update_and_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items = [
        SimpleNamespace(
            id=1,
            action="enroll",
            device_id=10,
            member_snapshot=json.dumps({"Id": "101", "FirstName": "Alex"}),
            dahua_user_id=None,
            enrollment_id=None,
            mindbody_client_id="101",
        ),
        SimpleNamespace(
            id=2,
            action="update",
            device_id=10,
            member_snapshot=json.dumps(
                {
                    "card_name": "Alex One",
                    "valid_start": "2026-01-01 00:00:00",
                    "valid_end": "2026-12-31 23:59:59",
                }
            ),
            dahua_user_id="101",
            enrollment_id=55,
            mindbody_client_id="101",
        ),
        SimpleNamespace(
            id=3,
            action="deactivate",
            device_id=10,
            member_snapshot=None,
            dahua_user_id="102",
            enrollment_id=77,
            mindbody_client_id="102",
        ),
        SimpleNamespace(
            id=4,
            action="bad-action",
            device_id=10,
            member_snapshot=None,
            dahua_user_id="103",
            enrollment_id=88,
            mindbody_client_id="103",
        ),
    ]
    marks: list[tuple[int, str, str | None]] = []
    called_update: dict[str, str | int | None] = {}

    async def fake_load_pending_queue_items(run_id: str):  # noqa: ANN202
        assert run_id == "run-1"
        return items

    async def fake_enroll_on_device(device_id: int, member: dict) -> bool:
        assert device_id == 10
        assert member["Id"] == "101"
        return True

    async def fake_deactivate_on_device(device_id: int, dahua_user_id: str) -> bool:  # noqa: ANN202
        assert device_id == 10
        assert dahua_user_id == "102"
        return False

    async def fake_reactivate_on_device(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("reactivate should not be called")

    async def fake_update_on_device(  # noqa: ANN202
        device_id: int,
        dahua_user_id: str,
        card_name: str | None = None,
        valid_start: str | None = None,
        valid_end: str | None = None,
    ):
        called_update.update(
            {
                "device_id": device_id,
                "dahua_user_id": dahua_user_id,
                "card_name": card_name,
                "valid_start": valid_start,
                "valid_end": valid_end,
            }
        )
        return True

    async def fake_mark_queue_item(
        item_id: int, status: str, error_message: str | None = None
    ) -> None:
        marks.append((item_id, status, error_message))

    async def fake_create_table_artifact(**_kwargs) -> None:  # noqa: ANN003
        return None

    async def fake_variable_aget(name: str, default=None):  # noqa: ANN001, ANN202
        if name == "dahua_push_enabled":
            return "true"
        return default

    monkeypatch.setattr(dahua_push_mod, "get_run_logger", lambda: _DummyLogger())
    monkeypatch.setattr("app.sync.flows.dahua_push.Variable.aget", staticmethod(fake_variable_aget))
    monkeypatch.setattr(dahua_push_mod, "load_pending_queue_items", fake_load_pending_queue_items)
    monkeypatch.setattr(dahua_push_mod, "enroll_on_device", fake_enroll_on_device)
    monkeypatch.setattr(dahua_push_mod, "deactivate_on_device", fake_deactivate_on_device)
    monkeypatch.setattr(dahua_push_mod, "reactivate_on_device", fake_reactivate_on_device)
    monkeypatch.setattr(dahua_push_mod, "update_on_device", fake_update_on_device)
    monkeypatch.setattr(dahua_push_mod, "mark_queue_item", fake_mark_queue_item)
    monkeypatch.setattr(dahua_push_mod, "create_table_artifact", fake_create_table_artifact)

    stats = await dahua_push_mod.sync_dahua_push_flow.fn(run_id="run-1")

    assert stats == {
        "enrolled": 1,
        "deactivated": 0,
        "reactivated": 0,
        "updated": 1,
        "failed": 2,
    }
    assert called_update == {
        "device_id": 10,
        "dahua_user_id": "101",
        "card_name": "Alex One",
        "valid_start": "2026-01-01 00:00:00",
        "valid_end": "2026-12-31 23:59:59",
    }

    by_id = {item_id: (status, error) for item_id, status, error in marks}
    assert by_id[1] == ("success", None)
    assert by_id[2] == ("success", None)
    assert by_id[3] == ("failed", "Device returned failure")
    assert by_id[4][0] == "failed"
    assert "Unknown action" in (by_id[4][1] or "")
