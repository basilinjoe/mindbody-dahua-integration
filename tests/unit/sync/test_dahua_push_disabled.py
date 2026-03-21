from __future__ import annotations

import pytest

from app.sync.flows import dahua_push as dahua_push_mod


class _DummyLogger:
    def info(self, *args, **kwargs) -> None:
        pass

    def warning(self, *args, **kwargs) -> None:
        pass


@pytest.mark.asyncio
async def test_dahua_push_disabled_skips_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """When dahua_push_enabled=false, all items are skipped."""
    from types import SimpleNamespace

    items = [SimpleNamespace(id=1, action="enroll")]

    async def fake_load_pending_queue_items(run_id):
        return items

    async def fake_variable_aget(name, default=None):
        if name == "dahua_push_enabled":
            return "false"
        return default

    async def fake_create_table_artifact(**kwargs):
        pass

    monkeypatch.setattr(dahua_push_mod, "load_pending_queue_items", fake_load_pending_queue_items)
    monkeypatch.setattr("app.sync.flows.dahua_push.Variable.aget", staticmethod(fake_variable_aget))
    monkeypatch.setattr(dahua_push_mod, "create_table_artifact", fake_create_table_artifact)

    stats = await dahua_push_mod.run_dahua_push("run-1", _DummyLogger())

    assert stats["skipped"] == 1
    assert stats["enrolled"] == 0


@pytest.mark.asyncio
async def test_dahua_push_empty_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """When no pending items, return zeros."""

    async def fake_load_pending_queue_items(run_id):
        return []

    async def fake_variable_aget(name, default=None):
        if name == "dahua_push_enabled":
            return "true"
        return default

    async def fake_create_table_artifact(**kwargs):
        pass

    monkeypatch.setattr(dahua_push_mod, "load_pending_queue_items", fake_load_pending_queue_items)
    monkeypatch.setattr("app.sync.flows.dahua_push.Variable.aget", staticmethod(fake_variable_aget))
    monkeypatch.setattr(dahua_push_mod, "create_table_artifact", fake_create_table_artifact)

    stats = await dahua_push_mod.run_dahua_push("run-1", _DummyLogger())

    assert stats["enrolled"] == 0
    assert stats["failed"] == 0
    assert "skipped" not in stats


@pytest.mark.asyncio
async def test_dahua_push_reactivate_action(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test the reactivate action path."""
    from types import SimpleNamespace

    items = [
        SimpleNamespace(
            id=1,
            action="reactivate",
            device_id=10,
            member_snapshot=None,
            dahua_user_id="100",
            enrollment_id=None,
            mindbody_client_id="100",
        ),
    ]
    marks: list = []

    async def fake_load_pending_queue_items(run_id):
        return items

    async def fake_variable_aget(name, default=None):
        return "true" if name == "dahua_push_enabled" else default

    async def fake_reactivate_on_device(device_id, dahua_user_id):
        return True

    async def fake_mark_queue_item(item_id, status, error_message=None):
        marks.append((item_id, status))

    async def fake_create_table_artifact(**kwargs):
        pass

    monkeypatch.setattr(dahua_push_mod, "load_pending_queue_items", fake_load_pending_queue_items)
    monkeypatch.setattr("app.sync.flows.dahua_push.Variable.aget", staticmethod(fake_variable_aget))
    monkeypatch.setattr(dahua_push_mod, "reactivate_on_device", fake_reactivate_on_device)
    monkeypatch.setattr(dahua_push_mod, "mark_queue_item", fake_mark_queue_item)
    monkeypatch.setattr(dahua_push_mod, "create_table_artifact", fake_create_table_artifact)

    stats = await dahua_push_mod.run_dahua_push("run-1", _DummyLogger())

    assert stats["reactivated"] == 1
    assert marks[0] == (1, "success")
