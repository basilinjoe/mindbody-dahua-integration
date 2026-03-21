from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.sync.flows import health as health_mod


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append(args)

    def warning(self, *args, **kwargs) -> None:
        self.messages.append(args)


@pytest.mark.asyncio
async def test_device_health_flow_online(monkeypatch: pytest.MonkeyPatch) -> None:
    devices = [
        SimpleNamespace(id=1, name="Gate1", host="10.0.0.1"),
        SimpleNamespace(id=2, name="Gate2", host="10.0.0.2"),
    ]
    statuses_updated: list[tuple] = []

    async def fake_load_all_devices():
        return devices

    async def fake_check_device_health_task(device_id: int) -> bool:
        return device_id == 1

    async def fake_update_status(db, device_id, status):
        statuses_updated.append((device_id, status))

    class FakeDB:
        async def commit(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class FakeSessionFactory:
        def __call__(self):
            return FakeDB()

    def fake_get_factory():
        return FakeSessionFactory()

    async def fake_create_markdown_artifact(**kwargs) -> None:
        pass

    monkeypatch.setattr(health_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(health_mod, "load_all_devices", fake_load_all_devices)
    monkeypatch.setattr(health_mod, "check_device_health_task", fake_check_device_health_task)
    monkeypatch.setattr(health_mod, "_get_async_session_factory", fake_get_factory)
    monkeypatch.setattr(
        health_mod, "devices_svc", SimpleNamespace(update_status=fake_update_status)
    )
    monkeypatch.setattr(health_mod, "create_markdown_artifact", fake_create_markdown_artifact)

    await health_mod.device_health_flow.fn()

    assert (1, "online") in statuses_updated
    assert (2, "offline") in statuses_updated


@pytest.mark.asyncio
async def test_device_health_flow_error(monkeypatch: pytest.MonkeyPatch) -> None:
    devices = [SimpleNamespace(id=1, name="Gate1", host="10.0.0.1")]
    statuses_updated: list[tuple] = []

    async def fake_load_all_devices():
        return devices

    async def fake_check_device_health_task(device_id: int) -> bool:
        raise ConnectionError("unreachable")

    async def fake_update_status(db, device_id, status):
        statuses_updated.append((device_id, status))

    class FakeDB:
        async def commit(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class FakeSessionFactory:
        def __call__(self):
            return FakeDB()

    def fake_get_factory():
        return FakeSessionFactory()

    async def fake_create_markdown_artifact(**kwargs) -> None:
        pass

    monkeypatch.setattr(health_mod, "get_run_logger", _DummyLogger)
    monkeypatch.setattr(health_mod, "load_all_devices", fake_load_all_devices)
    monkeypatch.setattr(health_mod, "check_device_health_task", fake_check_device_health_task)
    monkeypatch.setattr(health_mod, "_get_async_session_factory", fake_get_factory)
    monkeypatch.setattr(
        health_mod, "devices_svc", SimpleNamespace(update_status=fake_update_status)
    )
    monkeypatch.setattr(health_mod, "create_markdown_artifact", fake_create_markdown_artifact)

    await health_mod.device_health_flow.fn()

    assert (1, "error") in statuses_updated
