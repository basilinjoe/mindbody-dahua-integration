from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import health as health_module

_TEST_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost/test_sync"
)


class DummySyncEngine:
    def get_dahua_clients(self) -> list:
        return []


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health_module.router)
    app.state.db_session_factory = lambda: None
    app.state.sync_engine = DummySyncEngine()
    return app


def test_health_endpoint_returns_ok() -> None:
    app = _build_app()
    with TestClient(app) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readiness_ready_when_all_checks_pass(monkeypatch) -> None:
    app = _build_app()

    async def fake_dahua(_engine) -> bool:
        return True

    monkeypatch.setattr(health_module, "_check_database", lambda _factory: True)
    monkeypatch.setattr(health_module, "_check_dahua_devices", fake_dahua)

    with TestClient(app) as client:
        resp = client.get("/health/ready")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ready", "checks": {"database": True, "dahua_devices": True}}


def test_readiness_not_ready_when_database_fails(monkeypatch) -> None:
    app = _build_app()

    async def fake_dahua(_engine) -> bool:
        return True

    monkeypatch.setattr(health_module, "_check_database", lambda _factory: False)
    monkeypatch.setattr(health_module, "_check_dahua_devices", fake_dahua)

    with TestClient(app) as client:
        resp = client.get("/health/ready")

    assert resp.status_code == 200
    assert resp.json() == {
        "status": "not_ready",
        "checks": {"database": False, "dahua_devices": True},
    }


def test_readiness_not_ready_when_dahua_fails(monkeypatch) -> None:
    app = _build_app()

    async def fake_dahua(_engine) -> bool:
        return False

    monkeypatch.setattr(health_module, "_check_database", lambda _factory: True)
    monkeypatch.setattr(health_module, "_check_dahua_devices", fake_dahua)

    with TestClient(app) as client:
        resp = client.get("/health/ready")

    assert resp.status_code == 200
    assert resp.json() == {
        "status": "not_ready",
        "checks": {"database": True, "dahua_devices": False},
    }


def test_check_database_helper_true_and_false() -> None:
    engine = create_engine(_TEST_DB_URL)
    session_local = sessionmaker(bind=engine)
    try:
        assert health_module._check_database(session_local) is True
        assert health_module._check_database(lambda: None) is False
    finally:
        engine.dispose()


class HealthyClient:
    async def health_check(self) -> bool:
        return True


class BrokenClient:
    async def health_check(self) -> bool:
        raise RuntimeError("boom")


class EngineWithClients:
    def __init__(self, clients) -> None:
        self._clients = clients

    def get_dahua_clients(self) -> list:
        return self._clients


async def test_check_dahua_devices_helper_branches() -> None:
    assert await health_module._check_dahua_devices(EngineWithClients([])) is False
    assert await health_module._check_dahua_devices(EngineWithClients([HealthyClient()])) is True
    assert await health_module._check_dahua_devices(EngineWithClients([BrokenClient()])) is False
