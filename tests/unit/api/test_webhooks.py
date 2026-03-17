from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.webhooks import router as webhooks_router
from app.config import Settings


class SyncSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def sync_single_member(self, client_id: str, sync_type: str) -> None:
        self.calls.append((client_id, sync_type))


def _signature(body: bytes, key: str) -> str:
    return hmac.new(key.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _build_app(signature_key: str = "sig-key") -> FastAPI:
    app = FastAPI()
    app.include_router(webhooks_router)
    app.state.settings = Settings(secret_key="x", mindbody_webhook_signature_key=signature_key)
    app.state.sync_engine = SyncSpy()
    return app


def test_webhook_head_validation_returns_200() -> None:
    app = _build_app()
    with TestClient(app) as client:
        resp = client.head("/webhooks/mindbody")
    assert resp.status_code == 200


def test_webhook_invalid_signature_returns_401() -> None:
    app = _build_app(signature_key="secret")
    body = b'{"eventId":"client.updated","eventData":{"clientId":"1"}}'
    with TestClient(app) as client:
        resp = client.post(
            "/webhooks/mindbody", content=body, headers={"X-Mindbody-Signature": "bad"}
        )
    assert resp.status_code == 401


def test_webhook_invalid_json_returns_400() -> None:
    app = _build_app(signature_key="secret")
    body = b"not-json"
    with TestClient(app) as client:
        resp = client.post(
            "/webhooks/mindbody",
            content=body,
            headers={
                "X-Mindbody-Signature": _signature(body, "secret"),
                "Content-Type": "application/json",
            },
        )
    assert resp.status_code == 400


def test_webhook_relevant_event_triggers_sync() -> None:
    app = _build_app(signature_key="secret")
    payload = {"eventId": "client.updated", "eventData": {"clientId": "12345"}}
    body = json.dumps(payload).encode("utf-8")

    with patch("app.api.webhooks.run_deployment", new_callable=AsyncMock) as mock_deploy:
        with TestClient(app) as client:
            resp = client.post(
                "/webhooks/mindbody",
                content=body,
                headers={
                    "X-Mindbody-Signature": _signature(body, "secret"),
                    "Content-Type": "application/json",
                },
            )

    assert resp.status_code == 200
    mock_deploy.assert_called_once_with(
        "sync-member/default",
        parameters={"client_id": "12345", "sync_type": "webhook"},
        timeout=0,
    )


def test_webhook_irrelevant_event_does_not_trigger_sync() -> None:
    app = _build_app(signature_key="secret")
    payload = {"eventId": "staff.updated", "eventData": {"clientId": "12345"}}
    body = json.dumps(payload).encode("utf-8")

    with TestClient(app) as client:
        resp = client.post(
            "/webhooks/mindbody",
            content=body,
            headers={
                "X-Mindbody-Signature": _signature(body, "secret"),
                "Content-Type": "application/json",
            },
        )

    assert resp.status_code == 200
    assert app.state.sync_engine.calls == []
