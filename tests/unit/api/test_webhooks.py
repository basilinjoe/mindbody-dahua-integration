from __future__ import annotations

import hashlib
import hmac
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.webhooks import router as webhooks_router
from app.config import Settings


def _signature(body: bytes, key: str) -> str:
    return hmac.new(key.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _build_app(signature_key: str = "sig-key") -> FastAPI:
    app = FastAPI()
    app.include_router(webhooks_router)
    app.state.settings = Settings(
        secret_key="x",
        admin_password="x",
        mindbody_webhook_signature_key=signature_key,
    )
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


def test_webhook_no_signature_key_returns_401() -> None:
    """With no key configured the endpoint must reject all requests (fail closed)."""
    app = _build_app(signature_key="")
    body = b'{"eventId":"client.updated","eventData":{"clientId":"1"}}'
    with TestClient(app) as client:
        resp = client.post(
            "/webhooks/mindbody",
            content=body,
            headers={"X-Mindbody-Signature": "anything"},
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


def test_webhook_valid_event_returns_200() -> None:
    app = _build_app(signature_key="secret")
    payload = {"eventId": "client.updated", "eventData": {"clientId": "12345"}}
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
