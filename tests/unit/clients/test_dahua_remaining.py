from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.clients.dahua import DahuaClient


@dataclass
class DummyResponse:
    status_code: int
    text: str = ""
    headers: dict[str, str] | None = None
    content: bytes = b""

    def __post_init__(self) -> None:
        if self.headers is None:
            self.headers = {}


@pytest.mark.asyncio
async def test_upload_face_photo_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_post_json(path: str, json_body: dict):
        captured["path"] = path
        captured["body"] = json_body
        return DummyResponse(200, "OK")

    monkeypatch.setattr(client, "_post_json", fake_post_json)
    try:
        ok = await client.upload_face_photo("100", "base64data", user_name="Alice")
    finally:
        await client.close()

    assert ok is True
    assert captured["path"] == "/cgi-bin/FaceInfoManager.cgi?action=add"
    assert captured["body"]["UserID"] == "100"
    assert captured["body"]["PhotoData"] == ["base64data"]


@pytest.mark.asyncio
async def test_upload_face_photo_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_post_json(path: str, json_body: dict):
        return DummyResponse(500, "Error")

    monkeypatch.setattr(client, "_post_json", fake_post_json)
    try:
        ok = await client.upload_face_photo("100", "base64data")
    finally:
        await client.close()

    assert ok is False


@pytest.mark.asyncio
async def test_remove_face_photo_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_get(path: str, params=None):
        captured["path"] = path
        captured["params"] = params
        return DummyResponse(200, "OK")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.remove_face_photo("200")
    finally:
        await client.close()

    assert ok is True
    assert captured["params"]["action"] == "remove"
    assert captured["params"]["UserID"] == "200"


@pytest.mark.asyncio
async def test_remove_face_photo_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get(path: str, params=None):
        return DummyResponse(404, "Not Found")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.remove_face_photo("200")
    finally:
        await client.close()

    assert ok is False


@pytest.mark.asyncio
async def test_open_door(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_get(path: str, params=None):
        captured["path"] = path
        captured["params"] = params
        return DummyResponse(200)

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.open_door(door_id=1)
    finally:
        await client.close()

    assert ok is True
    assert captured["params"]["action"] == "openDoor"
    assert captured["params"]["channel"] == "1"


@pytest.mark.asyncio
async def test_close_door(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_get(path: str, params=None):
        captured["path"] = path
        captured["params"] = params
        return DummyResponse(200)

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.close_door(door_id=0)
    finally:
        await client.close()

    assert ok is True
    assert captured["params"]["action"] == "closeDoor"
    assert captured["params"]["channel"] == "0"


@pytest.mark.asyncio
async def test_open_door_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get(path: str, params=None):
        return DummyResponse(503)

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.open_door()
    finally:
        await client.close()

    assert ok is False


@pytest.mark.asyncio
async def test_health_check_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get(path: str, params=None):
        return DummyResponse(200)

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.health_check()
    finally:
        await client.close()

    assert ok is True


@pytest.mark.asyncio
async def test_health_check_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get(path: str, params=None):
        raise ConnectionError("unreachable")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.health_check()
    finally:
        await client.close()

    assert ok is False


@pytest.mark.asyncio
async def test_add_user_without_validity_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_get(path: str, params=None):
        captured["params"] = params
        return DummyResponse(200, "OK")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.add_user(user_id="50", card_name="Bob", card_no="MB00000050")
    finally:
        await client.close()

    assert ok is True
    assert "ValidFrom" not in captured["params"]
    assert "ValidTo" not in captured["params"]


@pytest.mark.asyncio
async def test_remove_user_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get(path: str, params=None):
        return DummyResponse(200, "OK")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.remove_user("300")
    finally:
        await client.close()

    assert ok is True
