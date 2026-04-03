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
async def test_door_params_parses_multiple_doors() -> None:
    client = DahuaClient("127.0.0.1", door_ids="0, 2,3")
    try:
        assert client._door_params() == {"Doors[0]": "0", "Doors[1]": "2", "Doors[2]": "3"}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_add_user_builds_expected_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1", door_ids="0,1")
    captured: dict = {}

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        captured["path"] = path
        captured["params"] = params
        return DummyResponse(200, "OK")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.add_user(
            user_id="100",
            card_name="Jane Doe",
            card_no="MB00000100",
            card_status=0,
            card_type=0,
            valid_start="2026-01-01 00:00:00",
            valid_end="2026-12-31 23:59:59",
        )
    finally:
        await client.close()

    assert ok is True
    assert captured["path"] == "/cgi-bin/recordUpdater.cgi"
    assert captured["params"]["action"] == "insert"
    assert captured["params"]["UserID"] == "100"
    assert captured["params"]["Doors[0]"] == "0"
    assert captured["params"]["Doors[1]"] == "1"
    assert captured["params"]["ValidDateStart"] == "2026-01-01 00:00:00"
    assert captured["params"]["ValidDateEnd"] == "2026-12-31 23:59:59"


@pytest.mark.asyncio
async def test_add_user_succeeds_with_recno_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dahua devices return 'RecNo=<N>' (not 'OK') on successful insert."""
    client = DahuaClient("127.0.0.1", door_ids="0")

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        return DummyResponse(200, "RecNo=349")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.add_user(
            user_id="29963401172",
            card_name="Test Member",
            card_no="MB29963401",
        )
    finally:
        await client.close()

    assert ok is True


@pytest.mark.asyncio
async def test_update_and_remove_user_status_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    calls: list[dict] = []

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        calls.append({"path": path, "params": params})
        if params["action"] == "update":
            return DummyResponse(200, "OK")
        return DummyResponse(500, "ERR")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        assert await client.update_user_status("200", 4) is True
        assert await client.remove_user("200") is False
    finally:
        await client.close()

    assert calls[0]["params"]["action"] == "update"
    assert calls[0]["params"]["CardStatus"] == "4"
    assert calls[1]["params"]["action"] == "remove"


@pytest.mark.asyncio
async def test_update_user_validity_builds_expected_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        captured["path"] = path
        captured["params"] = params
        return DummyResponse(200, "OK")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.update_user_validity(
            user_id="300",
            valid_start="2026-01-01 00:00:00",
            valid_end="2026-12-31 23:59:59",
        )
    finally:
        await client.close()

    assert ok is True
    assert captured["path"] == "/cgi-bin/recordUpdater.cgi"
    assert captured["params"]["action"] == "update"
    assert captured["params"]["name"] == "AccessControlCard"
    assert captured["params"]["UserID"] == "300"
    assert captured["params"]["ValidDateStart"] == "2026-01-01 00:00:00"
    assert captured["params"]["ValidDateEnd"] == "2026-12-31 23:59:59"


@pytest.mark.asyncio
async def test_update_user_validity_omits_empty_dates_and_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DahuaClient("127.0.0.1")
    captured: dict = {}

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        captured["path"] = path
        captured["params"] = params
        return DummyResponse(500, "ERR")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        ok = await client.update_user_validity(user_id="301", valid_start=None, valid_end=None)
    finally:
        await client.close()

    assert ok is False
    assert captured["path"] == "/cgi-bin/recordUpdater.cgi"
    assert "ValidDateStart" not in captured["params"]
    assert "ValidDateEnd" not in captured["params"]


@pytest.mark.asyncio
async def test_parse_record_finder_response_and_get_all_users(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DahuaClient("127.0.0.1")
    payload = (
        "records[0].UserID=1\n"
        "records[0].CardName=Alice\n"
        "records[0].CardNo=MB00000001\n"
        "records[1].UserID=2\n"
        "records[1].CardName=Bob\n"
    )

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        return DummyResponse(200, payload)

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        users = await client.get_all_users()
    finally:
        await client.close()

    assert len(users) == 2
    assert users[0]["UserID"] == "1"
    assert users[1]["CardName"] == "Bob"


@pytest.mark.asyncio
async def test_get_all_users_handles_non_200(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get(path: str, params=None):  # noqa: ANN001, ANN202
        return DummyResponse(503, "Service unavailable")

    monkeypatch.setattr(client, "_get", fake_get)
    try:
        users = await client.get_all_users()
    finally:
        await client.close()

    assert users == []


@pytest.mark.asyncio
async def test_get_user_match_and_no_match(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DahuaClient("127.0.0.1")

    async def fake_get_all_users():  # noqa: ANN202
        return [{"UserID": "11", "CardName": "A"}, {"UserID": "12", "CardName": "B"}]

    monkeypatch.setattr(client, "get_all_users", fake_get_all_users)
    try:
        assert (await client.get_user("12")) == {"UserID": "12", "CardName": "B"}
        assert await client.get_user("99") is None
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_capture_snapshot_returns_bytes_only_for_images(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DahuaClient("127.0.0.1")
    responses = [
        DummyResponse(200, headers={"content-type": "image/jpeg"}, content=b"img-bytes"),
        DummyResponse(200, headers={"content-type": "application/json"}, content=b"{}"),
    ]

    async def fake_http_get(url: str, params=None):  # noqa: ANN001, ANN202
        return responses.pop(0)

    monkeypatch.setattr(client._http, "get", fake_http_get)
    try:
        assert await client.capture_snapshot() == b"img-bytes"
        assert await client.capture_snapshot() is None
    finally:
        await client.close()
