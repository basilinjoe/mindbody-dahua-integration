from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from app.clients.mindbody import MindBodyClient
from app.config import Settings


def _settings() -> Settings:
    return Settings(
        mindbody_api_key="api-key",
        mindbody_site_id="site-id",
        mindbody_api_base_url="https://api.mindbodyonline.com/public/v6",
        mindbody_username="user",
        mindbody_password="pass",
        secret_key="test",
        admin_password="test",
    )


@pytest.mark.asyncio
@respx.mock
async def test_token_is_reused_before_expiry() -> None:
    settings = _settings()
    base = settings.mindbody_api_base_url.rstrip("/")
    token_route = respx.post(f"{base}/usertoken/issue").respond(200, json={"AccessToken": "abc123"})
    clients_route = respx.get(f"{base}/client/clients").respond(200, json={"Clients": []})
    mb = MindBodyClient(settings)
    try:
        await mb.get_clients()
        await mb.get_clients()
    finally:
        await mb.close()

    assert token_route.call_count == 1
    assert clients_route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_get_clients_includes_search_text_and_headers() -> None:
    settings = _settings()
    base = settings.mindbody_api_base_url.rstrip("/")
    respx.post(f"{base}/usertoken/issue").respond(200, json={"AccessToken": "token-1"})

    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers.get("Authorization", "")
        captured["search"] = request.url.params.get("request.searchText", "")
        return httpx.Response(200, json={"Clients": [{"Id": 10}]})

    respx.get(f"{base}/client/clients").mock(side_effect=handler)
    mb = MindBodyClient(settings)
    try:
        result = await mb.get_clients(search_text="john", limit=10, offset=20)
    finally:
        await mb.close()

    assert result == [{"Id": 10}]
    assert captured["auth"] == "Bearer token-1"
    assert captured["search"] == "john"


@pytest.mark.asyncio
@respx.mock
async def test_get_all_clients_paginates_until_short_page() -> None:
    settings = _settings()
    base = settings.mindbody_api_base_url.rstrip("/")
    respx.post(f"{base}/usertoken/issue").respond(200, json={"AccessToken": "token-2"})

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("request.offset", "0"))
        if offset == 0:
            return httpx.Response(200, json={"Clients": [{"Id": 1}, {"Id": 2}]})
        return httpx.Response(200, json={"Clients": []})

    respx.get(f"{base}/client/clients").mock(side_effect=handler)
    mb = MindBodyClient(settings)
    try:
        result = await mb.get_all_clients()
    finally:
        await mb.close()

    assert result == [{"Id": 1}, {"Id": 2}]


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_true_for_unexpired_membership() -> None:
    settings = _settings()
    base = settings.mindbody_api_base_url.rstrip("/")
    future = (datetime.now(UTC) + timedelta(days=10)).isoformat()

    respx.post(f"{base}/usertoken/issue").respond(200, json={"AccessToken": "token-3"})
    respx.get(f"{base}/client/activeclientmemberships").respond(
        200, json={"ClientMemberships": [{"ExpirationDate": future}]}
    )
    mb = MindBodyClient(settings)
    try:
        assert await mb.is_member_active("1") is True
    finally:
        await mb.close()


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_uses_contract_fallback() -> None:
    settings = _settings()
    base = settings.mindbody_api_base_url.rstrip("/")
    past = (datetime.now(UTC) - timedelta(days=5)).isoformat()
    future = (datetime.now(UTC) + timedelta(days=5)).isoformat()

    respx.post(f"{base}/usertoken/issue").respond(200, json={"AccessToken": "token-4"})
    respx.get(f"{base}/client/activeclientmemberships").respond(
        200, json={"ClientMemberships": [{"ExpirationDate": past}]}
    )
    respx.get(f"{base}/client/clientcontracts").respond(
        200, json={"Contracts": [{"EndDate": future}]}
    )
    mb = MindBodyClient(settings)
    try:
        assert await mb.is_member_active("2") is True
    finally:
        await mb.close()


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_false_for_invalid_dates_and_no_entitlements() -> None:
    settings = _settings()
    base = settings.mindbody_api_base_url.rstrip("/")

    respx.post(f"{base}/usertoken/issue").respond(200, json={"AccessToken": "token-5"})
    respx.get(f"{base}/client/activeclientmemberships").respond(
        200, json={"ClientMemberships": [{"ExpirationDate": "not-a-date"}]}
    )
    respx.get(f"{base}/client/clientcontracts").respond(
        200, json={"Contracts": [{"EndDate": "still-not-a-date"}]}
    )
    mb = MindBodyClient(settings)
    try:
        assert await mb.is_member_active("3") is False
    finally:
        await mb.close()
