from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import respx
from httpx import Response

from app.clients.mindbody import MindBodyClient
from app.config import Settings


def _make_settings() -> Settings:
    return Settings(
        mindbody_api_key="key",
        mindbody_site_id="site",
        mindbody_api_base_url="https://mb.test/public/v6",
        mindbody_username="user",
        mindbody_password="pass",
        secret_key="sk",
        admin_password="ap",
    )


@pytest.mark.asyncio
@respx.mock
async def test_get_all_clients_pagination() -> None:
    """get_all_clients should paginate and stop when a page is smaller than page_size."""
    client = MindBodyClient(settings=_make_settings())
    # Pre-set token to skip auth
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    page1 = [{"Id": str(i)} for i in range(200)]
    page2 = [{"Id": str(i)} for i in range(200, 350)]

    call_count = 0

    def handle_clients(request):
        nonlocal call_count
        offset = int(request.url.params.get("request.offset", "0"))
        call_count += 1
        if offset == 0:
            return Response(200, json={"Clients": page1})
        else:
            return Response(200, json={"Clients": page2})

    respx.get("https://mb.test/public/v6/client/clients").mock(side_effect=handle_clients)

    try:
        result = await client.get_all_clients()
    finally:
        await client.close()

    assert len(result) == 350
    assert call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_get_all_clients_with_modified_since() -> None:
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    respx.get("https://mb.test/public/v6/client/clients").mock(
        return_value=Response(200, json={"Clients": [{"Id": "1"}]})
    )

    try:
        result = await client.get_all_clients(modified_since=datetime(2026, 1, 1))
    finally:
        await client.close()

    assert len(result) == 1


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_with_active_membership() -> None:
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    future_date = (datetime.now(UTC) + timedelta(days=30)).isoformat()
    respx.get("https://mb.test/public/v6/client/activeclientmemberships").mock(
        return_value=Response(
            200, json={"ClientMemberships": [{"ExpirationDate": future_date}]}
        )
    )

    try:
        result = await client.is_member_active("100")
    finally:
        await client.close()

    assert result is True


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_no_expiry() -> None:
    """A membership with no ExpirationDate is considered active (ongoing)."""
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    respx.get("https://mb.test/public/v6/client/activeclientmemberships").mock(
        return_value=Response(
            200, json={"ClientMemberships": [{"ExpirationDate": None}]}
        )
    )

    try:
        result = await client.is_member_active("100")
    finally:
        await client.close()

    assert result is True


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_expired_membership_falls_back_to_contracts() -> None:
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    past_date = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    future_date = (datetime.now(UTC) + timedelta(days=30)).isoformat()

    respx.get("https://mb.test/public/v6/client/activeclientmemberships").mock(
        return_value=Response(
            200, json={"ClientMemberships": [{"ExpirationDate": past_date}]}
        )
    )
    respx.get("https://mb.test/public/v6/client/clientcontracts").mock(
        return_value=Response(
            200, json={"Contracts": [{"EndDate": future_date}]}
        )
    )

    try:
        result = await client.is_member_active("100")
    finally:
        await client.close()

    assert result is True


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_no_membership_no_contract() -> None:
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    past = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    respx.get("https://mb.test/public/v6/client/activeclientmemberships").mock(
        return_value=Response(
            200, json={"ClientMemberships": [{"ExpirationDate": past}]}
        )
    )
    respx.get("https://mb.test/public/v6/client/clientcontracts").mock(
        return_value=Response(200, json={"Contracts": [{"EndDate": past}]})
    )

    try:
        result = await client.is_member_active("100")
    finally:
        await client.close()

    assert result is False


@pytest.mark.asyncio
@respx.mock
async def test_is_member_active_with_bad_date_in_membership() -> None:
    """Malformed dates should be skipped, not raise."""
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    respx.get("https://mb.test/public/v6/client/activeclientmemberships").mock(
        return_value=Response(
            200, json={"ClientMemberships": [{"ExpirationDate": "not-a-date"}]}
        )
    )
    respx.get("https://mb.test/public/v6/client/clientcontracts").mock(
        return_value=Response(200, json={"Contracts": []})
    )

    try:
        result = await client.is_member_active("100")
    finally:
        await client.close()

    assert result is False


@pytest.mark.asyncio
@respx.mock
async def test_get_client_contracts() -> None:
    client = MindBodyClient(settings=_make_settings())
    client._token = "tok"
    client._token_expiry = datetime.now(UTC) + timedelta(hours=1)

    respx.get("https://mb.test/public/v6/client/clientcontracts").mock(
        return_value=Response(200, json={"Contracts": [{"Id": "c1"}]})
    )

    try:
        result = await client.get_client_contracts("100")
    finally:
        await client.close()

    assert result == [{"Id": "c1"}]
