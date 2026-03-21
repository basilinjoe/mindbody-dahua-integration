"""Test MindBody client token refresh safety margin and lock."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import respx
from httpx import Response

from app.clients.mindbody import _TOKEN_REFRESH_MARGIN, MindBodyClient
from app.config import Settings


def _make_client() -> MindBodyClient:
    settings = Settings(
        mindbody_api_key="key",
        mindbody_site_id="site",
        mindbody_username="user",
        mindbody_password="pass",
        admin_password="x",
        secret_key="x",
    )
    return MindBodyClient(settings=settings)


@pytest.mark.asyncio
@respx.mock
async def test_token_has_safety_margin() -> None:
    """Token expiry should include the safety margin (not full 23 hours)."""
    token_route = respx.post("https://api.mindbodyonline.com/public/v6/usertoken/issue").mock(
        return_value=Response(200, json={"AccessToken": "tok123"})
    )
    client = _make_client()
    try:
        await client._ensure_token()
        assert token_route.called
        # Verify expiry is ~22.5 hours from now (23h minus 30min margin), not full 23h
        expected = datetime.now(UTC) + timedelta(hours=23) - _TOKEN_REFRESH_MARGIN
        assert client._token_expiry is not None
        diff = abs((client._token_expiry - expected).total_seconds())
        assert diff < 5  # within 5 seconds tolerance
    finally:
        await client.close()


@pytest.mark.asyncio
@respx.mock
async def test_token_reused_within_expiry() -> None:
    """Token should be reused if still valid (not expired)."""
    token_route = respx.post("https://api.mindbodyonline.com/public/v6/usertoken/issue").mock(
        return_value=Response(200, json={"AccessToken": "tok456"})
    )
    client = _make_client()
    try:
        await client._ensure_token()
        assert token_route.call_count == 1
        await client._ensure_token()
        assert token_route.call_count == 1  # no second call
    finally:
        await client.close()
