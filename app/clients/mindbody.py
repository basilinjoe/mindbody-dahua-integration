from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import httpx

from app.config import Settings
from app.utils.retry import mindbody_retry

logger = logging.getLogger(__name__)

MINDBODY_PAGE_SIZE = 200  # MindBody API max items per page / bulk request

# Safety margin before token expiry to avoid using a token that expires mid-request
_TOKEN_REFRESH_MARGIN = timedelta(minutes=30)


class MindBodyClient:
    """MindBody Public API v6 client with automatic token management."""

    def __init__(self, settings: Settings) -> None:
        self._base = settings.mindbody_api_base_url.rstrip("/")
        self._api_key = settings.mindbody_api_key
        self._site_id = settings.mindbody_site_id
        self._username = settings.mindbody_username
        self._password = settings.mindbody_password
        self._token: str | None = None
        self._token_expiry: datetime | None = None
        self._token_lock = asyncio.Lock()
        self._http = httpx.AsyncClient(timeout=30)

    async def close(self) -> None:
        await self._http.aclose()

    # ---- Auth ---------------------------------------------------------------

    async def _ensure_token(self) -> str:
        if self._token and self._token_expiry and self._token_expiry > datetime.now(UTC):
            return self._token
        async with self._token_lock:
            # Double-check after acquiring lock (another coroutine may have refreshed)
            if self._token and self._token_expiry and self._token_expiry > datetime.now(UTC):
                return self._token
            logger.info("Requesting new MindBody user token")
            resp = await self._http.post(
                f"{self._base}/usertoken/issue",
                headers={"Api-Key": self._api_key, "SiteId": self._site_id},
                json={"Username": self._username, "Password": self._password},
            )
            resp.raise_for_status()
            data = resp.json()
            self._token = data["AccessToken"]
            # MindBody tokens are valid ~24h; refresh early to avoid mid-request expiry
            self._token_expiry = datetime.now(UTC) + timedelta(hours=23) - _TOKEN_REFRESH_MARGIN
            return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Api-Key": self._api_key,
            "SiteId": self._site_id,
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    # ---- Clients ------------------------------------------------------------

    @mindbody_retry
    async def get_clients(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        search_text: str = "",
        last_modified_date: datetime | None = None,
    ) -> list[dict]:
        """Get a page of clients."""
        await self._ensure_token()
        params: dict = {"request.limit": limit, "request.offset": offset}
        if search_text:
            params["request.searchText"] = search_text
        if last_modified_date:
            params["request.lastModifiedDate"] = last_modified_date.strftime("%Y-%m-%dT%H:%M:%S")
        resp = await self._http.get(
            f"{self._base}/client/clients", headers=self._headers(), params=params
        )
        resp.raise_for_status()
        return resp.json().get("Clients", [])

    async def get_all_clients(
        self,
        *,
        modified_since: datetime | None = None,
    ) -> list[dict]:
        """Auto-paginate through all clients, optionally filtered by last modified date."""
        all_clients: list[dict] = []
        offset = 0
        page_size = MINDBODY_PAGE_SIZE
        while True:
            page = await self.get_clients(
                limit=page_size,
                offset=offset,
                last_modified_date=modified_since,
            )
            if not page:
                break
            all_clients.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        logger.info(
            "Fetched %d clients from MindBody%s",
            len(all_clients),
            f" modified since {modified_since.isoformat()}" if modified_since else "",
        )
        return all_clients

    # ---- Memberships / Contracts -------------------------------------------

    @mindbody_retry
    async def get_active_memberships(self, client_id: str) -> list[dict]:
        await self._ensure_token()
        resp = await self._http.get(
            f"{self._base}/client/activeclientmemberships",
            headers=self._headers(),
            params={"request.clientId": client_id},
        )
        resp.raise_for_status()
        return resp.json().get("ClientMemberships", [])

    @mindbody_retry
    async def get_active_memberships_bulk(self, client_ids: list[str]) -> dict[str, list[dict]]:
        """Fetch active memberships for up to 200 clients in a single API call.

        Returns a dict keyed by client_id → list of membership dicts.
        Uses /activeclientsmemberships (plural) bulk endpoint.
        """
        await self._ensure_token()
        params = [("request.clientIds", cid) for cid in client_ids]
        params.append(("request.limit", str(len(client_ids))))
        resp = await self._http.get(
            f"{self._base}/client/activeclientsmemberships",
            headers=self._headers(),
            params=params,
        )
        resp.raise_for_status()
        result: dict[str, list[dict]] = {}
        raw_entries = resp.json().get("ClientMemberships", [])
        for entry in raw_entries:
            cid = entry.get("ClientId", "")
            result[cid] = entry.get("Memberships", [])
        # Log sample membership fields for debugging data-shape issues
        if raw_entries:
            sample = raw_entries[0].get("Memberships", [{}])
            if sample:
                logger.info(
                    "Membership sample keys: %s, sample: %s",
                    list(sample[0].keys()),
                    {k: sample[0][k] for k in list(sample[0].keys())[:8]},
                )
        return result

    @mindbody_retry
    async def get_client_contracts(self, client_id: str) -> list[dict]:
        await self._ensure_token()
        resp = await self._http.get(
            f"{self._base}/client/clientcontracts",
            headers=self._headers(),
            params={"request.clientId": client_id},
        )
        resp.raise_for_status()
        return resp.json().get("Contracts", [])

    # ---- Composite helpers --------------------------------------------------

    async def is_member_active(self, client_id: str) -> bool:
        """Check if a client has any active membership or contract."""
        memberships = await self.get_active_memberships(client_id)
        now = datetime.now(UTC)
        for m in memberships:
            exp = m.get("ExpirationDate")
            if exp is None:
                return True  # ongoing / no expiry
            try:
                exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                if exp_dt > now:
                    return True
            except (ValueError, TypeError):
                continue

        # Fallback: check contracts
        contracts = await self.get_client_contracts(client_id)
        for c in contracts:
            end_date = c.get("EndDate")
            if end_date is None:
                return True
            try:
                end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                if end_dt > now:
                    return True
            except (ValueError, TypeError):
                continue

        return False
