from __future__ import annotations

from typing import Any


class FakeMindBodyClient:
    def __init__(
        self,
        *,
        all_clients: list[dict] | None = None,
        active_map: dict[str, bool] | None = None,
        search_results: dict[str, list[dict]] | None = None,
        memberships_map: dict[str, list[dict]] | None = None,
        contracts_map: dict[str, list[dict]] | None = None,
    ) -> None:
        self.all_clients = all_clients or []
        self.active_map = active_map or {}
        self.search_results = search_results or {}
        self.memberships_map = memberships_map or {}
        self.contracts_map = contracts_map or {}
        self.raise_get_all: Exception | None = None
        self.closed = False
        self.calls: list[tuple[str, Any]] = []

    async def close(self) -> None:
        self.closed = True

    async def get_all_clients(self) -> list[dict]:
        self.calls.append(("get_all_clients", None))
        if self.raise_get_all:
            raise self.raise_get_all
        return self.all_clients

    async def get_clients(
        self, *, limit: int = 200, offset: int = 0, search_text: str = ""
    ) -> list[dict]:
        self.calls.append(
            ("get_clients", {"limit": limit, "offset": offset, "search_text": search_text})
        )
        if search_text in self.search_results:
            return self.search_results[search_text]
        if search_text:
            for client in self.all_clients:
                if str(client.get("Id")) == str(search_text):
                    return [client]
            return []
        return self.all_clients[offset : offset + limit]

    async def get_active_memberships(self, client_id: str) -> list[dict]:
        self.calls.append(("get_active_memberships", client_id))
        return self.memberships_map.get(str(client_id), [])

    async def get_client_contracts(self, client_id: str) -> list[dict]:
        self.calls.append(("get_client_contracts", client_id))
        return self.contracts_map.get(str(client_id), [])

    async def is_member_active(self, client_id: str) -> bool:
        self.calls.append(("is_member_active", client_id))
        return self.active_map.get(str(client_id), False)


class FakeDahuaClient:
    def __init__(self) -> None:
        self.add_user_result = True
        self.update_user_status_result = True
        self.upload_face_photo_result = True
        self.remove_user_result = True
        self.health_check_result = True
        self.raise_on_health = False
        self.all_users: list[dict] = []
        self.calls: list[tuple[str, Any]] = []

    async def add_user(self, **kwargs) -> bool:
        self.calls.append(("add_user", kwargs))
        return self.add_user_result

    async def update_user_status(self, user_id: str, card_status: int) -> bool:
        self.calls.append(("update_user_status", {"user_id": user_id, "card_status": card_status}))
        return self.update_user_status_result

    async def upload_face_photo(self, user_id: str, photo_base64: str, user_name: str = "") -> bool:
        self.calls.append(("upload_face_photo", {"user_id": user_id, "user_name": user_name}))
        return self.upload_face_photo_result

    async def remove_user(self, user_id: str) -> bool:
        self.calls.append(("remove_user", user_id))
        return self.remove_user_result

    async def health_check(self) -> bool:
        self.calls.append(("health_check", None))
        if self.raise_on_health:
            raise RuntimeError("health check failed")
        return self.health_check_result

    async def get_all_users(self) -> list[dict]:
        self.calls.append(("get_all_users", None))
        return self.all_users

    async def get_user(self, user_id: str) -> dict | None:
        self.calls.append(("get_user", user_id))
        for user in self.all_users:
            if user.get("UserID") == user_id:
                return user
        return None
