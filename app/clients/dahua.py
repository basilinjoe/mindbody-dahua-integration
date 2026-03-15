from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from app.utils.retry import dahua_retry

logger = logging.getLogger(__name__)


class DahuaClient:
    """
    HTTP CGI client for a single Dahua ASI7214S face recognition device.

    Uses Digest Auth.  All record CRUD goes through recordUpdater.cgi;
    face photos go through FaceInfoManager.cgi (POST with JSON body).
    """

    def __init__(
        self,
        host: str,
        port: int = 80,
        username: str = "admin",
        password: str = "",
        door_ids: str = "0",
        device_id: int | None = None,
        device_name: str = "",
    ) -> None:
        self.device_id = device_id
        self.device_name = device_name
        self._base = f"http://{host}:{port}"
        self._auth = httpx.DigestAuth(username, password)
        self._doors = [int(d.strip()) for d in door_ids.split(",") if d.strip()]
        self._semaphore = asyncio.Semaphore(2)  # limit concurrent device requests
        self._http = httpx.AsyncClient(timeout=15, auth=self._auth)

    async def close(self) -> None:
        await self._http.aclose()

    # ---- Helpers -----------------------------------------------------------

    def _door_params(self) -> dict[str, str]:
        return {f"Doors[{i}]": str(d) for i, d in enumerate(self._doors)}

    async def _get(self, path: str, params: dict | None = None) -> httpx.Response:
        async with self._semaphore:
            resp = await self._http.get(f"{self._base}{path}", params=params or {})
            return resp

    async def _post_json(self, path: str, json_body: dict) -> httpx.Response:
        async with self._semaphore:
            resp = await self._http.post(
                f"{self._base}{path}",
                json=json_body,
                headers={"Content-Type": "application/json"},
            )
            return resp

    # ---- Health ------------------------------------------------------------

    @dahua_retry
    async def health_check(self) -> bool:
        """Ping the device. Returns True if reachable."""
        try:
            resp = await self._get("/cgi-bin/magicBox.cgi", {"action": "getSystemInfo"})
            return resp.status_code == 200
        except Exception:
            return False

    # ---- User / Card Management (recordUpdater.cgi) -----------------------

    @dahua_retry
    async def add_user(
        self,
        user_id: str,
        card_name: str,
        card_no: str,
        card_status: int = 0,
        card_type: int = 0,
        valid_start: str | None = None,
        valid_end: str | None = None,
    ) -> bool:
        """Insert a new access-control user record on the device."""
        params: dict[str, str] = {
            "action": "insert",
            "name": "AccessControlCard",
            "UserID": user_id,
            "CardName": card_name,
            "CardNo": card_no,
            "CardStatus": str(card_status),
            "CardType": str(card_type),
        }
        params.update(self._door_params())
        if valid_start:
            params["ValidDateStart"] = valid_start
        if valid_end:
            params["ValidDateEnd"] = valid_end

        resp = await self._get("/cgi-bin/recordUpdater.cgi", params)
        ok = resp.status_code == 200 and "OK" in resp.text
        if not ok:
            logger.error("add_user %s failed on %s: %s", user_id, self.device_name, resp.text)
        return ok

    @dahua_retry
    async def update_user_status(self, user_id: str, card_status: int) -> bool:
        """
        Update CardStatus for an existing user.
        0 = Normal (active), 4 = Frozen (deactivated).
        """
        params = {
            "action": "update",
            "name": "AccessControlCard",
            "UserID": user_id,
            "CardStatus": str(card_status),
        }
        resp = await self._get("/cgi-bin/recordUpdater.cgi", params)
        ok = resp.status_code == 200 and "OK" in resp.text
        if not ok:
            logger.error("update_user_status %s -> %d failed on %s: %s", user_id, card_status, self.device_name, resp.text)
        return ok

    @dahua_retry
    async def update_user_validity(
        self, user_id: str, valid_start: str | None, valid_end: str | None
    ) -> bool:
        """Update ValidDateStart / ValidDateEnd for an existing user (access window)."""
        params: dict[str, str] = {
            "action": "update",
            "name": "AccessControlCard",
            "UserID": user_id,
        }
        if valid_start:
            params["ValidDateStart"] = valid_start
        if valid_end:
            params["ValidDateEnd"] = valid_end
        resp = await self._get("/cgi-bin/recordUpdater.cgi", params)
        ok = resp.status_code == 200 and "OK" in resp.text
        if not ok:
            logger.error(
                "update_user_validity %s failed on %s: %s", user_id, self.device_name, resp.text
            )
        return ok

    @dahua_retry
    async def remove_user(self, user_id: str) -> bool:
        """Fully remove a user and their face data from the device."""
        params = {
            "action": "remove",
            "name": "AccessControlCard",
            "UserID": user_id,
        }
        resp = await self._get("/cgi-bin/recordUpdater.cgi", params)
        ok = resp.status_code == 200 and "OK" in resp.text
        return ok

    # ---- Face Photo Management (FaceInfoManager.cgi) ----------------------

    @dahua_retry
    async def upload_face_photo(self, user_id: str, photo_base64: str, user_name: str = "") -> bool:
        """Upload a face photo for an existing user."""
        body = {
            "UserID": user_id,
            "Info": {"UserName": user_name},
            "PhotoData": [photo_base64],
        }
        resp = await self._post_json("/cgi-bin/FaceInfoManager.cgi?action=add", body)
        ok = resp.status_code == 200 and "OK" in resp.text
        if not ok:
            logger.error("upload_face_photo %s failed on %s: %s", user_id, self.device_name, resp.text[:300])
        return ok

    @dahua_retry
    async def remove_face_photo(self, user_id: str) -> bool:
        resp = await self._get(
            "/cgi-bin/FaceInfoManager.cgi",
            {"action": "remove", "UserID": user_id},
        )
        return resp.status_code == 200

    async def get_face_photo(self, user_id: str) -> str | None:
        """
        Check whether the user has a face enrolled on the device.

        The Dahua API does not provide an endpoint to download the stored photo
        (section 12.3.4 doFind returns MD5 hashes only). This method uses the
        correct 3-step find protocol (startFind → doFind → stopFind) and returns
        None in all cases — callers should source the photo from local storage.
        """
        # Step 1: start find session
        resp = await self._get(
            "/cgi-bin/FaceInfoManager.cgi",
            {"action": "startFind", "Condition.UserID": user_id},
        )
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
        except Exception:
            return None

        token = data.get("Token")
        total = data.get("Total", 0)

        if not token:
            return None

        # Step 2: fetch result (MD5 hashes only — no photo data in response)
        await self._get(
            "/cgi-bin/FaceInfoManager.cgi",
            {"action": "doFind", "Token": str(token), "Offset": "0", "Count": "1"},
        )

        # Step 3: always stop the session
        await self._get(
            "/cgi-bin/FaceInfoManager.cgi",
            {"action": "stopFind", "Token": str(token)},
        )

        # The device API cannot return photo data — return None regardless
        _ = total  # face count available if needed by callers in future
        return None

    # ---- Record Querying ---------------------------------------------------

    @dahua_retry
    async def get_all_users(self) -> list[dict]:
        """
        Query all AccessControlCard records on the device.
        Returns parsed list of user dicts.
        """
        resp = await self._get(
            "/cgi-bin/recordFinder.cgi",
            {"action": "find", "name": "AccessControlCard", "count": "10000"},
        )
        if resp.status_code != 200:
            logger.error("get_all_users failed on %s: %s", self.device_name, resp.text[:300])
            return []
        return self._parse_record_finder_response(resp.text)

    def _parse_record_finder_response(self, text: str) -> list[dict]:
        """Parse the key=value response from recordFinder.cgi into a list of dicts."""
        records: dict[int, dict] = {}
        for line in text.strip().splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, _, value = line.partition("=")
            # e.g. records[0].CardName=John
            if key.startswith("records["):
                bracket_end = key.index("]")
                idx = int(key[len("records["):bracket_end])
                field = key[bracket_end + 2:]  # skip "].
                if idx not in records:
                    records[idx] = {}
                records[idx][field] = value
        return list(records.values())

    async def get_user(self, user_id: str) -> dict | None:
        """Look up a single user by UserID."""
        users = await self.get_all_users()
        for u in users:
            if u.get("UserID") == user_id:
                return u
        return None

    # ---- Door Control ------------------------------------------------------

    @dahua_retry
    async def open_door(self, door_id: int = 0) -> bool:
        """Remotely open a door (for testing / admin)."""
        resp = await self._get(
            "/cgi-bin/accessControl.cgi",
            {"action": "openDoor", "UserID": "101", "Type": "Remote", "channel": str(door_id)},
        )
        return resp.status_code == 200

    @dahua_retry
    async def close_door(self, door_id: int = 0) -> bool:
        """Remotely close a door (for testing / admin)."""
        resp = await self._get(
            "/cgi-bin/accessControl.cgi",
            {"action": "closeDoor", "UserID": "101", "Type": "Remote", "channel": str(door_id)},
        )
        return resp.status_code == 200

    # ---- Snapshot / Camera -------------------------------------------------

    @dahua_retry
    async def capture_snapshot(self, channel: int = 0) -> bytes | None:
        """
        Capture a JPEG snapshot from the device camera.
        Returns raw JPEG bytes or None on failure.
        """
        async with self._semaphore:
            resp = await self._http.get(
                f"{self._base}/cgi-bin/snapshot.cgi",
                params={"channel": str(channel)},
            )
            if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("image/"):
                return resp.content
            logger.error(
                "capture_snapshot failed on %s (status %d, content-type %s)",
                self.device_name, resp.status_code, resp.headers.get("content-type", "?"),
            )
            return None
