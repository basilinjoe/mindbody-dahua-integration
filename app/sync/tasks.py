from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from prefect import task
from prefect.cache_policies import INPUTS
from prefect.concurrency.asyncio import concurrency

from app.clients.dahua import DahuaClient
from app.clients.mindbody import MINDBODY_PAGE_SIZE, MindBodyClient
from app.config import Settings
from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.database import _get_async_session_factory
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient as MindBodyClientModel
from app.services import devices as devices_svc
from app.services import members as members_svc
from app.services import memberships as memberships_svc
from app.services import queue as queue_svc
from app.sync.blocks import MindBodyCredentials

logger = logging.getLogger(__name__)


# ── Utility ────────────────────────────────────────────────────────────────────


def _make_dahua_user_id(client_id: str) -> str:
    return str(int(client_id)) if client_id.isdigit() else client_id


def _make_card_no(client_id: str) -> str:
    num = int(client_id) if client_id.isdigit() else abs(hash(client_id)) % 100_000_000
    return f"MB{num:08d}"


def _format_dahua_date(iso_str: str | None) -> str | None:
    """Convert ISO 8601 string (e.g. '2025-12-31T23:59:59Z') to Dahua format ('2025-12-31 23:59:59')."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        logger.warning("Malformed date from MindBody API, skipping: %r", iso_str)
        return None


def _settings_from_creds(creds: MindBodyCredentials) -> Settings:
    """Build a Settings object from MindBodyCredentials block values."""
    return Settings(
        mindbody_api_key=creds.api_key.get_secret_value(),
        mindbody_site_id=creds.site_id,
        mindbody_username=creds.username,
        mindbody_password=creds.password.get_secret_value(),
        mindbody_api_base_url=creds.base_url,
    )


async def _get_dahua_client(device_id: int) -> tuple[DahuaClient, DahuaDevice]:
    """Load device credentials from DB and create a DahuaClient. Not a task."""
    async with _get_async_session_factory()() as db:
        device = await devices_svc.get_by_id(db, device_id)
        if device is None:
            raise ValueError(f"Device {device_id} not found")
    return (
        DahuaClient(
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
            door_ids=device.door_ids,
        ),
        device,
    )


@asynccontextmanager
async def _dahua_device(device_id: int):
    """Open a DahuaClient with the per-device concurrency lock, close on exit."""
    client, _ = await _get_dahua_client(device_id)
    try:
        async with concurrency(f"dahua-device-{device_id}", occupy=1):
            yield client
    finally:
        await client.close()


# ── MindBody tasks ─────────────────────────────────────────────────────────────


@task(
    name="fetch-members",
    retries=3,
    retry_delay_seconds=30,
    cache_policy=INPUTS,
    cache_expiration=timedelta(minutes=5),
    tags=["mindbody"],
)
async def fetch_members(modified_since: datetime | None = None) -> list[dict]:
    """
    Fetch MindBody clients.
    - modified_since=None  → full fetch (all members)
    - modified_since=<dt>  → incremental (only members modified on/after this date)
    Cached 5 min so concurrent flows share the same API call.
    Uses request.lastModifiedDate — the correct spec parameter name.
    """
    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    try:
        return await client.get_all_clients(modified_since=modified_since)
    finally:
        await client.close()


# ── DB helper tasks ─────────────────────────────────────────────────────────────


@task(name="load-device-ids-by-gate-type", tags=["db"])
async def load_device_ids_by_gate_type(gate_type: str) -> list[int]:
    """Return IDs of enabled devices matching gate_type."""
    async with _get_async_session_factory()() as db:
        return await devices_svc.list_by_gate_type(db, gate_type)


# ── Dahua tasks ─────────────────────────────────────────────────────────────────


@task(name="enroll-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def enroll_on_device(device_id: int, member: dict) -> bool:
    """Add a member to a Dahua device and record the enrollment in DB."""
    client_id = str(member.get("Id", ""))
    user_id = _make_dahua_user_id(client_id)
    card_no = _make_card_no(client_id)
    first_name = member.get("FirstName", "")
    last_name = member.get("LastName", "")
    card_name = f"{first_name} {last_name}".strip() or f"Member-{client_id}"
    valid_start = member.get("valid_start")
    valid_end = member.get("valid_end")

    async with _dahua_device(device_id) as client:
        success = await client.add_user(
            user_id=user_id,
            card_name=card_name,
            card_no=card_no,
            valid_start=valid_start,
            valid_end=valid_end,
        )

    return success


@task(name="deactivate-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def deactivate_on_device(device_id: int, dahua_user_id: str) -> bool:
    """Freeze a user on a Dahua device (card_status=4)."""
    async with _dahua_device(device_id) as client:
        return await client.update_user_status(dahua_user_id, card_status=4)


@task(name="reactivate-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def reactivate_on_device(device_id: int, dahua_user_id: str) -> bool:
    """Unfreeze a user on a Dahua device (card_status=0)."""
    async with _dahua_device(device_id) as client:
        return await client.update_user_status(dahua_user_id, card_status=0)


@task(name="check-device-health", retries=1, tags=["dahua"])
async def check_device_health_task(device_id: int) -> bool:
    """Return True if the Dahua device responds to a health check."""
    client, _ = await _get_dahua_client(device_id)
    try:
        return await client.health_check()
    finally:
        await client.close()


@task(name="load-all-devices", tags=["db"])
async def load_all_devices() -> list[DahuaDevice]:
    """Return all enabled Dahua devices for health check."""
    async with _get_async_session_factory()() as db:
        return await devices_svc.list_all(db)


# ── MindBody user + membership persistence tasks ────────────────────────────────


@task(
    name="fetch-all-memberships",
    retries=2,
    retry_delay_seconds=15,
    cache_policy=INPUTS,
    cache_expiration=timedelta(minutes=5),
    tags=["mindbody"],
)
async def fetch_all_memberships(client_ids: list[str]) -> dict[str, list[dict]]:
    """
    Fetch active memberships for all given MindBody client IDs.
    Returns a dict keyed by client_id → list of membership dicts.
    Uses the bulk endpoint (/activeclientsmemberships) in batches of 200.
    """
    if not client_ids:
        return {}

    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    try:
        batch_size = MINDBODY_PAGE_SIZE
        batches = [client_ids[i : i + batch_size] for i in range(0, len(client_ids), batch_size)]
        sem = asyncio.Semaphore(3)

        async def _fetch(batch: list[str]):
            async with sem:
                return await client.get_active_memberships_bulk(batch)

        chunks = await asyncio.gather(*[_fetch(b) for b in batches], return_exceptions=True)
    finally:
        await client.close()

    result: dict[str, list[dict]] = {}
    for batch, chunk in zip(batches, chunks, strict=True):
        if isinstance(chunk, Exception):
            logger.warning(
                "Bulk membership fetch failed for batch of %d — skipping: %s", len(batch), chunk
            )
        else:
            result.update(chunk)
    return result


@task(name="upsert-mindbody-users-batch", tags=["db"])
async def upsert_mindbody_users_batch(members: list[dict]) -> int:
    """Upsert MindBody user details into the mindbody_clients table. Returns rows written."""
    async with _get_async_session_factory()() as db:
        return await members_svc.upsert_batch(db, members)


@task(name="upsert-mindbody-memberships-batch", tags=["db"])
async def upsert_mindbody_memberships_batch(memberships_by_client: dict[str, list[dict]]) -> int:
    """Upsert memberships for each client. Returns total rows written."""
    async with _get_async_session_factory()() as db:
        return await memberships_svc.upsert_batch(db, memberships_by_client)


# ── Dahua sync queue tasks ───────────────────────────────────────────────────────


@task(name="write-sync-queue-batch", tags=["db"])
async def write_sync_queue_batch(run_id: str, items: list[dict]) -> int:
    """Insert a batch of planned Dahua operations into dahua_sync_queue. Returns rows inserted."""
    async with _get_async_session_factory()() as db:
        return await queue_svc.write_batch(db, run_id, items)


@task(name="load-pending-queue-items", tags=["db"])
async def load_pending_queue_items(run_id: str) -> list[DahuaSyncQueue]:
    """Load pending queue items for a given run_id."""
    async with _get_async_session_factory()() as db:
        return await queue_svc.load_pending(db, run_id)


@task(name="mark-queue-item", tags=["db"])
async def mark_queue_item(item_id: int, status: str, error_message: str | None = None) -> None:
    """Update status and error_message for a single queue item."""
    async with _get_async_session_factory()() as db:
        await queue_svc.mark_item(db, item_id, status, error_message)


# ── Access window tasks ──────────────────────────────────────────────────────────


@task(name="load-membership-windows", tags=["db"])
async def load_membership_windows(client_ids: list[str]) -> dict[str, dict]:
    """Return {client_id: {valid_start, valid_end}} for active memberships."""
    async with _get_async_session_factory()() as db:
        return await memberships_svc.load_windows(db, client_ids)


@task(name="load-active-members-from-db", tags=["db"])
async def load_active_members_from_db() -> list[MindBodyClientModel]:
    """Return all MindBody clients that are active and have an active membership."""
    async with _get_async_session_factory()() as db:
        return await members_svc.load_active(db)


@task(name="fetch-dahua-users-for-device", retries=2, retry_delay_seconds=15, tags=["dahua"])
async def fetch_dahua_users_for_device(device_id: int) -> list[dict]:
    """
    Fetch all AccessControlCard records from a Dahua device.
    Returns list of dicts with UserID, CardStatus, ValidDateStart, ValidDateEnd, CardName, etc.
    UserID on the device matches mindbody_id for integer client IDs.
    """
    client, _ = await _get_dahua_client(device_id)
    try:
        return await client.get_all_users()
    finally:
        await client.close()


@task(name="update-window-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def update_window_on_device(
    device_id: int,
    dahua_user_id: str,
    valid_start: str | None,
    valid_end: str | None,
) -> bool:
    """Update ValidDateStart/ValidDateEnd for an existing user on a Dahua device."""
    async with _dahua_device(device_id) as client:
        return await client.update_user_validity(dahua_user_id, valid_start, valid_end)
