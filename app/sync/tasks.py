from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from prefect import task
from prefect.cache_policies import INPUTS
from prefect.concurrency.asyncio import concurrency
from sqlalchemy import select, update

from app.clients.dahua import DahuaClient
from app.clients.mindbody import MindBodyClient
from app.config import Settings
from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.database import _get_async_session_factory
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient as MindBodyClientModel
from app.models.mindbody_membership import MindBodyMembership
from app.sync.blocks import MindBodyCredentials
from app.utils.photo import download_photo, process_photo_for_dahua

if TYPE_CHECKING:
    pass

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
        result = await db.execute(select(DahuaDevice).where(DahuaDevice.id == device_id))
        device = result.scalar_one()
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


# ── MindBody tasks ─────────────────────────────────────────────────────────────


@task(
    name="fetch-members",
    retries=3,
    retry_delay_seconds=30,
    cache_policy=INPUTS,
    cache_expiration=timedelta(minutes=5),
    tags=["mindbody"],
)
async def fetch_members(modified_after: datetime | None = None) -> list[dict]:
    """
    Fetch MindBody clients.
    - modified_after=None  → full fetch (all members)
    - modified_after=<dt>  → incremental (only members modified since then)
    Cached 5 min so concurrent flows share the same API call.
    """
    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    try:
        return await client.get_all_clients(modified_after=modified_after)
    finally:
        await client.close()


# ── DB helper tasks ─────────────────────────────────────────────────────────────


@task(name="load-device-ids-by-gate-type", tags=["db"])
async def load_device_ids_by_gate_type(gate_type: str) -> list[int]:
    """
    Return IDs of enabled devices matching gate_type.
    gate_type="all" returns ALL enabled devices with integration enabled.
    Otherwise returns devices where gate_type matches exactly.
    """
    async with _get_async_session_factory()() as db:
        stmt = (
            select(DahuaDevice.id)
            .where(DahuaDevice.is_enabled.is_(True))
            .where(DahuaDevice.enable_integration.is_(True))
        )
        if gate_type != "all":
            stmt = stmt.where(
                (DahuaDevice.gate_type == gate_type) | (DahuaDevice.gate_type == "all")
            )
        result = await db.execute(stmt)
        return [row[0] for row in result.fetchall()]


# ── Dahua tasks ─────────────────────────────────────────────────────────────────


@task(name="enroll-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def enroll_on_device(device_id: int, member: dict, photo_max_kb: int = 200) -> bool:
    """Add a member to a Dahua device and record the enrollment in DB."""
    client_id = str(member.get("Id", ""))
    user_id = _make_dahua_user_id(client_id)
    card_no = _make_card_no(client_id)
    first_name = member.get("FirstName", "")
    last_name = member.get("LastName", "")
    card_name = f"{first_name} {last_name}".strip() or f"Member-{client_id}"
    valid_start = member.get("valid_start")
    valid_end = member.get("valid_end")

    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            success = await client.add_user(
                user_id=user_id,
                card_name=card_name,
                card_no=card_no,
                valid_start=valid_start,
                valid_end=valid_end,
            )

            # Attempt face photo upload
            photo_url = member.get("PhotoUrl")
            if success and photo_url and "default" not in photo_url.lower():
                try:
                    photo_bytes = await download_photo(photo_url)
                    if photo_bytes:
                        b64 = process_photo_for_dahua(photo_bytes, photo_max_kb)
                        if b64:
                            await client.upload_face_photo(user_id, b64, card_name)
                except Exception:
                    logger.warning("Photo upload failed for %s on device %d", client_id, device_id)
        finally:
            await client.close()

    return success


@task(name="deactivate-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def deactivate_on_device(device_id: int, dahua_user_id: str) -> bool:
    """Freeze a user on a Dahua device (card_status=4)."""
    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            return await client.update_user_status(dahua_user_id, card_status=4)
        finally:
            await client.close()


@task(name="reactivate-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def reactivate_on_device(device_id: int, dahua_user_id: str) -> bool:
    """Unfreeze a user on a Dahua device (card_status=0)."""
    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            return await client.update_user_status(dahua_user_id, card_status=0)
        finally:
            await client.close()


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
        result = await db.execute(select(DahuaDevice).where(DahuaDevice.is_enabled.is_(True)))
        return list(result.scalars().all())


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
    Runs as a single task (not one task per member) to keep Prefect UI clean.
    """
    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    sem = asyncio.Semaphore(10)

    async def _fetch_one(cid: str) -> tuple[str, list[dict]]:
        async with sem:
            try:
                memberships = await client.get_active_memberships(cid)
                return cid, memberships
            except Exception:
                logger.warning("Could not fetch memberships for client %s", cid)
                return cid, []

    try:
        pairs = await asyncio.gather(*[_fetch_one(cid) for cid in client_ids])
    finally:
        await client.close()
    return dict(pairs)


@task(name="upsert-mindbody-users-batch", tags=["db"])
async def upsert_mindbody_users_batch(members: list[dict]) -> int:
    """
    Async upsert of MindBody user details into the mindbody_clients table.
    Returns count of rows processed.
    """
    now = datetime.utcnow()
    processed = 0
    async with _get_async_session_factory()() as db:
        for m in members:
            mid = str(m.get("Id", "")).strip()
            if not mid:
                continue
            row = {
                "unique_id": str(m["UniqueId"]) if m.get("UniqueId") is not None else None,
                "first_name": m.get("FirstName", ""),
                "last_name": m.get("LastName", ""),
                "email": m.get("Email"),
                "mobile_phone": m.get("MobilePhone"),
                "home_phone": m.get("HomePhone"),
                "work_phone": m.get("WorkPhone"),
                "status": m.get("Status"),
                "active": bool(m.get("Active", False)),
                "birth_date": m.get("BirthDate"),
                "gender": m.get("Gender"),
                "created_at_mb": m.get("CreationDate"),
                "last_modified_at_mb": m.get("LastModifiedDateTime"),
                "photo_url": m.get("PhotoUrl"),
                "last_fetched_at": now,
            }
            result = await db.execute(
                select(MindBodyClientModel).where(MindBodyClientModel.mindbody_id == mid)
            )
            existing = result.scalar_one_or_none()
            if existing:
                for key, val in row.items():
                    setattr(existing, key, val)
            else:
                db.add(MindBodyClientModel(mindbody_id=mid, **row))
            processed += 1
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Failed to upsert mindbody users batch")
    return processed


@task(name="upsert-mindbody-memberships-batch", tags=["db"])
async def upsert_mindbody_memberships_batch(memberships_by_client: dict[str, list[dict]]) -> int:
    """
    Upsert membership rows into mindbody_memberships.
    For each client_id: deletes existing rows then inserts fresh ones.
    Returns total rows inserted.
    """
    from sqlalchemy import delete as sa_delete

    now_utc = datetime.now(UTC)
    now = now_utc.replace(tzinfo=None)  # naive UTC for TIMESTAMP WITHOUT TIME ZONE columns
    total_inserted = 0
    async with _get_async_session_factory()() as db:
        for client_id, memberships in memberships_by_client.items():
            # Replace strategy: delete current rows for this client then insert fresh
            await db.execute(
                sa_delete(MindBodyMembership).where(
                    MindBodyMembership.mindbody_client_id == client_id
                )
            )
            for mb in memberships:
                exp_str = mb.get("ExpirationDate")
                is_active = True
                if exp_str:
                    try:
                        exp_dt = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
                        is_active = exp_dt > now_utc
                    except (ValueError, TypeError):
                        pass
                db.add(
                    MindBodyMembership(
                        mindbody_client_id=client_id,
                        membership_id=str(mb.get("Id", "") or ""),
                        membership_name=mb.get("Name"),
                        status=mb.get("Status"),
                        start_date=mb.get("StartDate"),
                        expiration_date=exp_str,
                        is_active=is_active,
                        last_synced_at=now,
                    )
                )
                total_inserted += 1
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Failed to upsert memberships batch")
    return total_inserted


# ── Dahua sync queue tasks ───────────────────────────────────────────────────────


@task(name="write-sync-queue-batch", tags=["db"])
async def write_sync_queue_batch(run_id: str, items: list[dict]) -> int:
    """
    Insert a batch of planned Dahua operations into dahua_sync_queue with status='pending'.
    Each item dict must have: device_id, mindbody_client_id, action,
    and optionally: member_snapshot, dahua_user_id, enrollment_id.
    Returns count of rows inserted.
    """
    async with _get_async_session_factory()() as db:
        for item in items:
            db.add(
                DahuaSyncQueue(
                    run_id=run_id,
                    device_id=item["device_id"],
                    mindbody_client_id=item["mindbody_client_id"],
                    action=item["action"],
                    status="pending",
                    member_snapshot=item.get("member_snapshot"),
                    dahua_user_id=item.get("dahua_user_id"),
                    enrollment_id=item.get("enrollment_id"),
                )
            )
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Failed to write sync queue batch (run_id=%s)", run_id)
            return 0
    return len(items)


@task(name="load-pending-queue-items", tags=["db"])
async def load_pending_queue_items(run_id: str) -> list[DahuaSyncQueue]:
    """
    Load all actionable queue items for a given run_id.
    Includes both 'pending' (not yet attempted) and 'failed' (eligible for retry).
    Returns detached ORM objects (session is closed after load).
    """
    async with _get_async_session_factory()() as db:
        result = await db.execute(
            select(DahuaSyncQueue)
            .where(DahuaSyncQueue.run_id == run_id)
            .where(DahuaSyncQueue.status.in_(["pending", "failed"]))
        )
        items = list(result.scalars().all())
        # Expunge so objects can be used outside the session
        for item in items:
            db.expunge(item)
        return items


@task(name="mark-queue-item", tags=["db"])
async def mark_queue_item(item_id: int, status: str, error_message: str | None = None) -> None:
    """Update status, error_message, and processed_at for a single queue item."""
    async with _get_async_session_factory()() as db:
        await db.execute(
            update(DahuaSyncQueue)
            .where(DahuaSyncQueue.id == item_id)
            .values(
                status=status,
                error_message=error_message,
                processed_at=datetime.now(UTC),
            )
        )
        await db.commit()


# ── Access window tasks ──────────────────────────────────────────────────────────


@task(name="load-membership-windows", tags=["db"])
async def load_membership_windows(
    client_ids: list[str],
) -> dict[str, tuple[str | None, str | None]]:
    """
    For each client_id, return (start_date, expiration_date) of their best active
    membership from mindbody_memberships. NULL expiration_date (ongoing) is preferred.
    Returns {client_id: (start_date, expiration_date)}.
    """
    from app.models.mindbody_membership import MindBodyMembership as _Mbm

    result_map: dict[str, tuple[str | None, str | None]] = {}
    if not client_ids:
        return result_map

    async with _get_async_session_factory()() as db:
        rows = await db.execute(
            select(_Mbm.mindbody_client_id, _Mbm.start_date, _Mbm.expiration_date)
            .where(_Mbm.mindbody_client_id.in_(client_ids))
            .where(_Mbm.is_active.is_(True))
        )
        for cid, start, expiry in rows.fetchall():
            existing = result_map.get(cid)
            # Prefer NULL expiry (ongoing) over dated; among dated, prefer latest
            if existing is None:
                result_map[cid] = (start, expiry)
            elif expiry is None:
                result_map[cid] = (start, None)  # ongoing wins
            elif existing[1] is not None and expiry > existing[1]:
                result_map[cid] = (start, expiry)  # later expiry wins

    return result_map


@task(name="load-active-members-from-db", tags=["db"])
async def load_active_members_from_db() -> list[dict]:
    """
    Return all MindBody clients that are active AND have at least one active membership
    in the local DB. Shaped as API-compatible dicts (Id, FirstName, LastName, Gender,
    PhotoUrl, Email) so they slot directly into the existing enroll snapshot format.
    """
    from sqlalchemy import exists as sa_exists

    stmt = (
        select(MindBodyClientModel)
        .where(MindBodyClientModel.active.is_(True))
        .where(
            sa_exists(
                select(MindBodyMembership.id)
                .where(MindBodyMembership.mindbody_client_id == MindBodyClientModel.mindbody_id)
                .where(MindBodyMembership.is_active.is_(True))
                .correlate(MindBodyClientModel)
            )
        )
    )
    async with _get_async_session_factory()() as db:
        result = await db.execute(stmt)
        rows = list(result.scalars().all())
    return [
        {
            "Id": row.mindbody_id,
            "FirstName": row.first_name,
            "LastName": row.last_name,
            "Gender": row.gender,
            "PhotoUrl": row.photo_url,
            "Email": row.email,
        }
        for row in rows
    ]


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
    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            return await client.update_user_validity(dahua_user_id, valid_start, valid_end)
        finally:
            await client.close()
