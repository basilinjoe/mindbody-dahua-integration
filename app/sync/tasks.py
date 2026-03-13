from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from prefect import task
from prefect.cache_policies import INPUTS
from prefect.concurrency.asyncio import concurrency
from sqlalchemy import select, update

from app.clients.dahua import DahuaClient
from app.clients.mindbody import MindBodyClient
from app.config import Settings
from app.models.database import _get_async_session_factory
from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.member_device_enrollment import MemberDeviceEnrollment
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


@task(name="fetch-member", retries=2, retry_delay_seconds=10, tags=["mindbody"])
async def fetch_member(client_id: str) -> dict:
    """Fetch a single MindBody client by ID."""
    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    try:
        clients = await client.get_clients(search_text=client_id, limit=1)
        return clients[0] if clients else {}
    finally:
        await client.close()


@task(name="check-membership", retries=2, retry_delay_seconds=10, tags=["mindbody"])
async def check_membership(client_id: str) -> bool:
    """Return True if the MindBody member has an active membership."""
    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    try:
        return await client.is_member_active(client_id)
    finally:
        await client.close()


@task(name="get-active-member-ids", retries=2, retry_delay_seconds=10, tags=["mindbody"])
async def get_active_member_ids(client_ids: list[str]) -> set[str]:
    """Return subset of client_ids whose membership is currently active."""
    creds = await MindBodyCredentials.load("production")
    client = MindBodyClient(settings=_settings_from_creds(creds))
    try:
        active: set[str] = set()
        for cid in client_ids:
            try:
                if await client.is_member_active(cid):
                    active.add(cid)
            except Exception:
                logger.warning("Could not check membership for %s", cid)
        return active
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


@task(name="load-enrollments-for-device", tags=["db"])
async def load_enrollments_for_device(device_id: int) -> dict[str, MemberDeviceEnrollment]:
    """
    Return active enrollments for a specific device.
    Key: mindbody_client_id → MemberDeviceEnrollment row.
    """
    async with _get_async_session_factory()() as db:
        result = await db.execute(
            select(SyncedMember.mindbody_client_id, MemberDeviceEnrollment)
            .join(MemberDeviceEnrollment, MemberDeviceEnrollment.synced_member_id == SyncedMember.id)
            .where(MemberDeviceEnrollment.device_id == device_id)
        )
        return {row[0]: row[1] for row in result.fetchall()}


@task(name="load-active-enrollments-for-member", tags=["db"])
async def load_active_enrollments_for_member(client_id: str) -> list[MemberDeviceEnrollment]:
    """Return all active device enrollments for a given MindBody client_id."""
    async with _get_async_session_factory()() as db:
        result = await db.execute(
            select(MemberDeviceEnrollment)
            .join(SyncedMember, SyncedMember.id == MemberDeviceEnrollment.synced_member_id)
            .where(SyncedMember.mindbody_client_id == client_id)
            .where(MemberDeviceEnrollment.is_active.is_(True))
        )
        return list(result.scalars().all())


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
    gender = (member.get("Gender") or "").lower() or None

    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            success = await client.add_user(user_id=user_id, card_name=card_name, card_no=card_no)

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

    # Record enrollment in DB
    async with _get_async_session_factory()() as db:
        # Get or create SyncedMember
        result = await db.execute(
            select(SyncedMember).where(SyncedMember.mindbody_client_id == client_id)
        )
        synced = result.scalar_one_or_none()
        if synced is None:
            synced = SyncedMember(
                mindbody_client_id=client_id,
                dahua_user_id=user_id,
                card_no=card_no,
                first_name=first_name,
                last_name=last_name,
                email=member.get("Email"),
                gender=gender,
                is_active_in_mindbody=True,
                is_active_in_dahua=success,
                has_face_photo=False,
                is_manual=False,
                last_synced_at=datetime.now(timezone.utc),
            )
            db.add(synced)
            await db.flush()  # get synced.id
        else:
            synced.is_active_in_dahua = success
            synced.last_synced_at = datetime.now(timezone.utc)
            if gender:
                synced.gender = gender

        # Add enrollment record
        enrollment = MemberDeviceEnrollment(
            synced_member_id=synced.id,
            device_id=device_id,
            dahua_user_id=user_id,
            is_active=success,
        )
        db.add(enrollment)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Failed to record enrollment for %s on device %d", client_id, device_id)

    return success


@task(name="deactivate-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def deactivate_on_device(device_id: int, dahua_user_id: str, enrollment_id: int | None = None) -> bool:
    """Freeze a user on a Dahua device (card_status=4) and mark enrollment inactive."""
    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            success = await client.update_user_status(dahua_user_id, card_status=4)
        finally:
            await client.close()

    if success and enrollment_id:
        async with _get_async_session_factory()() as db:
            await db.execute(
                update(MemberDeviceEnrollment)
                .where(MemberDeviceEnrollment.id == enrollment_id)
                .values(is_active=False, deactivated_at=datetime.now(timezone.utc))
            )
            await db.commit()

    return success


@task(name="reactivate-on-device", retries=2, retry_delay_seconds=5, tags=["dahua"])
async def reactivate_on_device(device_id: int, dahua_user_id: str, enrollment_id: int | None = None) -> bool:
    """Unfreeze a user on a Dahua device (card_status=0) and mark enrollment active."""
    async with concurrency(f"dahua-device-{device_id}", occupy=1):
        client, _ = await _get_dahua_client(device_id)
        try:
            success = await client.update_user_status(dahua_user_id, card_status=0)
        finally:
            await client.close()

    if success and enrollment_id:
        async with _get_async_session_factory()() as db:
            await db.execute(
                update(MemberDeviceEnrollment)
                .where(MemberDeviceEnrollment.id == enrollment_id)
                .values(is_active=True, deactivated_at=None)
            )
            await db.commit()

    return success


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
        result = await db.execute(
            select(DahuaDevice).where(DahuaDevice.is_enabled.is_(True))
        )
        return list(result.scalars().all())
