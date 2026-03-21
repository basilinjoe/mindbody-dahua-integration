from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import DateTime

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership


async def get_stats(db: AsyncSession) -> dict:
    """Return a dict of aggregate counts for the dashboard summary panel."""
    now = datetime.now(UTC)

    total_members = (await db.execute(select(func.count(MindBodyClient.id)))).scalar() or 0

    active_members = (
        await db.execute(
            select(func.count(MindBodyClient.id)).where(MindBodyClient.active.is_(True))
        )
    ).scalar() or 0

    active_with_membership = (
        await db.execute(
            select(func.count(MindBodyClient.id.distinct()))
            .join(
                MindBodyMembership,
                MindBodyClient.mindbody_id == MindBodyMembership.mindbody_client_id,
            )
            .where(
                MindBodyClient.active.is_(True),
                MindBodyMembership.is_active.is_(True),
                or_(
                    MindBodyMembership.expiration_date.is_(None),
                    cast(MindBodyMembership.expiration_date, DateTime(timezone=True)) > now,
                ),
            )
        )
    ).scalar() or 0

    total_devices = (
        await db.execute(select(func.count(DahuaDevice.id)).where(DahuaDevice.is_enabled.is_(True)))
    ).scalar() or 0

    online_devices = (
        await db.execute(
            select(func.count(DahuaDevice.id)).where(
                DahuaDevice.is_enabled.is_(True),
                DahuaDevice.status == "online",
            )
        )
    ).scalar() or 0

    pending_queue = (
        await db.execute(
            select(func.count(DahuaSyncQueue.id)).where(DahuaSyncQueue.status == "pending")
        )
    ).scalar() or 0

    failed_queue = (
        await db.execute(
            select(func.count(DahuaSyncQueue.id)).where(DahuaSyncQueue.status == "failed")
        )
    ).scalar() or 0

    from datetime import timedelta

    cutoff_24h = (now - timedelta(hours=24)).replace(tzinfo=None)
    failed_24h = (
        await db.execute(
            select(func.count(DahuaSyncQueue.id)).where(
                DahuaSyncQueue.status == "failed",
                DahuaSyncQueue.created_at >= cutoff_24h,
            )
        )
    ).scalar() or 0

    return {
        "total_members": total_members,
        "active_members": active_members,
        "active_with_membership": active_with_membership,
        "total_devices": total_devices,
        "online_devices": online_devices,
        "pending_queue": pending_queue,
        "failed_queue": failed_queue,
        "failed_24h": failed_24h,
    }


async def get_recent_queue(
    db: AsyncSession, limit: int = 10
) -> list[tuple[DahuaSyncQueue, str | None]]:
    """Return (queue_item, device_name) tuples for the most recent queue rows."""
    result = await db.execute(
        select(DahuaSyncQueue, DahuaDevice.name)
        .outerjoin(DahuaDevice, DahuaDevice.id == DahuaSyncQueue.device_id)
        .order_by(DahuaSyncQueue.created_at.desc())
        .limit(limit)
    )
    return list(result.all())


async def get_mb_breakdown(db: AsyncSession) -> dict:
    """Return gender and subscription breakdown for the dashboard."""
    # Gender breakdown
    gender_result = await db.execute(
        select(MindBodyClient.gender, func.count(MindBodyClient.id))
        .where(MindBodyClient.active.is_(True))
        .group_by(MindBodyClient.gender)
    )
    gender_counts = {(row[0] or "Unknown"): row[1] for row in gender_result}

    total = sum(gender_counts.values()) or 1  # avoid division by zero
    male_count = gender_counts.get("Male", 0)
    female_count = gender_counts.get("Female", 0)

    # Subscription breakdown
    active_sub_count = (
        await db.execute(
            select(func.count(MindBodyClient.id.distinct()))
            .join(
                MindBodyMembership,
                MindBodyClient.mindbody_id == MindBodyMembership.mindbody_client_id,
            )
            .where(
                MindBodyClient.active.is_(True),
                MindBodyMembership.is_active.is_(True),
            )
        )
    ).scalar() or 0
    no_sub_count = sum(gender_counts.values()) - active_sub_count

    return {
        "male_count": male_count,
        "male_pct": round(male_count * 100 / total),
        "female_count": female_count,
        "female_pct": round(female_count * 100 / total),
        "active_sub_count": active_sub_count,
        "active_sub_pct": round(active_sub_count * 100 / total),
        "no_sub_count": no_sub_count,
        "no_sub_pct": round(no_sub_count * 100 / total),
    }


async def get_device_rows(db: AsyncSession) -> list[dict]:
    """Return dicts with device, pending count, and failed_24h count for enabled devices."""
    from datetime import timedelta

    devices_result = await db.execute(
        select(DahuaDevice).where(DahuaDevice.is_enabled.is_(True)).order_by(DahuaDevice.name)
    )
    devices = list(devices_result.scalars().all())

    cutoff = (datetime.now(UTC) - timedelta(hours=24)).replace(tzinfo=None)
    rows = []
    for device in devices:
        pending = (
            await db.execute(
                select(func.count(DahuaSyncQueue.id)).where(
                    DahuaSyncQueue.device_id == device.id,
                    DahuaSyncQueue.status == "pending",
                )
            )
        ).scalar() or 0

        failed_24h = (
            await db.execute(
                select(func.count(DahuaSyncQueue.id)).where(
                    DahuaSyncQueue.device_id == device.id,
                    DahuaSyncQueue.status == "failed",
                    DahuaSyncQueue.created_at >= cutoff,
                )
            )
        ).scalar() or 0

        rows.append({"device": device, "pending": pending, "failed_24h": failed_24h})
    return rows
