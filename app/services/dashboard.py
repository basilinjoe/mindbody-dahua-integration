from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import or_

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership


async def get_stats(db: AsyncSession) -> dict:
    """Return a dict of aggregate counts for the dashboard summary panel."""
    now = datetime.now(UTC)

    total_members = (
        await db.execute(select(func.count(MindBodyClient.id)))
    ).scalar() or 0

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
                    MindBodyMembership.expiration_date > now,
                ),
            )
        )
    ).scalar() or 0

    total_devices = (
        await db.execute(
            select(func.count(DahuaDevice.id)).where(DahuaDevice.is_enabled.is_(True))
        )
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

    return {
        "total_members": total_members,
        "active_members": active_members,
        "active_with_membership": active_with_membership,
        "total_devices": total_devices,
        "online_devices": online_devices,
        "pending_queue": pending_queue,
        "failed_queue": failed_queue,
    }


async def get_recent_queue(db: AsyncSession, limit: int = 10) -> list[DahuaSyncQueue]:
    """Return the most recent DahuaSyncQueue rows ordered by created_at descending."""
    result = await db.execute(
        select(DahuaSyncQueue).order_by(DahuaSyncQueue.created_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


async def get_mb_breakdown(db: AsyncSession) -> dict[str, int]:
    """Return {gender_label: count} for active MindBody members."""
    result = await db.execute(
        select(MindBodyClient.gender, func.count(MindBodyClient.id))
        .where(MindBodyClient.active.is_(True))
        .group_by(MindBodyClient.gender)
    )
    return {(row[0] or "Unknown"): row[1] for row in result}


async def get_device_rows(db: AsyncSession) -> list[DahuaDevice]:
    """Return enabled DahuaDevice rows ordered by name."""
    result = await db.execute(
        select(DahuaDevice)
        .where(DahuaDevice.is_enabled.is_(True))
        .order_by(DahuaDevice.name)
    )
    return list(result.scalars().all())
