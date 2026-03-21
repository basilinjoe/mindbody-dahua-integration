from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.device import DahuaDevice


async def list_all(db: AsyncSession) -> list[DahuaDevice]:
    """Return all enabled DahuaDevice rows."""
    result = await db.execute(select(DahuaDevice).where(DahuaDevice.is_enabled.is_(True)))
    return list(result.scalars().all())


async def list_by_gate_type(db: AsyncSession, gate_type: str) -> list[int]:
    """Return IDs of enabled devices whose gate_type matches 'all' or the given type."""
    result = await db.execute(
        select(DahuaDevice.id).where(
            DahuaDevice.is_enabled.is_(True),
            DahuaDevice.gate_type.in_(["all", gate_type]),
        )
    )
    return list(result.scalars().all())


async def get_by_id(db: AsyncSession, device_id: int) -> DahuaDevice | None:
    """Return a single DahuaDevice by primary key, or None."""
    result = await db.execute(select(DahuaDevice).where(DahuaDevice.id == device_id))
    return result.scalar_one_or_none()


async def update_status(
    db: AsyncSession,
    device_id: int,
    status: str,
) -> None:
    """Set device.status and, if online, device.last_seen_at. No-op if device not found."""
    result = await db.execute(select(DahuaDevice).where(DahuaDevice.id == device_id))
    device = result.scalar_one_or_none()
    if device is None:
        return
    device.status = status
    if status == "online":
        device.last_seen_at = datetime.now(UTC)
    await db.commit()
