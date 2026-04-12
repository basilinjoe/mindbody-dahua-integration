from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dahua_sync_queue import DahuaSyncQueue


async def write_batch(
    db: AsyncSession, run_id: str, items: list[dict], flow_type: str = "full"
) -> int:
    """Insert a batch of queue items for the given run_id. Returns number of rows inserted."""
    if not items:
        return 0
    rows = [DahuaSyncQueue(run_id=run_id, flow_type=flow_type, **item) for item in items]
    db.add_all(rows)
    await db.commit()
    return len(rows)


async def load_pending(db: AsyncSession, run_id: str) -> list[DahuaSyncQueue]:
    """Return all pending DahuaSyncQueue items for the given run_id."""
    result = await db.execute(
        select(DahuaSyncQueue).where(
            DahuaSyncQueue.run_id == run_id,
            DahuaSyncQueue.status == "pending",
        )
    )
    return list(result.scalars().all())


async def mark_item(
    db: AsyncSession,
    item_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """Set status (and optional error_message) on a queue item. No-op if item not found."""
    result = await db.execute(select(DahuaSyncQueue).where(DahuaSyncQueue.id == item_id))
    item = result.scalar_one_or_none()
    if item is None:
        return
    item.status = status
    item.error_message = error_message
    item.processed_at = datetime.now(UTC)
    await db.commit()
