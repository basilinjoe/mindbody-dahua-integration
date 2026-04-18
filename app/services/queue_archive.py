from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dahua_sync_queue import DahuaSyncQueue

logger = logging.getLogger(__name__)

ARCHIVE_DIR = Path(__file__).resolve().parent.parent / "exports" / "sync_queue_archive"


def _serialize_queue_item(item: DahuaSyncQueue) -> dict:
    """Convert a DahuaSyncQueue ORM instance to a JSON-safe dict."""
    return {
        "id": item.id,
        "run_id": item.run_id,
        "flow_type": item.flow_type,
        "device_id": item.device_id,
        "mindbody_client_id": item.mindbody_client_id,
        "action": item.action,
        "status": item.status,
        "member_snapshot": item.member_snapshot,
        "dahua_user_id": item.dahua_user_id,
        "enrollment_id": item.enrollment_id,
        "error_message": item.error_message,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "processed_at": item.processed_at.isoformat() if item.processed_at else None,
    }


async def archive_previous_runs(
    db: AsyncSession, current_run_id: str, flow_type: str | None = None
) -> int:
    """Archive queue items from previous runs to JSON files, then delete from DB.

    Each distinct run_id gets its own JSON file under ARCHIVE_DIR.
    When flow_type is provided, only archive rows matching that flow_type.
    Returns total number of rows archived.
    """
    # Find all distinct run_ids that are NOT the current run
    q = select(DahuaSyncQueue.run_id).where(DahuaSyncQueue.run_id != current_run_id)
    if flow_type is not None:
        q = q.where(DahuaSyncQueue.flow_type == flow_type)
    result = await db.execute(q.distinct())
    old_run_ids = list(result.scalars().all())

    if not old_run_ids:
        return 0

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archived_at = datetime.now(UTC).isoformat()
    total_archived = 0

    for run_id in old_run_ids:
        items_q = select(DahuaSyncQueue).where(DahuaSyncQueue.run_id == run_id)
        if flow_type is not None:
            items_q = items_q.where(DahuaSyncQueue.flow_type == flow_type)
        rows = await db.execute(items_q.order_by(DahuaSyncQueue.id))
        items = list(rows.scalars().all())

        if not items:
            continue

        payload = {
            "run_id": run_id,
            "archived_at": archived_at,
            "item_count": len(items),
            "items": [_serialize_queue_item(item) for item in items],
        }

        file_suffix = f"_{flow_type}" if flow_type else ""
        file_path = ARCHIVE_DIR / f"sync_queue_{run_id}{file_suffix}.json"
        try:
            file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except OSError:
            logger.warning("Failed to write archive file %s — skipping deletion", file_path)
            continue

        delete_q = delete(DahuaSyncQueue).where(DahuaSyncQueue.run_id == run_id)
        if flow_type is not None:
            delete_q = delete_q.where(DahuaSyncQueue.flow_type == flow_type)
        await db.execute(delete_q)
        total_archived += len(items)

    await db.commit()
    return total_archived
