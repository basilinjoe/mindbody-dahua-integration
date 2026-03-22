from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mindbody_membership import MindBodyMembership


async def upsert_batch(db: AsyncSession, memberships_by_client: dict[str, list[dict]]) -> int:
    """Upsert raw MindBody membership dicts keyed by client_id. Returns number of rows written."""
    now = datetime.now(UTC)
    rows = []
    for client_id, memberships in memberships_by_client.items():
        for m in memberships:
            membership_id = str(m.get("Id", "")).strip()
            if not membership_id:
                continue
            # The activeclientsmemberships endpoint returns only active memberships,
            # so default status/is_active accordingly when the field is absent.
            raw_status = m.get("Status")
            rows.append(
                {
                    "mindbody_client_id": str(client_id),
                    "membership_id": membership_id,
                    "membership_name": str(m.get("Name", "") or ""),
                    "status": str(raw_status) if raw_status is not None else "Active",
                    "start_date": str(v) if (v := m.get("StartDate")) is not None else None,
                    "expiration_date": str(v) if (v := m.get("ExpirationDate")) is not None else None,
                    "is_active": raw_status == "Active" if raw_status is not None else True,
                    "last_synced_at": now,
                }
            )
    if not rows:
        return 0
    # asyncpg limits query args to 32,767; with 8 columns that's ~4,095 rows max.
    chunk_size = 2000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        stmt = insert(MindBodyMembership).values(chunk)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_mb_client_membership",
            set_={
                k: stmt.excluded[k]
                for k in chunk[0]
                if k not in ("mindbody_client_id", "membership_id")
            },
        )
        await db.execute(stmt)
    await db.commit()
    return len(rows)


async def load_windows(db: AsyncSession, client_ids: list[str]) -> dict[str, dict]:
    """Return a dict mapping client_id → {valid_start, valid_end} for active memberships."""
    result = await db.execute(
        select(
            MindBodyMembership.mindbody_client_id,
            func.min(MindBodyMembership.start_date).label("valid_start"),
            func.max(MindBodyMembership.expiration_date).label("valid_end"),
        )
        .where(
            MindBodyMembership.mindbody_client_id.in_(client_ids),
            MindBodyMembership.is_active.is_(True),
        )
        .group_by(MindBodyMembership.mindbody_client_id)
    )
    return {
        row.mindbody_client_id: {"valid_start": row.valid_start, "valid_end": row.valid_end}
        for row in result
    }
