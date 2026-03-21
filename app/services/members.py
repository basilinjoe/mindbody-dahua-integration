from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import cast, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import DateTime

from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership


async def upsert_batch(db: AsyncSession, members: list[dict]) -> int:
    """Upsert a batch of raw MindBody member dicts. Returns number of rows written."""
    now = datetime.now(UTC)
    seen: set[str] = set()
    rows = []
    for m in members:
        mid = str(m.get("Id", "")).strip()
        if not mid or mid in seen:
            continue
        seen.add(mid)
        rows.append(
            {
                "mindbody_id": mid,
                "unique_id": m.get("UniqueId"),
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
                "last_fetched_at": now,
            }
        )
    if not rows:
        return 0
    stmt = insert(MindBodyClient).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["mindbody_id"],
        set_={k: stmt.excluded[k] for k in rows[0] if k != "mindbody_id"},
    )
    await db.execute(stmt)
    await db.commit()
    return len(rows)


async def load_active(db: AsyncSession) -> list[MindBodyClient]:
    """Return all MindBodyClient rows that are active and have at least one active membership."""
    result = await db.execute(
        select(MindBodyClient)
        .join(
            MindBodyMembership,
            MindBodyClient.mindbody_id == MindBodyMembership.mindbody_client_id,
        )
        .where(
            MindBodyClient.active.is_(True),
            MindBodyMembership.status == "Active",
            or_(
                MindBodyMembership.expiration_date.is_(None),
                cast(MindBodyMembership.expiration_date, DateTime(timezone=True))
                > datetime.now(UTC),
            ),
        )
        .distinct()
    )
    return list(result.scalars().all())


async def get_last_fetched_at(db: AsyncSession) -> datetime | None:
    """Return the most recent last_fetched_at timestamp across all member rows."""
    result = await db.execute(select(func.max(MindBodyClient.last_fetched_at)))
    return result.scalar_one_or_none()
