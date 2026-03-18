from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.models.mindbody_client import MindBodyClient

logger = logging.getLogger(__name__)

_FIELDS = (
    "unique_id",
    "first_name",
    "last_name",
    "email",
    "mobile_phone",
    "home_phone",
    "work_phone",
    "status",
    "active",
    "birth_date",
    "gender",
    "created_at_mb",
    "last_modified_at_mb",
)


def _row_from_dict(c: dict, now: datetime) -> dict:
    return {
        "unique_id": c.get("UniqueId"),
        "first_name": c.get("FirstName", ""),
        "last_name": c.get("LastName", ""),
        "email": c.get("Email"),
        "mobile_phone": c.get("MobilePhone"),
        "home_phone": c.get("HomePhone"),
        "work_phone": c.get("WorkPhone"),
        "status": c.get("Status"),
        "active": bool(c.get("Active", False)),
        "birth_date": c.get("BirthDate"),
        "gender": c.get("Gender"),
        "created_at_mb": c.get("CreationDate"),
        "last_modified_at_mb": c.get("LastModifiedDateTime"),
        "last_fetched_at": now,
    }


def refresh_mindbody_clients(db: Session, clients: list[dict]) -> None:
    """Full replace: delete everything, then insert the fresh batch."""
    db.query(MindBodyClient).delete()
    db.flush()

    now = datetime.now(UTC)
    seen_ids: set[str] = set()
    for c in clients:
        mid = str(c.get("Id", "")).strip()
        if not mid or mid in seen_ids:
            continue
        seen_ids.add(mid)
        db.add(MindBodyClient(mindbody_id=mid, **_row_from_dict(c, now)))

    logger.info("Full refresh: %d records inserted into mindbody_clients", len(seen_ids))


def upsert_mindbody_clients(db: Session, clients: list[dict]) -> tuple[int, int]:
    """Incremental upsert: update existing rows, insert new ones.

    Returns (updated, inserted) counts.
    """
    now = datetime.now(UTC)
    seen_ids: set[str] = set()
    updated = inserted = 0

    for c in clients:
        mid = str(c.get("Id", "")).strip()
        if not mid or mid in seen_ids:
            continue
        seen_ids.add(mid)

        fields = _row_from_dict(c, now)
        existing = db.query(MindBodyClient).filter_by(mindbody_id=mid).first()
        if existing:
            for key, val in fields.items():
                setattr(existing, key, val)
            updated += 1
        else:
            db.add(MindBodyClient(mindbody_id=mid, **fields))
            inserted += 1

    logger.info("Incremental refresh: %d updated, %d inserted", updated, inserted)
    return updated, inserted


def get_last_fetched_at(db: Session) -> datetime | None:
    """Return the most recent last_fetched_at timestamp from the local cache."""
    result = (
        db.query(MindBodyClient.last_fetched_at)
        .order_by(MindBodyClient.last_fetched_at.desc())
        .first()
    )
    return result[0] if result else None
