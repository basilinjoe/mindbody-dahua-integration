from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.mindbody_client import MindBodyClient

logger = logging.getLogger(__name__)


def refresh_mindbody_clients(db: Session, clients: list[dict]) -> None:
    """Replace the local mindbody_clients cache with a fresh batch from MindBody.

    Deletes all existing rows, then inserts the new records.  Duplicate IDs
    within the batch (which the MindBody API occasionally returns) are silently
    skipped so the unique constraint is never violated.
    """
    db.query(MindBodyClient).delete()
    db.flush()

    now = datetime.now(timezone.utc)
    seen_ids: set[str] = set()
    for c in clients:
        mid = str(c.get("Id", "")).strip()
        if not mid or mid in seen_ids:
            continue
        seen_ids.add(mid)
        db.add(MindBodyClient(
            mindbody_id=mid,
            unique_id=c.get("UniqueId"),
            first_name=c.get("FirstName", ""),
            last_name=c.get("LastName", ""),
            email=c.get("Email"),
            mobile_phone=c.get("MobilePhone"),
            home_phone=c.get("HomePhone"),
            work_phone=c.get("WorkPhone"),
            status=c.get("Status"),
            active=bool(c.get("Active", False)),
            birth_date=c.get("BirthDate"),
            gender=c.get("Gender"),
            created_at_mb=c.get("CreationDate"),
            last_modified_at_mb=c.get("LastModifiedDateTime"),
            photo_url=c.get("PhotoUrl"),
            last_fetched_at=now,
        ))

    logger.info("Refreshed mindbody_clients table with %d records", len(seen_ids))
