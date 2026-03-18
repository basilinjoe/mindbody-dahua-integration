from __future__ import annotations

import logging
from collections import Counter
from datetime import UTC, datetime

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from app.sync.tasks import fetch_members, upsert_mindbody_users_batch

logger = logging.getLogger(__name__)


@flow(name="sync-mindbody-users", log_prints=True)
async def sync_mindbody_users_flow(modified_after: datetime | None = None) -> int:
    """
    Fetch MindBody client details and upsert into the mindbody_clients table.
    Returns count of rows upserted.
    """
    flow_logger = get_run_logger()
    scope = "incremental" if modified_after else "full"
    flow_logger.info("MindBody user sync started (scope=%s)", scope)

    members = await fetch_members(modified_after=modified_after)
    flow_logger.info("Fetched %d members from MindBody", len(members))

    id_counts = Counter(str(m["Id"]) for m in members if m.get("Id"))
    duplicate_ids = {mid: count for mid, count in id_counts.items() if count > 1}
    if duplicate_ids:
        flow_logger.warning("Duplicate mindbody_id values in API response: %s", duplicate_ids)
        deduped: dict[str, dict] = {}
        for m in members:
            mid = str(m.get("Id", ""))
            if not mid:
                continue
            existing = deduped.get(mid)
            if existing is None or (m.get("Active") and not existing.get("Active")):
                deduped[mid] = m
        members = list(deduped.values())
        flow_logger.info("After dedup: %d members", len(members))

    if not members:
        flow_logger.info("No members to upsert")
        return 0

    count = await upsert_mindbody_users_batch(members)
    flow_logger.info("Upserted %d user records into mindbody_clients", count)

    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
    await create_markdown_artifact(
        key="mindbody-user-sync",
        markdown=f"## MindBody User Sync — {timestamp} UTC\n- Scope: {scope}\n- Upserted: {count} users",
    )

    return count
