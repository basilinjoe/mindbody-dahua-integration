from __future__ import annotations

import logging
from datetime import datetime, timezone

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from app.sync.tasks import (
    fetch_all_memberships,
    fetch_members,
    upsert_mindbody_memberships_batch,
)

logger = logging.getLogger(__name__)


@flow(name="sync-mindbody-memberships", log_prints=True)
async def sync_mindbody_memberships_flow(modified_after: datetime | None = None) -> int:
    """
    Fetch active memberships for all MindBody clients and upsert into the
    mindbody_memberships table. Returns total membership rows upserted.
    """
    flow_logger = get_run_logger()
    scope = "incremental" if modified_after else "full"
    flow_logger.info("MindBody membership sync started (scope=%s)", scope)

    # fetch_members is cached by INPUTS — if called with the same modified_after
    # value in the same flow run context it will return a cached result.
    members = await fetch_members(modified_after=modified_after)
    flow_logger.info("Fetched %d members from MindBody", len(members))

    client_ids = [str(m["Id"]) for m in members if m.get("Id")]
    if not client_ids:
        flow_logger.info("No client IDs to process")
        return 0

    flow_logger.info("Fetching memberships for %d clients", len(client_ids))
    memberships_by_client = await fetch_all_memberships(client_ids)

    total = await upsert_mindbody_memberships_batch(memberships_by_client)
    flow_logger.info("Upserted %d membership rows into mindbody_memberships", total)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    await create_markdown_artifact(
        key="mindbody-membership-sync",
        markdown=(
            f"## MindBody Membership Sync — {timestamp} UTC\n"
            f"- Scope: {scope}\n"
            f"- Clients processed: {len(client_ids)}\n"
            f"- Membership rows upserted: {total}"
        ),
    )

    return total
