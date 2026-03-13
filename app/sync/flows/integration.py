from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from app.sync.tasks import (
    deactivate_on_device,
    enroll_on_device,
    fetch_members,
    get_active_member_ids,
    load_device_ids_by_gate_type,
    load_enrollments_for_device,
    reactivate_on_device,
)

logger = logging.getLogger(__name__)


@flow(name="sync-integration", log_prints=True)
async def sync_integration_flow(
    force_full: bool = False,
    sync_type: str = "scheduled",
) -> None:
    """
    Main integration flow:
    1. Fetch MindBody members (incremental or full)
    2. Classify by gender
    3. Load matching devices by gate_type
    4. Enroll/deactivate/reactivate on each device independently

    Ensures every active male member is on every male device, and
    every active female member is on every female device.
    """
    flow_logger = get_run_logger()
    photo_max_kb = int(await Variable.get("photo_max_size_kb", default="200"))

    # 1. Determine fetch window
    last_sync_str = await Variable.get("last_sync_at", default=None)
    modified_after = None
    if not force_full and last_sync_str:
        try:
            modified_after = datetime.fromisoformat(last_sync_str)
        except ValueError:
            flow_logger.warning("Invalid last_sync_at value: %s, doing full fetch", last_sync_str)

    run_started_at = datetime.now(timezone.utc)
    flow_logger.info(
        "%s sync started (modified_after=%s)",
        "Full" if force_full else "Incremental",
        modified_after,
    )

    # 2. Fetch members
    members = await fetch_members(modified_after=modified_after)
    flow_logger.info("Fetched %d members from MindBody", len(members))

    if not members:
        flow_logger.info("No members to process")
        await Variable.set("last_sync_at", run_started_at.isoformat())
        return

    # 3. Classify by gender
    male_members = [m for m in members if (m.get("Gender") or "").lower() == "male"]
    female_members = [m for m in members if (m.get("Gender") or "").lower() == "female"]
    flow_logger.info("Classified: %d male, %d female", len(male_members), len(female_members))

    # 4. Load devices by gate_type
    male_device_ids = await load_device_ids_by_gate_type("male")
    female_device_ids = await load_device_ids_by_gate_type("female")
    flow_logger.info(
        "Devices: %d male gates, %d female gates",
        len(male_device_ids),
        len(female_device_ids),
    )

    # 5. Check active membership for all fetched member IDs
    all_ids = [str(m.get("Id", "")) for m in members if m.get("Id")]
    active_ids = await get_active_member_ids(all_ids)
    flow_logger.info("%d / %d members have active membership", len(active_ids), len(all_ids))

    # 6. Process each gender group on its devices
    male_stats = await _process_group(male_members, male_device_ids, active_ids, photo_max_kb, flow_logger)
    female_stats = await _process_group(female_members, female_device_ids, active_ids, photo_max_kb, flow_logger)

    # 7. Save last sync timestamp
    await Variable.set("last_sync_at", run_started_at.isoformat())

    # 8. Publish artifact
    await create_table_artifact(
        key="sync-results",
        table=[
            {"gate_type": "male", **male_stats},
            {"gate_type": "female", **female_stats},
        ],
        description=(
            f"## {'Full' if force_full else 'Incremental'} Sync — "
            f"{run_started_at.strftime('%Y-%m-%d %H:%M')} UTC"
        ),
    )

    flow_logger.info(
        "Sync complete — male: %s | female: %s",
        male_stats,
        female_stats,
    )


async def _process_group(
    group_members: list[dict],
    device_ids: list[int],
    active_ids: set[str],
    photo_max_kb: int,
    flow_logger,
) -> dict:
    """
    Ensure every active member in the group is enrolled on EVERY device in device_ids.
    Processes each device independently — newly added devices auto-catch-up.
    """
    if not device_ids:
        return {"enrolled": 0, "deactivated": 0, "reactivated": 0}

    active_group_ids = {str(m.get("Id", "")) for m in group_members if str(m.get("Id", "")) in active_ids}
    member_map = {str(m.get("Id", "")): m for m in group_members}

    totals = {"enrolled": 0, "deactivated": 0, "reactivated": 0}

    for device_id in device_ids:
        device_enrollments = await load_enrollments_for_device(device_id)
        # device_enrollments: dict[mindbody_client_id, MemberDeviceEnrollment]

        to_enroll = [member_map[cid] for cid in active_group_ids if cid not in device_enrollments]
        to_deactivate = [
            e for cid, e in device_enrollments.items() if cid not in active_ids and e.is_active
        ]
        to_reactivate = [
            e for cid, e in device_enrollments.items()
            if cid in active_group_ids and not e.is_active
        ]

        flow_logger.info(
            "Device %d: enroll=%d deactivate=%d reactivate=%d",
            device_id, len(to_enroll), len(to_deactivate), len(to_reactivate),
        )

        # Execute in parallel within device
        if to_enroll:
            await asyncio.gather(
                *[enroll_on_device(device_id, m, photo_max_kb) for m in to_enroll],
                return_exceptions=True,
            )
        if to_deactivate:
            await asyncio.gather(
                *[deactivate_on_device(device_id, e.dahua_user_id, e.id) for e in to_deactivate],
                return_exceptions=True,
            )
        if to_reactivate:
            await asyncio.gather(
                *[reactivate_on_device(device_id, e.dahua_user_id, e.id) for e in to_reactivate],
                return_exceptions=True,
            )

        totals["enrolled"] += len(to_enroll)
        totals["deactivated"] += len(to_deactivate)
        totals["reactivated"] += len(to_reactivate)

    return totals
