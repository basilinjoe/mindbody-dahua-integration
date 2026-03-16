from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from app.sync.flows.dahua_push import sync_dahua_push_flow
from app.sync.flows.mindbody_memberships import sync_mindbody_memberships_flow
from app.sync.flows.mindbody_users import sync_mindbody_users_flow
from app.sync.tasks import (
    _format_dahua_date,
    fetch_members,
    get_active_member_ids,
    load_device_ids_by_gate_type,
    load_enrollments_for_device,
    load_membership_windows,
    write_sync_queue_batch,
)

logger = logging.getLogger(__name__)


@flow(name="sync-integration", log_prints=True)
async def sync_integration_flow(
    force_full: bool = False,
    sync_type: str = "scheduled",
) -> None:
    """
    Main integration flow:
    1. Persist MindBody user + membership details (subflows)
    2. Fetch MindBody members (incremental or full)
    3. Classify by gender, load devices, check memberships
    4. Compute enroll/deactivate/reactivate operations → write to dahua_sync_queue
    5. Execute operations against Dahua devices (subflow)
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

    # 2. Persist user + membership details to DB (subflows)
    await sync_mindbody_users_flow(modified_after=modified_after)
    await sync_mindbody_memberships_flow(modified_after=modified_after)

    # 3. Fetch members
    members = await fetch_members(modified_after=modified_after)
    flow_logger.info("Fetched %d members from MindBody", len(members))

    if not members:
        flow_logger.info("No members to process")
        await Variable.set("last_sync_at", run_started_at.isoformat(), overwrite=True)
        return

    # 4. Classify by gender
    male_members = [m for m in members if (m.get("Gender") or "").lower() == "male"]
    female_members = [m for m in members if (m.get("Gender") or "").lower() == "female"]
    flow_logger.info("Classified: %d male, %d female", len(male_members), len(female_members))

    # 5. Load devices by gate_type
    male_device_ids = await load_device_ids_by_gate_type("male")
    female_device_ids = await load_device_ids_by_gate_type("female")
    flow_logger.info(
        "Devices: %d male gates, %d female gates",
        len(male_device_ids),
        len(female_device_ids),
    )

    # 6. Check active membership for all fetched member IDs
    all_ids = [str(m.get("Id", "")) for m in members if m.get("Id")]
    active_ids = await get_active_member_ids(all_ids)
    flow_logger.info("%d / %d members have active membership", len(active_ids), len(all_ids))

    # 7. Plan operations (no Dahua calls yet)
    run_id = str(uuid4())
    male_items = await _plan_group(male_members, male_device_ids, active_ids, flow_logger)
    female_items = await _plan_group(female_members, female_device_ids, active_ids, flow_logger)
    all_items = male_items + female_items

    flow_logger.info(
        "Planned %d operations (run_id=%s): %d enroll, %d deactivate, %d reactivate",
        len(all_items),
        run_id,
        sum(1 for i in all_items if i["action"] == "enroll"),
        sum(1 for i in all_items if i["action"] == "deactivate"),
        sum(1 for i in all_items if i["action"] == "reactivate"),
    )

    # 8. Persist the plan to dahua_sync_queue
    await write_sync_queue_batch(run_id, all_items)

    # 9. Execute the plan against Dahua devices
    push_stats = await sync_dahua_push_flow(run_id=run_id, photo_max_kb=photo_max_kb)

    # 10. Save last sync timestamp
    await Variable.set("last_sync_at", run_started_at.isoformat(), overwrite=True)

    # 11. Publish artifact
    await create_table_artifact(
        key="sync-results",
        table=[
            {"metric": "enrolled", "count": push_stats.get("enrolled", 0)},
            {"metric": "deactivated", "count": push_stats.get("deactivated", 0)},
            {"metric": "reactivated", "count": push_stats.get("reactivated", 0)},
            {"metric": "failed", "count": push_stats.get("failed", 0)},
        ],
        description=(
            f"## {'Full' if force_full else 'Incremental'} Sync — "
            f"{run_started_at.strftime('%Y-%m-%d %H:%M')} UTC  \n"
            f"run_id: `{run_id}`"
        ),
    )

    flow_logger.info("Sync complete — %s", push_stats)


async def _plan_group(
    group_members: list[dict],
    device_ids: list[int],
    active_ids: set[str],
    flow_logger,
) -> list[dict]:
    """
    Compute the set of enroll/deactivate/reactivate/update_window operations needed
    for a gender group. Returns a list of queue item dicts — does NOT execute any
    Dahua operations.
    """
    if not device_ids:
        return []

    active_group_ids = {str(m.get("Id", "")) for m in group_members if str(m.get("Id", "")) in active_ids}
    member_map = {str(m.get("Id", "")): m for m in group_members}

    # Load membership windows for all group members in a single DB query
    all_client_ids = list({str(m.get("Id", "")) for m in group_members if m.get("Id")})
    membership_windows = await load_membership_windows(all_client_ids)

    items: list[dict] = []

    for device_id in device_ids:
        device_enrollments = await load_enrollments_for_device(device_id)

        to_enroll = [member_map[cid] for cid in active_group_ids if cid not in device_enrollments]
        to_deactivate = [
            e for cid, e in device_enrollments.items() if cid not in active_ids and e.is_active
        ]
        to_reactivate = [
            e for cid, e in device_enrollments.items()
            if cid in active_group_ids and not e.is_active
        ]

        # Detect stale access windows for existing active enrollments
        to_update_window = []
        for cid, enrollment in device_enrollments.items():
            if cid not in active_group_ids or not enrollment.is_active:
                continue  # handled by deactivate/reactivate logic
            _, expiration_date = membership_windows.get(cid, (None, None))
            new_valid_end = _format_dahua_date(expiration_date)
            if enrollment.valid_end != new_valid_end:
                to_update_window.append((cid, enrollment))

        flow_logger.info(
            "Device %d: enroll=%d deactivate=%d reactivate=%d update_window=%d",
            device_id, len(to_enroll), len(to_deactivate), len(to_reactivate), len(to_update_window),
        )

        for m in to_enroll:
            cid = str(m.get("Id", ""))
            start_date, expiration_date = membership_windows.get(cid, (None, None))
            snapshot = json.dumps({
                "Id": m.get("Id"),
                "FirstName": m.get("FirstName", ""),
                "LastName": m.get("LastName", ""),
                "Gender": m.get("Gender"),
                "PhotoUrl": m.get("PhotoUrl"),
                "Email": m.get("Email"),
                "valid_start": _format_dahua_date(start_date),
                "valid_end": _format_dahua_date(expiration_date),
            })
            items.append({
                "device_id": device_id,
                "mindbody_client_id": cid,
                "action": "enroll",
                "member_snapshot": snapshot,
                "dahua_user_id": None,
                "enrollment_id": None,
            })

        for e in to_deactivate:
            items.append({
                "device_id": device_id,
                "mindbody_client_id": e.dahua_user_id,
                "action": "deactivate",
                "member_snapshot": None,
                "dahua_user_id": e.dahua_user_id,
                "enrollment_id": e.id,
            })

        for e in to_reactivate:
            items.append({
                "device_id": device_id,
                "mindbody_client_id": e.dahua_user_id,
                "action": "reactivate",
                "member_snapshot": None,
                "dahua_user_id": e.dahua_user_id,
                "enrollment_id": e.id,
            })

        for cid, enrollment in to_update_window:
            start_date, expiration_date = membership_windows.get(cid, (None, None))
            items.append({
                "device_id": device_id,
                "mindbody_client_id": cid,
                "action": "update_window",
                "member_snapshot": json.dumps({
                    "valid_start": _format_dahua_date(start_date),
                    "valid_end": _format_dahua_date(expiration_date),
                }),
                "dahua_user_id": enrollment.dahua_user_id,
                "enrollment_id": enrollment.id,
            })

    return items
