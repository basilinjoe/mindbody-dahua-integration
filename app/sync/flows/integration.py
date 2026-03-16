from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from app.sync.flows.dahua_push import run_dahua_push
from app.sync.tasks import (
    _format_dahua_date,
    fetch_all_memberships,
    fetch_members,
    fetch_dahua_users_for_device,
    load_active_members_from_db,
    load_device_ids_by_gate_type,
    load_membership_windows,
    upsert_mindbody_memberships_batch,
    upsert_mindbody_users_batch,
    write_sync_queue_batch,
)

logger = logging.getLogger(__name__)


@flow(name="sync-integration", log_prints=True)
async def sync_integration_flow(sync_type: str = "scheduled") -> None:
    """
    Main integration flow:
    1. Fetch all MindBody clients + memberships in parallel → upsert to local DB
    2. Query local DB for active members with active memberships, classified by gender
    3. Fetch live user records from each Dahua device and compare against active members
       (UserID on Dahua device == mindbody_id)
    4. Compute enroll / deactivate / reactivate / update_window operations
    5. Write operations to dahua_sync_queue and execute against devices
    """
    flow_logger = get_run_logger()
    photo_max_kb = int(await Variable.get("photo_max_size_kb", default="200"))
    run_id = str(uuid4())
    run_started_at = datetime.now(timezone.utc)
    flow_logger.info("Integration sync started (run_id=%s)", run_id)

    # ── Step 1: Fetch from MindBody ────────────────────────────────────────────
    members = await fetch_members(modified_after=None)
    flow_logger.info("Fetched %d members from MindBody", len(members))

    if not members:
        flow_logger.info("No members to process")
        return

    client_ids = [str(m["Id"]) for m in members if m.get("Id")]

    # Upsert users + fetch memberships in parallel, then upsert memberships
    _, memberships_by_client = await asyncio.gather(
        upsert_mindbody_users_batch(members),
        fetch_all_memberships(client_ids),
    )
    await upsert_mindbody_memberships_batch(memberships_by_client)
    flow_logger.info("Upserted %d clients and their memberships to local DB", len(members))

    # ── Step 2: Load active candidates from DB ─────────────────────────────────
    # Only members marked active in MindBody AND with an active membership qualify.
    active_members = await load_active_members_from_db()
    flow_logger.info("Active members with valid membership: %d", len(active_members))

    if not active_members:
        flow_logger.info("No active members — nothing to push")
        return

    active_male_ids: set[str] = {
        m["Id"] for m in active_members if (m.get("Gender") or "").lower() == "male"
    }
    active_female_ids: set[str] = {
        m["Id"] for m in active_members if (m.get("Gender") or "").lower() == "female"
    }
    member_map: dict[str, dict] = {m["Id"]: m for m in active_members}
    flow_logger.info(
        "Classified: %d active male, %d active female",
        len(active_male_ids), len(active_female_ids),
    )

    # ── Step 3: Load devices + membership windows in parallel ──────────────────
    all_active_ids = list(active_male_ids | active_female_ids)
    (male_device_ids, female_device_ids), membership_windows = await asyncio.gather(
        asyncio.gather(
            load_device_ids_by_gate_type("male"),
            load_device_ids_by_gate_type("female"),
        ),
        load_membership_windows(all_active_ids),
    )
    flow_logger.info(
        "Devices — male gates: %d, female gates: %d",
        len(male_device_ids), len(female_device_ids),
    )

    all_device_ids = list(dict.fromkeys(male_device_ids + female_device_ids))
    if not all_device_ids:
        flow_logger.warning("No enabled devices found — nothing to push")
        return

    # ── Step 4: Fetch live users from all Dahua devices in parallel ────────────
    dahua_users_results = await asyncio.gather(
        *[fetch_dahua_users_for_device(did) for did in all_device_ids],
        return_exceptions=True,
    )
    dahua_users_by_device: dict[int, list[dict]] = {}
    for device_id, result in zip(all_device_ids, dahua_users_results):
        if isinstance(result, Exception):
            flow_logger.error(
                "Failed to fetch users from device %d: %s", device_id, result
            )
            dahua_users_by_device[device_id] = []
        else:
            dahua_users_by_device[device_id] = result
            flow_logger.info(
                "Device %d: found %d existing users on device", device_id, len(result)
            )

    # ── Step 5: Plan operations for each device ────────────────────────────────
    all_items: list[dict] = []

    for device_id in male_device_ids:
        items = _plan_device_operations(
            device_id=device_id,
            active_member_ids=active_male_ids,
            member_map=member_map,
            dahua_users=dahua_users_by_device.get(device_id, []),
            membership_windows=membership_windows,
        )
        flow_logger.info(
            "Device %d (male): enroll=%d deactivate=%d reactivate=%d update_window=%d",
            device_id,
            sum(1 for i in items if i["action"] == "enroll"),
            sum(1 for i in items if i["action"] == "deactivate"),
            sum(1 for i in items if i["action"] == "reactivate"),
            sum(1 for i in items if i["action"] == "update_window"),
        )
        all_items.extend(items)

    for device_id in female_device_ids:
        items = _plan_device_operations(
            device_id=device_id,
            active_member_ids=active_female_ids,
            member_map=member_map,
            dahua_users=dahua_users_by_device.get(device_id, []),
            membership_windows=membership_windows,
        )
        flow_logger.info(
            "Device %d (female): enroll=%d deactivate=%d reactivate=%d update_window=%d",
            device_id,
            sum(1 for i in items if i["action"] == "enroll"),
            sum(1 for i in items if i["action"] == "deactivate"),
            sum(1 for i in items if i["action"] == "reactivate"),
            sum(1 for i in items if i["action"] == "update_window"),
        )
        all_items.extend(items)

    flow_logger.info(
        "Total planned: %d operations — enroll=%d deactivate=%d reactivate=%d update_window=%d",
        len(all_items),
        sum(1 for i in all_items if i["action"] == "enroll"),
        sum(1 for i in all_items if i["action"] == "deactivate"),
        sum(1 for i in all_items if i["action"] == "reactivate"),
        sum(1 for i in all_items if i["action"] == "update_window"),
    )

    if not all_items:
        flow_logger.info("All devices already in sync — nothing to do")
        return

    # ── Step 6: Write queue + execute ─────────────────────────────────────────
    await write_sync_queue_batch(run_id, all_items)

    push_stats = await run_dahua_push(run_id, photo_max_kb, flow_logger)

    await create_table_artifact(
        key="sync-results",
        table=[
            {"metric": "enrolled", "count": push_stats.get("enrolled", 0)},
            {"metric": "deactivated", "count": push_stats.get("deactivated", 0)},
            {"metric": "reactivated", "count": push_stats.get("reactivated", 0)},
            {"metric": "window_updated", "count": push_stats.get("window_updated", 0)},
            {"metric": "failed", "count": push_stats.get("failed", 0)},
        ],
        description=(
            f"## Integration Sync — {run_started_at.strftime('%Y-%m-%d %H:%M')} UTC\n"
            f"run_id: `{run_id}`"
        ),
    )
    flow_logger.info("Sync complete — %s", push_stats)


def _plan_device_operations(
    device_id: int,
    active_member_ids: set[str],
    member_map: dict[str, dict],
    dahua_users: list[dict],
    membership_windows: dict[str, tuple[str | None, str | None]],
) -> list[dict]:
    """
    Compare active MindBody members against live Dahua device records.
    UserID on the Dahua device is expected to equal the MindBody client ID.

    Returns a list of queue item dicts (action ∈ enroll|deactivate|reactivate|update_window).
    Does NOT execute any device operations.
    """
    # Build a map of UserID → Dahua user record for O(1) lookups
    dahua_map: dict[str, dict] = {
        u["UserID"]: u for u in dahua_users if u.get("UserID")
    }

    items: list[dict] = []

    # ── Active members: enroll / reactivate / update_window ───────────────────
    for cid in active_member_ids:
        start_date, expiration_date = membership_windows.get(cid, (None, None))
        new_valid_start = _format_dahua_date(start_date)
        new_valid_end = _format_dahua_date(expiration_date)

        if cid not in dahua_map:
            # Not on device yet → enroll
            m = member_map.get(cid, {})
            items.append({
                "device_id": device_id,
                "mindbody_client_id": cid,
                "action": "enroll",
                "member_snapshot": json.dumps({
                    "Id": cid,
                    "FirstName": m.get("FirstName", ""),
                    "LastName": m.get("LastName", ""),
                    "Gender": m.get("Gender"),
                    "PhotoUrl": m.get("PhotoUrl"),
                    "Email": m.get("Email"),
                    "valid_start": new_valid_start,
                    "valid_end": new_valid_end,
                }),
                "dahua_user_id": None,
                "enrollment_id": None,
            })

        else:
            dahua_user = dahua_map[cid]
            card_status = str(dahua_user.get("CardStatus", "0"))

            if card_status == "4":
                # Frozen on device but membership is active → reactivate
                items.append({
                    "device_id": device_id,
                    "mindbody_client_id": cid,
                    "action": "reactivate",
                    "member_snapshot": None,
                    "dahua_user_id": cid,
                    "enrollment_id": None,
                })

            else:
                # Active on device — check if access window needs updating
                current_valid_end = dahua_user.get("ValidDateEnd") or ""
                current_valid_start = dahua_user.get("ValidDateStart") or ""
                if new_valid_end and (
                    new_valid_end != current_valid_end
                    or (new_valid_start and new_valid_start != current_valid_start)
                ):
                    items.append({
                        "device_id": device_id,
                        "mindbody_client_id": cid,
                        "action": "update_window",
                        "member_snapshot": json.dumps({
                            "valid_start": new_valid_start,
                            "valid_end": new_valid_end,
                        }),
                        "dahua_user_id": cid,
                        "enrollment_id": None,
                    })

    # ── Users on device not in active set → deactivate ────────────────────────
    for user_id, dahua_user in dahua_map.items():
        if user_id in active_member_ids:
            continue  # handled above
        card_status = str(dahua_user.get("CardStatus", "0"))
        if card_status != "4":  # only act if not already frozen
            items.append({
                "device_id": device_id,
                "mindbody_client_id": user_id,
                "action": "deactivate",
                "member_snapshot": None,
                "dahua_user_id": user_id,
                "enrollment_id": None,
            })

    return items
