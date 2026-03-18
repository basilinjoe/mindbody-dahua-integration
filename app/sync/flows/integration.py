from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from datetime import UTC, datetime
from uuid import uuid4

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact

from app.sync.flows.dahua_push import run_dahua_push
from app.sync.tasks import (
    _format_dahua_date,
    fetch_all_memberships,
    fetch_dahua_users_for_device,
    fetch_members,
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
    run_id = str(uuid4())
    run_started_at = datetime.now(UTC)
    flow_logger.info("Integration sync started (run_id=%s)", run_id)

    # ── Step 1: Fetch from MindBody ────────────────────────────────────────────
    all_members = await fetch_members()
    flow_logger.info("Fetched %d members from MindBody", len(all_members))

    id_counts = Counter(str(m["Id"]) for m in all_members if m.get("Id"))
    duplicate_ids = {mid: count for mid, count in id_counts.items() if count > 1}
    if duplicate_ids:
        flow_logger.warning("Duplicate mindbody_id values in API response: %s", duplicate_ids)
        deduped: dict[str, dict] = {}
        for m in all_members:
            mid = str(m.get("Id", ""))
            if not mid:
                continue
            existing = deduped.get(mid)
            if existing is None or (m.get("Active") and not existing.get("Active")):
                deduped[mid] = m
        all_members = list(deduped.values())
        flow_logger.info("After dedup: %d members", len(all_members))

    active_members_from_api = [m for m in all_members if m.get("Active")]
    flow_logger.info("%d active members after dedup", len(active_members_from_api))

    if not all_members:
        flow_logger.info("No members fetched from MindBody")
        return

    # Upsert ALL members (including inactive) so the DB reflects current active status.
    # Only fetch memberships for active members to avoid unnecessary API calls.
    active_client_ids = [str(m["Id"]) for m in active_members_from_api if m.get("Id")]
    _, memberships_by_client = await asyncio.gather(
        upsert_mindbody_users_batch(all_members),
        fetch_all_memberships(active_client_ids),
    )
    await upsert_mindbody_memberships_batch(memberships_by_client)
    flow_logger.info(
        "Upserted %d clients to local DB (%d active, memberships fetched for active)",
        len(all_members),
        len(active_members_from_api),
    )

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
        len(active_male_ids),
        len(active_female_ids),
    )

    # ── Step 3: Load devices + membership windows in parallel ──────────────────
    all_active_ids = list(member_map.keys())
    (male_device_ids, female_device_ids), membership_windows = await asyncio.gather(
        asyncio.gather(
            load_device_ids_by_gate_type("male"),
            load_device_ids_by_gate_type("female"),
        ),
        load_membership_windows(all_active_ids),
    )
    flow_logger.info(
        "Devices — male gates: %d, female gates: %d",
        len(male_device_ids),
        len(female_device_ids),
    )

    all_device_ids = list(dict.fromkeys(male_device_ids + female_device_ids))
    if not all_device_ids:
        flow_logger.warning(
            "No enabled devices with integration enabled found — nothing to push. "
            "Enable integration for a device via the admin UI (Devices → enable_integration)."
        )
        return

    # ── Step 4: Fetch live users from all Dahua devices in parallel ────────────
    dahua_users_results = await asyncio.gather(
        *[fetch_dahua_users_for_device(did) for did in all_device_ids],
        return_exceptions=True,
    )
    dahua_users_by_device: dict[int, list[dict]] = {}
    for device_id, result in zip(all_device_ids, dahua_users_results, strict=False):
        if isinstance(result, Exception):
            flow_logger.error("Failed to fetch users from device %d: %s", device_id, result)
            dahua_users_by_device[device_id] = []
        else:
            dahua_users_by_device[device_id] = result
            flow_logger.info("Device %d: found %d existing users on device", device_id, len(result))

    # ── Step 5: Plan operations for each device ────────────────────────────────
    all_items: list[dict] = []

    for gate_label, device_ids, active_ids in [
        ("male", male_device_ids, active_male_ids),
        ("female", female_device_ids, active_female_ids),
    ]:
        for device_id in device_ids:
            items = _plan_device_operations(
                device_id=device_id,
                active_member_ids=active_ids,
                member_map=member_map,
                dahua_users=dahua_users_by_device.get(device_id, []),
                membership_windows=membership_windows,
            )
            c = Counter(i["action"] for i in items)
            flow_logger.info(
                "Device %d (%s): enroll=%d deactivate=%d reactivate=%d update_window=%d",
                device_id, gate_label,
                c["enroll"], c["deactivate"], c["reactivate"], c["update_window"],
            )
            all_items.extend(items)

    total = Counter(i["action"] for i in all_items)
    flow_logger.info(
        "Total planned: %d operations — enroll=%d deactivate=%d reactivate=%d update_window=%d",
        len(all_items),
        total["enroll"], total["deactivate"], total["reactivate"], total["update_window"],
    )

    if not all_items:
        flow_logger.info("All devices already in sync — nothing to do")
        return

    # ── Step 6: Write queue + execute ─────────────────────────────────────────
    await write_sync_queue_batch(run_id, all_items)

    push_stats = await run_dahua_push(run_id, flow_logger)

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
    dahua_map: dict[str, dict] = {u["UserID"]: u for u in dahua_users if u.get("UserID")}

    items: list[dict] = []

    # ── Active members: enroll / reactivate / update_window ───────────────────
    for cid in active_member_ids:
        start_date, expiration_date = membership_windows.get(cid, (None, None))
        new_valid_start = _format_dahua_date(start_date)
        new_valid_end = _format_dahua_date(expiration_date)

        if cid not in dahua_map:
            # Not on device yet → enroll
            m = member_map.get(cid, {})
            items.append(
                {
                    "device_id": device_id,
                    "mindbody_client_id": cid,
                    "action": "enroll",
                    "member_snapshot": json.dumps(
                        {
                            "Id": cid,
                            "FirstName": m.get("FirstName", ""),
                            "LastName": m.get("LastName", ""),
                            "Gender": m.get("Gender"),
                            "Email": m.get("Email"),
                            "valid_start": new_valid_start,
                            "valid_end": new_valid_end,
                        }
                    ),
                    "dahua_user_id": None,
                    "enrollment_id": None,
                }
            )

        else:
            dahua_user = dahua_map[cid]
            card_status = str(dahua_user.get("CardStatus", "0"))

            if card_status == "4":
                # Frozen on device but membership is active → reactivate
                items.append(
                    {
                        "device_id": device_id,
                        "mindbody_client_id": cid,
                        "action": "reactivate",
                        "member_snapshot": None,
                        "dahua_user_id": cid,
                        "enrollment_id": None,
                    }
                )

            else:
                # Active on device — check if access window needs updating
                current_valid_end = dahua_user.get("ValidDateEnd") or ""
                current_valid_start = dahua_user.get("ValidDateStart") or ""
                if new_valid_end and (
                    new_valid_end != current_valid_end
                    or (new_valid_start and new_valid_start != current_valid_start)
                ):
                    items.append(
                        {
                            "device_id": device_id,
                            "mindbody_client_id": cid,
                            "action": "update_window",
                            "member_snapshot": json.dumps(
                                {
                                    "valid_start": new_valid_start,
                                    "valid_end": new_valid_end,
                                }
                            ),
                            "dahua_user_id": cid,
                            "enrollment_id": None,
                        }
                    )

    # ── Users on device not in active set → deactivate ────────────────────────
    for user_id, dahua_user in dahua_map.items():
        if user_id in active_member_ids:
            continue  # handled above
        card_status = str(dahua_user.get("CardStatus", "0"))
        if card_status != "4":  # only act if not already frozen
            items.append(
                {
                    "device_id": device_id,
                    "mindbody_client_id": user_id,
                    "action": "deactivate",
                    "member_snapshot": None,
                    "dahua_user_id": user_id,
                    "enrollment_id": None,
                }
            )

    return items
