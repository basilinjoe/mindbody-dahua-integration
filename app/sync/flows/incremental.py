from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from datetime import UTC, datetime, timedelta

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.runtime import flow_run

from app.models.database import ensure_timestamps_tz
from app.sync.flows.dahua_push import run_dahua_push
from app.sync.tasks import (
    _format_dahua_date,
    _make_dahua_user_id,
    advance_watermark,
    archive_previous_sync_queue,
    fetch_all_memberships,
    fetch_dahua_users_for_device,
    fetch_members,
    load_active_members_by_ids,
    load_device_ids_by_gate_type,
    load_last_fetched_at,
    load_membership_windows,
    upsert_mindbody_memberships_batch,
    upsert_mindbody_users_batch,
    write_sync_queue_batch,
)

logger = logging.getLogger(__name__)

# Safety margin subtracted from the watermark to handle clock skew
_WATERMARK_MARGIN = timedelta(minutes=2)


@flow(name="sync-incremental", log_prints=True)
async def sync_incremental_flow(sync_type: str = "scheduled") -> None:
    """
    Incremental sync flow — runs frequently (every 5 min by default).
    1. Fetch only MindBody clients modified since the last run
    2. Upsert changed members + their memberships to local DB
    3. For active changed members: enroll / reactivate / update on Dahua devices
    4. Does NOT handle deactivation — the daily full sync covers that
    """
    flow_logger = get_run_logger()
    run_id = str(flow_run.id)
    run_started_at = datetime.now(UTC)
    flow_logger.info(
        "Incremental sync started (run_id=%s, sync_type=%s, ts=%s)",
        run_id,
        sync_type,
        run_started_at.isoformat(),
    )

    await ensure_timestamps_tz()

    # ── Step 0: Archive previous incremental queue runs ───────────────────────
    try:
        archived = await archive_previous_sync_queue(run_id, flow_type="incremental")
        if archived:
            flow_logger.info("Archived %d incremental queue items from previous runs", archived)
    except Exception:
        flow_logger.warning("Failed to archive previous incremental queue runs", exc_info=True)

    # ── Step 1: Load watermark ────────────────────────────────────────────────
    last_fetched_at = await load_last_fetched_at()
    if last_fetched_at is None:
        flow_logger.warning(
            "No previous fetch recorded — incremental sync requires at least one "
            "full sync to have run. Skipping."
        )
        return

    modified_since = last_fetched_at - _WATERMARK_MARGIN
    flow_logger.info("Fetching members modified since %s", modified_since.isoformat())

    # ── Step 2: Fetch changed members from MindBody ───────────────────────────
    changed_members = await fetch_members(modified_since=modified_since)
    flow_logger.info("Fetched %d changed members from MindBody", len(changed_members))

    if not changed_members:
        flow_logger.info("No changes since last run — nothing to do")
        return

    # Deduplicate (prefer active over inactive for duplicate IDs)
    id_counts = Counter(str(m["Id"]) for m in changed_members if m.get("Id"))
    duplicate_ids = {mid: count for mid, count in id_counts.items() if count > 1}
    if duplicate_ids:
        flow_logger.warning("Duplicate mindbody_id values in API response: %s", duplicate_ids)
        deduped: dict[str, dict] = {}
        for m in changed_members:
            mid = str(m.get("Id", ""))
            if not mid:
                continue
            existing = deduped.get(mid)
            if existing is None or (m.get("Active") and not existing.get("Active")):
                deduped[mid] = m
        changed_members = list(deduped.values())
        flow_logger.info("After dedup: %d members", len(changed_members))

    # ── Step 3: Upsert members + fetch memberships in parallel ────────────────
    active_from_api = [m for m in changed_members if m.get("Active")]
    active_client_ids = [str(m["Id"]) for m in active_from_api if m.get("Id")]
    flow_logger.info(
        "Upserting %d changed members; %d active",
        len(changed_members),
        len(active_client_ids),
    )

    if active_client_ids:
        upsert_count, memberships_by_client = await asyncio.gather(
            upsert_mindbody_users_batch(changed_members, fetched_at=modified_since),
            fetch_all_memberships(active_client_ids),
        )
        flow_logger.info(
            "Upserted %d rows; fetched memberships for %d clients",
            upsert_count,
            len(memberships_by_client),
        )
        if memberships_by_client:
            membership_upsert_count = await upsert_mindbody_memberships_batch(
                memberships_by_client
            )
            flow_logger.info("Upserted %d membership rows", membership_upsert_count)
    else:
        upsert_count = await upsert_mindbody_users_batch(
            changed_members, fetched_at=modified_since
        )
        flow_logger.info(
            "Upserted %d rows; skipped membership fetch (no active members in changed set)",
            upsert_count,
        )

    # ── Step 4: Load active changed members from DB ───────────────────────────
    all_changed_ids = [str(m["Id"]) for m in changed_members if m.get("Id")]
    active_members_orm = await load_active_members_by_ids(all_changed_ids)
    flow_logger.info(
        "Active members with valid membership among changed set: %d", len(active_members_orm)
    )

    if not active_members_orm:
        flow_logger.info("No active members among changed records — nothing to push")
        return

    active_members = [
        {
            "Id": m.mindbody_id,
            "FirstName": m.first_name,
            "LastName": m.last_name,
            "Gender": m.gender,
            "Email": m.email,
        }
        for m in active_members_orm
    ]

    # ── Step 5: Classify by gender ────────────────────────────────────────────
    active_male_ids: set[str] = set()
    active_female_ids: set[str] = set()
    ungendered_ids: set[str] = set()

    for m in active_members:
        gender = (m.get("Gender") or "").lower()
        mid = m["Id"]
        if gender == "male":
            active_male_ids.add(mid)
        elif gender == "female":
            active_female_ids.add(mid)
        else:
            ungendered_ids.add(mid)
            active_male_ids.add(mid)
            active_female_ids.add(mid)

    member_map: dict[str, dict] = {m["Id"]: m for m in active_members}

    if ungendered_ids:
        flow_logger.warning(
            "%d active member(s) have no gender set — routing to all gates",
            len(ungendered_ids),
        )
    flow_logger.info(
        "Classified: %d male, %d female, %d ungendered (routed to all)",
        len(active_male_ids) - len(ungendered_ids),
        len(active_female_ids) - len(ungendered_ids),
        len(ungendered_ids),
    )

    # ── Step 6: Load devices + membership windows in parallel ─────────────────
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
        flow_logger.warning("No enabled devices with integration enabled — nothing to push")
        return

    # ── Step 7: Fetch live users from Dahua devices ───────────────────────────
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

    # ── Step 8: Plan incremental operations (no deactivation) ─────────────────
    all_items: list[dict] = []

    for gate_label, device_ids, active_ids in [
        ("male", male_device_ids, active_male_ids),
        ("female", female_device_ids, active_female_ids),
    ]:
        for device_id in device_ids:
            items = _plan_incremental_operations(
                device_id=device_id,
                active_member_ids=active_ids,
                member_map=member_map,
                dahua_users=dahua_users_by_device.get(device_id, []),
                membership_windows=membership_windows,
            )
            c = Counter(i["action"] for i in items)
            flow_logger.info(
                "Device %d (%s): enroll=%d reactivate=%d update=%d",
                device_id,
                gate_label,
                c["enroll"],
                c["reactivate"],
                c["update"],
            )
            all_items.extend(items)

    total = Counter(i["action"] for i in all_items)
    flow_logger.info(
        "Total planned: %d operations — enroll=%d reactivate=%d update=%d",
        len(all_items),
        total["enroll"],
        total["reactivate"],
        total["update"],
    )

    if not all_items:
        flow_logger.info("All changed members already in sync — nothing to do")
        return

    # ── Step 9: Write queue + execute push ────────────────────────────────────
    flow_logger.info("Writing %d items to sync queue (run_id=%s)", len(all_items), run_id)
    await write_sync_queue_batch(run_id, all_items, flow_type="incremental")

    push_start = datetime.now(UTC)
    flow_logger.info("Starting Dahua push phase")
    push_stats = await run_dahua_push(run_id, flow_logger)

    await create_table_artifact(
        key="incremental-sync-results",
        table=[
            {"metric": "enrolled", "count": push_stats.get("enrolled", 0)},
            {"metric": "reactivated", "count": push_stats.get("reactivated", 0)},
            {"metric": "updated", "count": push_stats.get("updated", 0)},
            {"metric": "failed", "count": push_stats.get("failed", 0)},
        ],
        description=(
            f"## Incremental Sync — {run_started_at.strftime('%Y-%m-%d %H:%M')} UTC\n"
            f"run_id: `{run_id}`"
        ),
    )
    # Advance the watermark only after a successful push so that a mid-run crash
    # does not skip members on the next incremental run.
    watermark_count = await advance_watermark(all_changed_ids, run_started_at)
    flow_logger.info("Watermark advanced for %d members to %s", watermark_count, run_started_at)

    elapsed = (datetime.now(UTC) - run_started_at).total_seconds()
    push_elapsed = (datetime.now(UTC) - push_start).total_seconds()
    flow_logger.info(
        "Incremental sync complete — push took %.1fs, total %.1fs — %s",
        push_elapsed,
        elapsed,
        push_stats,
    )


def _plan_incremental_operations(
    device_id: int,
    active_member_ids: set[str],
    member_map: dict[str, dict],
    dahua_users: list[dict],
    membership_windows: dict[str, dict],
) -> list[dict]:
    """
    Plan enroll / reactivate / update operations for changed active members.
    Does NOT generate deactivation actions — the daily full sync handles that.
    """
    dahua_map: dict[str, dict] = {u["UserID"]: u for u in dahua_users if u.get("UserID")}

    items: list[dict] = []

    for cid in active_member_ids:
        window = membership_windows.get(cid, {})
        start_date = window.get("valid_start")
        expiration_date = window.get("valid_end")
        new_valid_start = _format_dahua_date(start_date)
        new_valid_end = _format_dahua_date(expiration_date)

        device_uid = _make_dahua_user_id(cid)

        if device_uid not in dahua_map:
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
            dahua_user = dahua_map[device_uid]
            card_status = str(dahua_user.get("CardStatus", "0"))

            if card_status == "4":
                # Frozen on device but membership is active → reactivate
                items.append(
                    {
                        "device_id": device_id,
                        "mindbody_client_id": cid,
                        "action": "reactivate",
                        "member_snapshot": None,
                        "dahua_user_id": device_uid,
                        "enrollment_id": None,
                    }
                )
                # Also update access window if it changed while frozen
                current_valid_end = dahua_user.get("ValidDateEnd") or ""
                current_valid_start = dahua_user.get("ValidDateStart") or ""
                window_changed = (new_valid_end or new_valid_start) and (
                    new_valid_end != current_valid_end or new_valid_start != current_valid_start
                )
                if window_changed:
                    items.append(
                        {
                            "device_id": device_id,
                            "mindbody_client_id": cid,
                            "action": "update",
                            "member_snapshot": json.dumps(
                                {
                                    "card_name": None,
                                    "valid_start": new_valid_start,
                                    "valid_end": new_valid_end,
                                }
                            ),
                            "dahua_user_id": device_uid,
                            "enrollment_id": None,
                        }
                    )

            else:
                # Active on device — check if name or access window needs updating
                m = member_map.get(cid, {})
                first_name = m.get("FirstName", "")
                last_name = m.get("LastName", "")
                new_card_name = f"{first_name} {last_name}".strip()
                current_card_name = dahua_user.get("CardName") or ""
                current_valid_end = dahua_user.get("ValidDateEnd") or ""
                current_valid_start = dahua_user.get("ValidDateStart") or ""

                name_changed = new_card_name and new_card_name != current_card_name
                window_changed = (new_valid_end or new_valid_start) and (
                    new_valid_end != current_valid_end or new_valid_start != current_valid_start
                )

                if name_changed or window_changed:
                    items.append(
                        {
                            "device_id": device_id,
                            "mindbody_client_id": cid,
                            "action": "update",
                            "member_snapshot": json.dumps(
                                {
                                    "card_name": new_card_name if name_changed else None,
                                    "valid_start": new_valid_start if window_changed else None,
                                    "valid_end": new_valid_end if window_changed else None,
                                }
                            ),
                            "dahua_user_id": device_uid,
                            "enrollment_id": None,
                        }
                    )

    return items
