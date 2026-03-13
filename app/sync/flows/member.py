from __future__ import annotations

import asyncio
import logging

from prefect import flow, get_run_logger
from prefect.variables import Variable

from app.sync.tasks import (
    check_membership,
    deactivate_on_device,
    enroll_on_device,
    fetch_member,
    load_active_enrollments_for_member,
    load_device_ids_by_gate_type,
    load_enrollments_for_device,
    reactivate_on_device,
)

logger = logging.getLogger(__name__)


@flow(name="sync-member", log_prints=True)
async def sync_member_flow(client_id: str, sync_type: str = "webhook") -> None:
    """
    Single-member sync — called by webhook or manual trigger.
    Routes by Gender field → correct gate_type devices.
    """
    flow_logger = get_run_logger()
    flow_logger.info("Syncing member %s (trigger: %s)", client_id, sync_type)

    is_active = await check_membership(client_id)
    flow_logger.info("Member %s active: %s", client_id, is_active)

    if not is_active:
        # Deactivate only on devices this member is actually enrolled on
        enrollments = await load_active_enrollments_for_member(client_id)
        if enrollments:
            await asyncio.gather(
                *[deactivate_on_device(e.device_id, e.dahua_user_id, e.id) for e in enrollments],
                return_exceptions=True,
            )
            flow_logger.info("Deactivated %s on %d devices", client_id, len(enrollments))
        else:
            flow_logger.info("Member %s has no active enrollments — nothing to deactivate", client_id)
        return

    # Active member — enroll or reactivate
    member = await fetch_member(client_id)
    gender = (member.get("Gender") or "").lower()
    device_ids = await load_device_ids_by_gate_type(gender if gender else "all")
    photo_max_kb = int(await Variable.get("photo_max_size_kb", default="200"))

    if not device_ids:
        flow_logger.warning("No devices found for gate_type=%s", gender)
        return

    for device_id in device_ids:
        device_enrollments = await load_enrollments_for_device(device_id)
        if client_id not in device_enrollments:
            await enroll_on_device(device_id, member, photo_max_kb)
            flow_logger.info("Enrolled %s on device %d", client_id, device_id)
        elif not device_enrollments[client_id].is_active:
            await reactivate_on_device(
                device_id,
                device_enrollments[client_id].dahua_user_id,
                device_enrollments[client_id].id,
            )
            flow_logger.info("Reactivated %s on device %d", client_id, device_id)
        else:
            flow_logger.info("Member %s already active on device %d", client_id, device_id)
