from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from app.sync.tasks import (
    deactivate_on_device,
    enroll_on_device,
    load_pending_queue_items,
    mark_queue_item,
    reactivate_on_device,
    update_on_device,
)

logger = logging.getLogger(__name__)


async def run_dahua_push(run_id: str, flow_logger) -> dict:
    """
    Core push logic — can be called directly from another flow to avoid
    Prefect subflow tracking (which requires matching client/server versions).
    """
    flow_logger.info("Dahua push started (run_id=%s)", run_id)

    items = await load_pending_queue_items(run_id)
    push_enabled_raw = await Variable.aget("dahua_push_enabled", default="true")
    flow_logger.info("Loaded %d pending queue items for run_id=%s", len(items), run_id)
    if items:
        from collections import Counter

        action_counts = Counter(i.action for i in items)
        device_counts = Counter(i.device_id for i in items)
        flow_logger.info(
            "Queue breakdown — by action: %s, by device: %s",
            dict(action_counts),
            dict(device_counts),
        )

    push_enabled = str(push_enabled_raw).lower().strip()
    if push_enabled != "true":
        flow_logger.warning(
            "Dahua push is DISABLED (dahua_push_enabled=%r) — skipping all device operations",
            push_enabled,
        )
        return {
            "enrolled": 0,
            "deactivated": 0,
            "reactivated": 0,
            "updated": 0,
            "failed": 0,
            "skipped": len(items),
        }

    if not items:
        flow_logger.info("No pending items — nothing to push")
        return {"enrolled": 0, "deactivated": 0, "reactivated": 0, "updated": 0, "failed": 0}

    stats = {"enrolled": 0, "deactivated": 0, "reactivated": 0, "updated": 0, "failed": 0}
    _stat_key = {
        "enroll": "enrolled",
        "deactivate": "deactivated",
        "reactivate": "reactivated",
        "update": "updated",
    }

    # Cap concurrent operations per device to avoid flooding Dahua CGI endpoints.
    # This guards standalone runs; Prefect concurrency slots provide the same limit
    # when a Prefect server is available.
    per_device_limit = 2
    device_semaphores: dict[int, asyncio.Semaphore] = {
        device_id: asyncio.Semaphore(per_device_limit)
        for device_id in {item.device_id for item in items}
    }

    async def _execute(item):
        async with device_semaphores[item.device_id]:
            return await _execute_inner(item)

    async def _execute_inner(item):
        logger.info(
            "Executing queue item %d: action=%s device=%d client=%s",
            item.id,
            item.action,
            item.device_id,
            item.mindbody_client_id,
        )
        try:
            if item.action == "enroll":
                member = json.loads(item.member_snapshot or "{}")
                logger.info(
                    "  Enrolling client=%s on device=%d (name=%s %s, window=%s→%s)",
                    item.mindbody_client_id,
                    item.device_id,
                    member.get("FirstName", ""),
                    member.get("LastName", ""),
                    member.get("valid_start"),
                    member.get("valid_end"),
                )
                result = await enroll_on_device(item.device_id, member)
                success = bool(result)
            elif item.action == "deactivate":
                logger.info(
                    "  Deactivating user=%s on device=%d",
                    item.dahua_user_id,
                    item.device_id,
                )
                success = await deactivate_on_device(item.device_id, item.dahua_user_id)
            elif item.action == "reactivate":
                logger.info(
                    "  Reactivating user=%s on device=%d",
                    item.dahua_user_id,
                    item.device_id,
                )
                success = await reactivate_on_device(item.device_id, item.dahua_user_id)
            elif item.action == "update":
                snapshot = json.loads(item.member_snapshot or "{}")
                logger.info(
                    "  Updating user=%s on device=%d: name=%s, window=%s→%s",
                    item.dahua_user_id,
                    item.device_id,
                    snapshot.get("card_name"),
                    snapshot.get("valid_start"),
                    snapshot.get("valid_end"),
                )
                success = await update_on_device(
                    item.device_id,
                    item.dahua_user_id,
                    card_name=snapshot.get("card_name"),
                    valid_start=snapshot.get("valid_start"),
                    valid_end=snapshot.get("valid_end"),
                )
            else:
                raise ValueError(f"Unknown action: {item.action!r}")

            if success:
                await mark_queue_item(item.id, "success")
                logger.info("  Item %d succeeded", item.id)
                return item.action, None
            else:
                await mark_queue_item(item.id, "failed", "Device returned failure")
                logger.warning("  Item %d: device returned failure", item.id)
                return item.action, "Device returned failure"

        except Exception as exc:
            err = str(exc)
            logger.warning(
                "Queue item %d failed (action=%s device=%d client=%s): %s",
                item.id,
                item.action,
                item.device_id,
                item.mindbody_client_id,
                err,
            )
            await mark_queue_item(item.id, "failed", err)
            return item.action, err

    results = await asyncio.gather(*[_execute(item) for item in items], return_exceptions=True)

    for res in results:
        if isinstance(res, BaseException):
            stats["failed"] += 1
        else:
            action, err = res  # type: ignore[misc]
            if err is None:
                stats[_stat_key.get(action, "failed")] += 1
            else:
                stats["failed"] += 1

    flow_logger.info(
        "Dahua push complete — enrolled=%d deactivated=%d reactivated=%d updated=%d failed=%d",
        stats["enrolled"],
        stats["deactivated"],
        stats["reactivated"],
        stats["updated"],
        stats["failed"],
    )

    run_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
    await create_table_artifact(  # type: ignore[misc]
        key="dahua-push-results",
        table=[{"metric": k, "count": v} for k, v in stats.items()],
        description=f"## Dahua Push — {run_ts} UTC  \nrun_id: `{run_id}`",
    )

    return stats


@flow(name="sync-dahua-push", log_prints=True)
async def sync_dahua_push_flow(run_id: str) -> dict:
    """Standalone flow wrapper — delegates to run_dahua_push."""
    return await run_dahua_push(run_id, get_run_logger())
