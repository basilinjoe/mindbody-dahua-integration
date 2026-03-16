from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from app.sync.tasks import (
    deactivate_on_device,
    enroll_on_device,
    load_pending_queue_items,
    mark_queue_item,
    reactivate_on_device,
    update_window_on_device,
)

logger = logging.getLogger(__name__)


async def run_dahua_push(run_id: str, photo_max_kb: int, flow_logger) -> dict:
    """
    Core push logic — can be called directly from another flow to avoid
    Prefect subflow tracking (which requires matching client/server versions).
    """
    flow_logger.info("Dahua push started (run_id=%s)", run_id)

    items = await load_pending_queue_items(run_id)
    push_enabled_raw = await Variable.aget("dahua_push_enabled", default="true")
    flow_logger.info("Loaded %d pending queue items", len(items))

    push_enabled = str(push_enabled_raw).lower().strip()
    if push_enabled != "true":
        flow_logger.warning(
            "Dahua push is DISABLED (dahua_push_enabled=%r) — skipping all device operations",
            push_enabled,
        )
        return {"enrolled": 0, "deactivated": 0, "reactivated": 0, "window_updated": 0, "failed": 0, "skipped": len(items)}

    if not items:
        flow_logger.info("No pending items — nothing to push")
        return {"enrolled": 0, "deactivated": 0, "reactivated": 0, "window_updated": 0, "failed": 0}

    stats = {"enrolled": 0, "deactivated": 0, "reactivated": 0, "window_updated": 0, "failed": 0}
    _stat_key = {
        "enroll": "enrolled",
        "deactivate": "deactivated",
        "reactivate": "reactivated",
        "update_window": "window_updated",
    }

    async def _execute(item):
        try:
            if item.action == "enroll":
                member = json.loads(item.member_snapshot or "{}")
                result = await enroll_on_device(item.device_id, member, photo_max_kb)
                success = bool(result)
            elif item.action == "deactivate":
                success = await deactivate_on_device(
                    item.device_id, item.dahua_user_id, item.enrollment_id
                )
            elif item.action == "reactivate":
                success = await reactivate_on_device(
                    item.device_id, item.dahua_user_id, item.enrollment_id
                )
            elif item.action == "update_window":
                window = json.loads(item.member_snapshot or "{}")
                success = await update_window_on_device(
                    item.device_id, item.dahua_user_id,
                    window.get("valid_start"), window.get("valid_end"),
                    item.enrollment_id,
                )
            else:
                raise ValueError(f"Unknown action: {item.action!r}")

            if success:
                await mark_queue_item(item.id, "success")
                return item.action, None
            else:
                await mark_queue_item(item.id, "failed", "Device returned failure")
                return item.action, "Device returned failure"

        except Exception as exc:
            err = str(exc)
            logger.warning(
                "Queue item %d failed (action=%s device=%d client=%s): %s",
                item.id, item.action, item.device_id, item.mindbody_client_id, err,
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
        "Dahua push complete — enrolled=%d deactivated=%d reactivated=%d window_updated=%d failed=%d",
        stats["enrolled"], stats["deactivated"], stats["reactivated"],
        stats["window_updated"], stats["failed"],
    )

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    await create_table_artifact(  # type: ignore[misc]
        key="dahua-push-results",
        table=[{"metric": k, "count": v} for k, v in stats.items()],
        description=f"## Dahua Push — {run_ts} UTC  \nrun_id: `{run_id}`",
    )

    return stats


@flow(name="sync-dahua-push", log_prints=True)
async def sync_dahua_push_flow(run_id: str, photo_max_kb: int = 200) -> dict:
    """Standalone flow wrapper — delegates to run_dahua_push."""
    return await run_dahua_push(run_id, photo_max_kb, get_run_logger())
