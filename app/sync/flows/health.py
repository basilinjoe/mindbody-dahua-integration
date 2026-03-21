from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from app.models.database import _get_async_session_factory
from app.services import devices as devices_svc
from app.sync.tasks import check_device_health_task, load_all_devices

logger = logging.getLogger(__name__)


@flow(name="device-health", log_prints=True)
async def device_health_flow() -> None:
    """Check health of all enabled Dahua devices and update DB status."""
    flow_logger = get_run_logger()
    devices = await load_all_devices()
    flow_logger.info("Checking health of %d devices", len(devices))

    async def _check_one(device):
        try:
            online = await check_device_health_task(device.id)
            status = "online" if online else "offline"
        except Exception as e:
            status = "error"
            flow_logger.warning("Health check failed for device %d: %s", device.id, e)

        async with _get_async_session_factory()() as db:
            await devices_svc.update_status(db, device.id, status)
            await db.commit()

        flow_logger.info("Device %s (%s): %s", device.name, device.host, status)
        return f"| {device.name} | {device.host} | {status} |"

    rows = await asyncio.gather(*[_check_one(d) for d in devices])

    table_rows = "\n".join(rows)
    markdown = f"""## Device Health Check — {datetime.now(UTC).strftime("%Y-%m-%d %H:%M")} UTC

| Device | Host | Status |
|--------|------|--------|
{table_rows}
"""
    await create_markdown_artifact(key="health-results", markdown=markdown)
