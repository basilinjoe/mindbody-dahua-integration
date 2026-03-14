from __future__ import annotations

import logging
from datetime import datetime, timezone

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from sqlalchemy import update

from app.models.database import _get_async_session_factory
from app.models.device import DahuaDevice
from app.sync.tasks import check_device_health_task, load_all_devices

logger = logging.getLogger(__name__)


@flow(name="device-health", log_prints=True)
async def device_health_flow() -> None:
    """Check health of all enabled Dahua devices and update DB status."""
    flow_logger = get_run_logger()
    devices = await load_all_devices()
    flow_logger.info("Checking health of %d devices", len(devices))

    rows = []
    for device in devices:
        try:
            online = await check_device_health_task(device.id)
            status = "online" if online else "offline"
        except Exception as e:
            status = "error"
            flow_logger.warning("Health check failed for device %d: %s", device.id, e)

        # Update DB
        async with _get_async_session_factory()() as db:
            await db.execute(
                update(DahuaDevice)
                .where(DahuaDevice.id == device.id)
                .values(
                    status=status,
                    last_seen_at=datetime.now(timezone.utc) if status == "online" else None,
                )
            )
            await db.commit()

        rows.append(f"| {device.name} | {device.host} | {status} |")
        flow_logger.info("Device %s (%s): %s", device.name, device.host, status)

    table_rows = "\n".join(rows)
    markdown = f"""## Device Health Check — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC

| Device | Host | Status |
|--------|------|--------|
{table_rows}
"""
    await create_markdown_artifact(key="health-results", markdown=markdown)
