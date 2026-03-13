"""
Prefect worker entry point.

Start with:
    python -m app.sync.worker

On startup:
 1. Registers MindBodyCredentials block type with Prefect server
 2. Creates per-device concurrency limits (max 2 concurrent calls per device)
 3. Starts prefect serve() with 4 deployments:
    - sync-integration/incremental  (every N minutes, incremental fetch)
    - sync-integration/full         (daily 2 AM, full reconciliation)
    - sync-member/default           (webhook/manual trigger)
    - device-health/scheduled       (every M minutes, health check)
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from prefect import serve
from prefect.client.orchestration import get_client
from prefect.variables import Variable

from app.models.database import init_async_db, init_db
from app.models.device import DahuaDevice
from app.sync.blocks import MindBodyCredentials
from app.sync.flows.health import device_health_flow
from app.sync.flows.integration import sync_integration_flow
from app.sync.flows.member import sync_member_flow

logger = logging.getLogger(__name__)


async def ensure_concurrency_limits(device_ids: list[int]) -> None:
    """Create per-device Prefect concurrency limits (idempotent)."""
    async with get_client() as client:
        for device_id in device_ids:
            tag = f"dahua-device-{device_id}"
            try:
                await client.create_concurrency_limit(
                    tag=tag,
                    concurrency_limit=2,
                )
                logger.info("Created concurrency limit: %s (max 2)", tag)
            except Exception:
                # Already exists — ignore
                pass


async def _setup() -> tuple[int, int]:
    """Initialise DB, register block types, set up concurrency limits."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    database_url = os.environ.get("DATABASE_URL", "sqlite:///./data/sync.db")

    # Sync DB for reading device list
    sync_factory = init_db(database_url)
    db = sync_factory()
    try:
        device_ids = [
            d.id
            for d in db.query(DahuaDevice)
            .filter(DahuaDevice.is_enabled.is_(True))
            .all()
        ]
    finally:
        db.close()

    # Async DB for Prefect tasks
    init_async_db(database_url)

    # Register block type with Prefect server
    try:
        MindBodyCredentials.register_type_and_schema()
        logger.info("MindBodyCredentials block type registered")
    except Exception as e:
        logger.warning("Could not register block type (server may not be ready): %s", e)

    # Concurrency limits
    await ensure_concurrency_limits(device_ids)

    # Read schedule intervals from Prefect Variables
    interval = int(await Variable.get("sync_interval_minutes", default="30"))
    health_interval = int(await Variable.get("health_interval_minutes", default="5"))
    logger.info("Sync interval: %d min | Health interval: %d min", interval, health_interval)

    return interval, health_interval


async def main() -> None:
    interval, health_interval = await _setup()

    await serve(
        # Webhook/manual trigger: single member sync
        sync_member_flow.to_deployment(
            name="default",
            tags=["webhook"],
        ),

        # Incremental sync — every N minutes, only modified members
        sync_integration_flow.to_deployment(
            name="incremental",
            interval=timedelta(minutes=interval),
            parameters={"force_full": False, "sync_type": "scheduled"},
            tags=["integration", "incremental"],
        ),

        # Full reconciliation — daily 2 AM, catches lapsed memberships
        sync_integration_flow.to_deployment(
            name="full",
            cron="0 2 * * *",
            parameters={"force_full": True, "sync_type": "scheduled"},
            tags=["integration", "full"],
        ),

        # Scheduled health check
        device_health_flow.to_deployment(
            name="scheduled",
            interval=timedelta(minutes=health_interval),
            tags=["health"],
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
