"""
Prefect worker entry point.

Start with:
    python -m app.sync.worker

On startup:
 1. Registers MindBodyCredentials block type with Prefect server
 2. Creates per-device concurrency limits (max 2 concurrent calls per device)
 3. Starts prefect serve() with 2 deployments:
    - sync-integration/full         (every N minutes, full reconciliation)
    - device-health/scheduled       (every M minutes, health check)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from prefect import aserve
from prefect.client.orchestration import get_client
from prefect.variables import Variable

from app.models.database import init_async_db, init_db
from app.models.device import DahuaDevice
from app.sync.blocks import MindBodyCredentials
from app.sync.flows.dahua_push import sync_dahua_push_flow
from app.sync.flows.health import device_health_flow
from app.sync.flows.integration import sync_integration_flow

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


async def _sync_variables_from_env() -> None:
    """Set Prefect Variables from env vars when provided, otherwise leave existing values."""
    mapping = {
        "SYNC_INTERVAL_MINUTES": "sync_interval_minutes",
        "HEALTH_INTERVAL_MINUTES": "health_interval_minutes",
        "DAHUA_PUSH_ENABLED": "dahua_push_enabled",
    }
    for env_key, var_name in mapping.items():
        value = os.environ.get(env_key)
        if value is not None:
            await Variable.set(var_name, value, overwrite=True)
            logger.info("Prefect variable '%s' set to %s (from env)", var_name, value)


async def _create_block_from_env() -> None:
    """Create or update the MindBodyCredentials block 'production' from env vars.

    If any of the required MINDBODY_* env vars are missing, this is a no-op
    and the block must be created manually via the Prefect UI.
    """
    api_key = os.environ.get("MINDBODY_API_KEY")
    site_id = os.environ.get("MINDBODY_SITE_ID")
    username = os.environ.get("MINDBODY_USERNAME")
    password = os.environ.get("MINDBODY_PASSWORD")
    base_url = os.environ.get("MINDBODY_BASE_URL", "https://api.mindbodyonline.com/public/v6")

    if not all([api_key, site_id, username, password]):
        logger.info(
            "MINDBODY_API_KEY / MINDBODY_SITE_ID / MINDBODY_USERNAME / MINDBODY_PASSWORD "
            "not all set — skipping block auto-creation (create manually via Prefect UI)"
        )
        return

    block = MindBodyCredentials(
        api_key=api_key,  # type: ignore[arg-type]
        site_id=site_id,  # type: ignore[arg-type]
        username=username,  # type: ignore[arg-type]
        password=password,  # type: ignore[arg-type]
        base_url=base_url,
    )
    await block.save("production", overwrite=True)
    logger.info("MindBodyCredentials block 'production' created/updated from env vars")


async def _setup() -> tuple[int, int]:
    """Initialise DB, register block types, set up concurrency limits."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    database_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost/sync")

    # Sync DB for reading device list
    sync_factory = init_db(database_url)
    db = sync_factory()
    try:
        device_ids = [
            d.id for d in db.query(DahuaDevice).filter(DahuaDevice.is_enabled.is_(True)).all()
        ]
    finally:
        db.close()

    # Async DB for Prefect tasks
    init_async_db(database_url)

    # Register block type with Prefect server
    try:
        await MindBodyCredentials.register_type_and_schema()
        logger.info("MindBodyCredentials block type registered")
    except Exception as e:
        logger.warning("Could not register block type (server may not be ready): %s", e)

    # Auto-create block instance from env vars if provided
    await _create_block_from_env()

    # Concurrency limits
    await ensure_concurrency_limits(device_ids)

    # Sync variables from env vars (idempotent)
    await _sync_variables_from_env()

    # Seed dahua_push_enabled default (false) only if not already set
    if await Variable.get("dahua_push_enabled", default=None) is None:
        await Variable.set("dahua_push_enabled", "false")
        logger.info("Prefect variable 'dahua_push_enabled' initialised to 'false'")

    # Read schedule intervals from Prefect Variables
    interval = int(await Variable.get("sync_interval_minutes", default="30"))
    health_interval = int(await Variable.get("health_interval_minutes", default="5"))
    logger.info("Sync interval: %d min | Health interval: %d min", interval, health_interval)

    return interval, health_interval


async def main() -> None:
    interval, health_interval = await _setup()

    await aserve(
        # Full sync — every N minutes
        await sync_integration_flow.ato_deployment(
            name="full",
            interval=timedelta(minutes=interval),
            parameters={"sync_type": "scheduled"},
            tags=["integration", "full"],
        ),
        # Scheduled health check
        await device_health_flow.ato_deployment(
            name="scheduled",
            interval=timedelta(minutes=health_interval),
            tags=["health"],
        ),
        # Manual-only — retry a push by supplying the original run_id
        await sync_dahua_push_flow.ato_deployment(
            name="retry",
            tags=["dahua", "push"],
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
