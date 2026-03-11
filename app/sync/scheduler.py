from __future__ import annotations

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.sync.engine import SyncEngine

logger = logging.getLogger(__name__)


class SyncScheduler:
    """Manages periodic full-sync and device health-check jobs."""

    def __init__(self, sync_engine: SyncEngine, sync_interval_min: int = 30, health_interval_min: int = 5) -> None:
        self._engine = sync_engine
        self._sync_interval = sync_interval_min
        self._health_interval = health_interval_min
        self._scheduler = AsyncIOScheduler()

    def start(self) -> None:
        self._scheduler.add_job(
            self._run_full_sync,
            trigger="interval",
            minutes=self._sync_interval,
            id="full_sync",
            replace_existing=True,
        )
        self._scheduler.add_job(
            self._run_health_check,
            trigger="interval",
            minutes=self._health_interval,
            id="device_health",
            replace_existing=True,
        )
        self._scheduler.start()
        logger.info(
            "Scheduler started: full_sync every %d min, health_check every %d min",
            self._sync_interval,
            self._health_interval,
        )

    def stop(self) -> None:
        self._scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

    def pause_sync(self) -> None:
        """Pause the scheduled full sync job (health checks continue)."""
        self._scheduler.pause_job("full_sync")
        logger.info("Full sync job paused")

    def resume_sync(self) -> None:
        """Resume the scheduled full sync job."""
        self._scheduler.resume_job("full_sync")
        logger.info("Full sync job resumed")

    @property
    def is_sync_paused(self) -> bool:
        """Return True if the full sync job is currently paused."""
        job = self._scheduler.get_job("full_sync")
        return job is not None and job.next_run_time is None

    async def _run_full_sync(self) -> None:
        logger.info("Scheduled full sync starting")
        try:
            report = await self._engine.full_sync()
            logger.info(
                "Scheduled full sync done: enrolled=%d deactivated=%d reactivated=%d errors=%d",
                report.enrolled,
                report.deactivated,
                report.reactivated,
                len(report.errors),
            )
        except Exception:
            logger.exception("Scheduled full sync failed")

    async def _run_health_check(self) -> None:
        try:
            await self._engine.check_device_health()
        except Exception:
            logger.exception("Scheduled health check failed")
