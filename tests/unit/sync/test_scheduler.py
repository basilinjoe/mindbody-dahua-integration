from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.sync.engine import SyncReport
from app.sync.scheduler import SyncScheduler


@dataclass
class DummyEngine:
    full_sync: AsyncMock
    check_device_health: AsyncMock


def test_scheduler_start_registers_expected_jobs() -> None:
    engine = DummyEngine(full_sync=AsyncMock(), check_device_health=AsyncMock())
    scheduler = SyncScheduler(engine, sync_interval_min=15, health_interval_min=3)
    scheduler._scheduler = MagicMock()

    scheduler.start()

    assert scheduler._scheduler.add_job.call_count == 2
    first_call = scheduler._scheduler.add_job.call_args_list[0]
    second_call = scheduler._scheduler.add_job.call_args_list[1]
    assert first_call.kwargs["id"] == "full_sync"
    assert first_call.kwargs["minutes"] == 15
    assert second_call.kwargs["id"] == "device_health"
    assert second_call.kwargs["minutes"] == 3
    scheduler._scheduler.start.assert_called_once()


def test_scheduler_stop_calls_shutdown() -> None:
    engine = DummyEngine(full_sync=AsyncMock(), check_device_health=AsyncMock())
    scheduler = SyncScheduler(engine)
    scheduler._scheduler = MagicMock()

    scheduler.stop()
    scheduler._scheduler.shutdown.assert_called_once_with(wait=False)


@pytest.mark.asyncio
async def test_run_full_sync_success() -> None:
    report = SyncReport()
    engine = DummyEngine(full_sync=AsyncMock(return_value=report), check_device_health=AsyncMock())
    scheduler = SyncScheduler(engine)

    await scheduler._run_full_sync()
    engine.full_sync.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_full_sync_failure_is_swallowed() -> None:
    engine = DummyEngine(
        full_sync=AsyncMock(side_effect=RuntimeError("boom")), check_device_health=AsyncMock()
    )
    scheduler = SyncScheduler(engine)

    await scheduler._run_full_sync()
    engine.full_sync.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_health_check_failure_is_swallowed() -> None:
    engine = DummyEngine(
        full_sync=AsyncMock(), check_device_health=AsyncMock(side_effect=RuntimeError("down"))
    )
    scheduler = SyncScheduler(engine)

    await scheduler._run_health_check()
    engine.check_device_health.assert_awaited_once()
