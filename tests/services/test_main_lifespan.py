from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_seed_admin_uses_async_session():
    """_seed_admin must accept an AsyncSession, not a sync session factory."""
    import inspect

    import app.main as main_mod

    assert inspect.iscoroutinefunction(main_mod._seed_admin)


@pytest.mark.asyncio
async def test_seed_devices_uses_async_session():
    import inspect

    import app.main as main_mod

    assert inspect.iscoroutinefunction(main_mod._seed_devices)


@pytest.mark.asyncio
async def test_recover_stuck_jobs_uses_async_session():
    import inspect

    import app.main as main_mod

    assert inspect.iscoroutinefunction(main_mod._recover_stuck_export_jobs)
