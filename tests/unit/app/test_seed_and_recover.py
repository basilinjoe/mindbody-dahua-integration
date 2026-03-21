from __future__ import annotations

import json

import pytest

from app.config import Settings
from app.models.database import Base, init_async_db


def _make_settings(**overrides) -> Settings:
    defaults = dict(
        mindbody_api_key="k",
        mindbody_site_id="s",
        mindbody_username="u",
        mindbody_password="p",
        secret_key="sk",
        admin_username="admin",
        admin_password="changeme",
        database_url="sqlite+aiosqlite:///",
        dahua_devices="",
        dahua_default_host="",
        dahua_default_password="",
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
async def async_db():
    factory = init_async_db("sqlite+aiosqlite:///")
    from app.models.database import async_engine

    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with factory() as session:
        yield session
    await async_engine.dispose()


@pytest.mark.asyncio
async def test_seed_devices_from_json(async_db):
    from app.main import _seed_devices

    settings = _make_settings(
        dahua_devices=json.dumps(
            [
                {"name": "Gate A", "host": "10.0.0.1", "password": "p1"},
                {"name": "Gate B", "host": "10.0.0.2", "password": "p2"},
            ]
        )
    )
    await _seed_devices(async_db, settings)

    from sqlalchemy import select

    from app.models.device import DahuaDevice

    result = await async_db.execute(select(DahuaDevice))
    devices = list(result.scalars().all())
    assert len(devices) == 2
    assert {d.name for d in devices} == {"Gate A", "Gate B"}


@pytest.mark.asyncio
async def test_seed_devices_from_default_host(async_db):
    from app.main import _seed_devices

    settings = _make_settings(dahua_default_host="192.168.1.100", dahua_default_password="secret")
    await _seed_devices(async_db, settings)

    from sqlalchemy import select

    from app.models.device import DahuaDevice

    result = await async_db.execute(select(DahuaDevice))
    devices = list(result.scalars().all())
    assert len(devices) == 1
    assert devices[0].host == "192.168.1.100"


@pytest.mark.asyncio
async def test_seed_devices_skips_duplicate_host(async_db):
    from app.main import _seed_devices

    settings = _make_settings(
        dahua_devices=json.dumps([{"name": "G1", "host": "10.0.0.1", "password": "p"}])
    )
    await _seed_devices(async_db, settings)
    await _seed_devices(async_db, settings)  # second call should skip

    from sqlalchemy import select

    from app.models.device import DahuaDevice

    result = await async_db.execute(select(DahuaDevice))
    devices = list(result.scalars().all())
    assert len(devices) == 1


@pytest.mark.asyncio
async def test_seed_devices_no_devices(async_db):
    from app.main import _seed_devices

    settings = _make_settings()
    await _seed_devices(async_db, settings)

    from sqlalchemy import select

    from app.models.device import DahuaDevice

    result = await async_db.execute(select(DahuaDevice))
    assert list(result.scalars().all()) == []


@pytest.mark.asyncio
async def test_seed_devices_invalid_json(async_db):
    from app.main import _seed_devices

    settings = _make_settings(dahua_devices="not-json")
    await _seed_devices(async_db, settings)  # should not raise


@pytest.mark.asyncio
async def test_seed_devices_non_array_json(async_db):
    from app.main import _seed_devices

    settings = _make_settings(dahua_devices='{"host": "10.0.0.1"}')
    await _seed_devices(async_db, settings)  # should not raise


@pytest.mark.asyncio
async def test_recover_stuck_export_jobs(async_db):
    from app.main import _recover_stuck_export_jobs
    from app.models.export_job import ExportJob, ExportStatus

    job1 = ExportJob(status=ExportStatus.running)
    job2 = ExportJob(status=ExportStatus.pending)
    job3 = ExportJob(status=ExportStatus.complete)
    async_db.add_all([job1, job2, job3])
    await async_db.commit()

    await _recover_stuck_export_jobs(async_db)

    await async_db.refresh(job1)
    await async_db.refresh(job2)
    await async_db.refresh(job3)
    assert job1.status == ExportStatus.failed
    assert job2.status == ExportStatus.failed
    assert job3.status == ExportStatus.complete  # unchanged


@pytest.mark.asyncio
async def test_recover_no_stuck_jobs(async_db):
    from app.main import _recover_stuck_export_jobs

    await _recover_stuck_export_jobs(async_db)  # should not raise
