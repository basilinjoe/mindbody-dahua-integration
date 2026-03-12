from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.sync_log import SyncLog
from app.sync import engine as engine_mod
from app.sync.engine import SyncEngine
from tests.helpers.fakes import FakeDahuaClient, FakeMindBodyClient
from tests.helpers.factories import make_device, make_member


@pytest.fixture
def engine_settings() -> Settings:
    return Settings(
        secret_key="sync-test",
        photo_max_size_kb=190,
        sync_interval_minutes=30,
        device_health_interval_minutes=5,
    )


def _build_engine(
    fake_mindbody: FakeMindBodyClient,
    db_session_factory,
    engine_settings: Settings,
) -> SyncEngine:
    return SyncEngine(fake_mindbody, db_session_factory, engine_settings)


@pytest.mark.asyncio
async def test_full_sync_reports_error_when_no_enabled_devices(db_session_factory, engine_settings: Settings) -> None:
    mb = FakeMindBodyClient(all_clients=[{"Id": "1"}], active_map={"1": True})
    engine = _build_engine(mb, db_session_factory, engine_settings)

    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]
    report = await engine.full_sync()

    assert report.errors == ["No enabled Dahua devices configured"]


@pytest.mark.asyncio
async def test_full_sync_handles_mindbody_fetch_failure(db_session_factory, engine_settings: Settings) -> None:
    mb = FakeMindBodyClient()
    mb.raise_get_all = RuntimeError("mindbody unavailable")
    engine = _build_engine(mb, db_session_factory, engine_settings)
    engine._dahua_clients = {1: FakeDahuaClient()}
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]

    report = await engine.full_sync()

    assert len(report.errors) == 1
    assert "Failed to fetch MindBody clients" in report.errors[0]


@pytest.mark.asyncio
async def test_full_sync_enroll_path_creates_member_and_log(
    db_session_factory,
    engine_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mb = FakeMindBodyClient(
        all_clients=[{"Id": "101", "FirstName": "Ana", "LastName": "Lopez", "Email": "ana@example.com", "PhotoUrl": "https://img/1.jpg"}],
        active_map={"101": True},
    )
    engine = _build_engine(mb, db_session_factory, engine_settings)
    fake_dc = FakeDahuaClient()
    engine._dahua_clients = {1: fake_dc}
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]

    async def fake_download(url: str) -> bytes:  # noqa: ARG001
        return b"raw-image"

    monkeypatch.setattr(engine_mod, "download_photo", fake_download)
    monkeypatch.setattr(engine_mod, "process_photo_for_dahua", lambda *_args, **_kwargs: "base64-photo")

    report = await engine.full_sync()

    db = db_session_factory()
    try:
        member = db.query(SyncedMember).filter_by(mindbody_client_id="101").first()
        log = db.query(SyncLog).filter_by(mindbody_client_id="101", action="enroll").first()
        assert member is not None
        assert member.is_active_in_dahua is True
        assert member.has_face_photo is True
        assert log is not None
    finally:
        db.close()

    assert report.enrolled == 1
    assert report.photos_uploaded == 1


@pytest.mark.asyncio
async def test_full_sync_deactivate_path_updates_member_status(
    db_session_factory,
    engine_settings: Settings,
) -> None:
    db = db_session_factory()
    db.add(make_member(mindbody_client_id="202", dahua_user_id="202", is_active_in_mindbody=True, is_active_in_dahua=True))
    db.commit()
    db.close()

    mb = FakeMindBodyClient(all_clients=[{"Id": "202"}], active_map={"202": False})
    engine = _build_engine(mb, db_session_factory, engine_settings)
    fake_dc = FakeDahuaClient()
    fake_dc.update_user_status_result = True
    engine._dahua_clients = {1: fake_dc}
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]

    report = await engine.full_sync()

    db2 = db_session_factory()
    try:
        member = db2.query(SyncedMember).filter_by(mindbody_client_id="202").first()
        assert member is not None
        assert member.is_active_in_mindbody is False
        assert member.is_active_in_dahua is False
    finally:
        db2.close()

    assert report.deactivated == 1


@pytest.mark.asyncio
async def test_full_sync_reactivate_path_and_photo_retry(
    db_session_factory,
    engine_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = db_session_factory()
    member = make_member(
        mindbody_client_id="303",
        dahua_user_id="303",
        is_active_in_mindbody=False,
        is_active_in_dahua=False,
        has_face_photo=False,
        face_photo_source=None,
    )
    db.add(member)
    db.commit()
    db.close()

    mb = FakeMindBodyClient(
        all_clients=[{"Id": "303", "FirstName": "Re", "LastName": "Active", "PhotoUrl": "https://img/303.jpg"}],
        active_map={"303": True},
    )
    engine = _build_engine(mb, db_session_factory, engine_settings)
    fake_dc = FakeDahuaClient()
    fake_dc.update_user_status_result = True
    fake_dc.upload_face_photo_result = True
    engine._dahua_clients = {1: fake_dc}
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]

    async def fake_download(url: str) -> bytes:  # noqa: ARG001
        return b"reactivate-image"

    monkeypatch.setattr(engine_mod, "download_photo", fake_download)
    monkeypatch.setattr(engine_mod, "process_photo_for_dahua", lambda *_args, **_kwargs: "b64")

    report = await engine.full_sync()

    db2 = db_session_factory()
    try:
        updated = db2.query(SyncedMember).filter_by(mindbody_client_id="303").first()
        assert updated is not None
        assert updated.is_active_in_mindbody is True
        assert updated.is_active_in_dahua is True
        assert updated.has_face_photo is True
        assert updated.face_photo_source == "mindbody"
    finally:
        db2.close()

    assert report.reactivated == 1


@pytest.mark.asyncio
async def test_sync_single_member_branches(db_session_factory, engine_settings: Settings) -> None:
    mb = FakeMindBodyClient(active_map={"10": True, "11": False, "12": True}, search_results={"10": [{"Id": "10"}], "12": [{"Id": "12"}]})
    engine = _build_engine(mb, db_session_factory, engine_settings)
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]

    db = db_session_factory()
    db.add(make_member(mindbody_client_id="11", dahua_user_id="11", is_active_in_dahua=True))
    db.add(make_member(mindbody_client_id="12", dahua_user_id="12", is_active_in_dahua=False))
    db.commit()
    db.close()

    engine._enroll_member = AsyncMock()  # type: ignore[method-assign]
    engine._deactivate_member = AsyncMock()  # type: ignore[method-assign]
    engine._reactivate_member = AsyncMock()  # type: ignore[method-assign]

    await engine.sync_single_member("10", "webhook")
    await engine.sync_single_member("11", "webhook")
    await engine.sync_single_member("12", "webhook")

    assert engine._enroll_member.await_count == 1
    assert engine._deactivate_member.await_count == 1
    assert engine._reactivate_member.await_count == 1


@pytest.mark.asyncio
async def test_enroll_manual_member_sets_manual_flags(db_session_factory, engine_settings: Settings) -> None:
    mb = FakeMindBodyClient()
    engine = _build_engine(mb, db_session_factory, engine_settings)
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]
    engine._dahua_clients = {1: FakeDahuaClient()}

    db = db_session_factory()
    try:
        member = await engine.enroll_manual_member(
            first_name="Manual",
            last_name="User",
            email="manual@example.com",
            manual_id="9001",
            photo_base64="fake-b64",
            db=db,
        )
        db.commit()
        db.refresh(member)
        assert member.is_manual is True
        assert member.face_photo_source == "manual"
        assert member.is_active_in_dahua is True
    finally:
        db.close()


@pytest.mark.asyncio
async def test_check_device_health_updates_statuses(db_session_factory, engine_settings: Settings) -> None:
    db = db_session_factory()
    d1 = make_device(name="Gate A", host="10.0.0.1")
    d2 = make_device(name="Gate B", host="10.0.0.2")
    db.add_all([d1, d2])
    db.commit()
    db.refresh(d1)
    db.refresh(d2)
    db.close()

    mb = FakeMindBodyClient()
    engine = _build_engine(mb, db_session_factory, engine_settings)
    c1 = FakeDahuaClient()
    c1.health_check_result = True
    c2 = FakeDahuaClient()
    c2.raise_on_health = True
    engine._dahua_clients = {d1.id: c1, d2.id: c2}
    engine.refresh_devices = lambda db: None  # type: ignore[method-assign]

    await engine.check_device_health()

    db2 = db_session_factory()
    try:
        u1 = db2.query(DahuaDevice).get(d1.id)
        u2 = db2.query(DahuaDevice).get(d2.id)
        assert u1 is not None and u1.status == "online"
        assert u1.last_seen_at is not None
        assert u2 is not None and u2.status == "error"
    finally:
        db2.close()


def test_make_helpers_generate_stable_identifiers() -> None:
    assert engine_mod._make_dahua_user_id("0012") == "12"
    assert engine_mod._make_dahua_user_id("ABC-1") == "ABC-1"
    assert engine_mod._make_card_no("77") == "MB00000077"
