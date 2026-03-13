from __future__ import annotations

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import Settings
from app.main import create_app
from app.models.admin_user import AdminUser
from app.models.database import Base
from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.mindbody_client import MindBodyClient
from app.models.sync_log import SyncLog
from tests.helpers.fakes import FakeMindBodyClient


@pytest.fixture
def settings() -> Settings:
    return Settings(
        mindbody_api_key="test-api-key",
        mindbody_site_id="test-site-id",
        mindbody_api_base_url="https://api.mindbodyonline.com/public/v6",
        mindbody_username="test-user",
        mindbody_password="test-password",
        mindbody_webhook_signature_key="test-signature-key",
        dahua_devices="",
        dahua_default_host="",
        dahua_default_password="",
        database_url="sqlite+pysqlite:///:memory:",
        admin_username="admin",
        admin_password="changeme",
        secret_key="unit-test-secret-key",
        sync_interval_minutes=30,
        device_health_interval_minutes=5,
        log_level="INFO",
    )


@pytest.fixture
def db_session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)
    try:
        yield SessionLocal
    finally:
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture
def db_session(db_session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    db = db_session_factory()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def fake_mindbody_client() -> FakeMindBodyClient:
    return FakeMindBodyClient()


@pytest.fixture
def app(
    settings: Settings,
    db_session_factory: sessionmaker[Session],
    fake_mindbody_client: FakeMindBodyClient,
):
    app = create_app(
        settings=settings,
        db_session_factory_override=db_session_factory,
        mindbody_client_factory=lambda _: fake_mindbody_client,
    )
    return app


@pytest.fixture
def client(app) -> Generator[TestClient, None, None]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def logged_in_client(client: TestClient, settings: Settings) -> TestClient:
    resp = client.post(
        "/admin/login",
        data={"username": settings.admin_username, "password": settings.admin_password},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    return client
