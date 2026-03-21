from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.main import create_app
from app.models import database as db_module
from app.models.database import Base
from tests.helpers.fakes import FakeMindBodyClient

_TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "sqlite:///")
_TEST_ASYNC_DB_URL = _TEST_DB_URL.replace("sqlite:///", "sqlite+aiosqlite:///")


@pytest.fixture(autouse=True)
async def _reset_async_db_globals():
    """Reset global async engine state before each test to prevent cross-test contamination."""
    old_engine = db_module.async_engine
    old_factory = db_module.AsyncSessionLocal
    db_module.async_engine = None
    db_module.AsyncSessionLocal = None
    yield
    # Dispose the engine created during this test to close aiosqlite connections
    if db_module.async_engine is not None and db_module.async_engine is not old_engine:
        await db_module.async_engine.dispose()
    db_module.async_engine = old_engine
    db_module.AsyncSessionLocal = old_factory


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
        database_url=_TEST_ASYNC_DB_URL,
        admin_username="admin",
        admin_password="changeme",
        secret_key="unit-test-secret-key",
        secure_cookies=False,
        sync_interval_minutes=30,
        device_health_interval_minutes=5,
        log_level="INFO",
    )


@pytest.fixture
def db_session_factory() -> Generator[sessionmaker[Session], None, None]:
    engine = create_engine(_TEST_DB_URL)
    session_local = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(bind=engine)
    try:
        yield session_local
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
    return create_app(
        settings=settings,
        db_session_factory_override=db_session_factory,
        mindbody_client_factory=lambda _: fake_mindbody_client,
    )


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
