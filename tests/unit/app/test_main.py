from __future__ import annotations

import json

from app.main import _seed_admin, _seed_devices, create_app
from app.models.admin_user import AdminUser
from app.models.device import DahuaDevice


def test_create_app_registers_stateful_routes_and_static(settings, db_session_factory, fake_mindbody_client) -> None:
    app = create_app(
        settings=settings,
        start_scheduler=False,
        db_session_factory_override=db_session_factory,
        mindbody_client_factory=lambda _: fake_mindbody_client,
    )

    route_paths = {route.path for route in app.routes}
    assert "/health" in route_paths
    assert "/admin/login" in route_paths
    assert any(getattr(route, "path", None) == "/static" for route in app.routes)


def test_seed_admin_creates_single_admin_user(settings, db_session_factory) -> None:
    _seed_admin(db_session_factory, settings)
    _seed_admin(db_session_factory, settings)

    db = db_session_factory()
    try:
        users = db.query(AdminUser).all()
        assert len(users) == 1
        assert users[0].username == settings.admin_username
    finally:
        db.close()


def test_seed_devices_supports_json_array(settings, db_session_factory) -> None:
    settings.dahua_devices = json.dumps(
        [
            {"name": "Main Gate", "host": "10.0.0.10", "password": "a"},
            {"name": "Side Gate", "host": "10.0.0.11", "password": "b", "door_ids": "0,1"},
        ]
    )

    _seed_devices(db_session_factory, settings)

    db = db_session_factory()
    try:
        devices = db.query(DahuaDevice).order_by(DahuaDevice.host).all()
        assert [device.name for device in devices] == ["Main Gate", "Side Gate"]
        assert devices[1].door_ids == "0,1"
    finally:
        db.close()


def test_seed_devices_falls_back_to_default_host(settings, db_session_factory) -> None:
    settings.dahua_default_host = "10.0.0.20"
    settings.dahua_default_password = "pw"

    _seed_devices(db_session_factory, settings)

    db = db_session_factory()
    try:
        device = db.query(DahuaDevice).filter_by(host="10.0.0.20").first()
        assert device is not None
        assert device.name == "Default Device"
    finally:
        db.close()
