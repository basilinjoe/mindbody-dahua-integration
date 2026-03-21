from __future__ import annotations

from app.models.admin_user import AdminUser
from app.models.device import DahuaDevice


def make_admin_user(
    username: str = "admin", password: str = "changeme", is_active: bool = True
) -> AdminUser:
    user = AdminUser(username=username, is_active=is_active)
    user.set_password(password)
    return user


def make_device(
    *,
    name: str = "Gate 1",
    host: str = "192.168.1.100",
    port: int = 80,
    username: str = "admin",
    password: str = "pass",
    door_ids: str = "0",
    is_enabled: bool = True,
    status: str = "unknown",
) -> DahuaDevice:
    return DahuaDevice(
        name=name,
        host=host,
        port=port,
        username=username,
        password=password,
        door_ids=door_ids,
        is_enabled=is_enabled,
        status=status,
    )
