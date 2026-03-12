from __future__ import annotations

from app.models.admin_user import AdminUser
from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.sync_log import SyncLog


def make_admin_user(username: str = "admin", password: str = "changeme", is_active: bool = True) -> AdminUser:
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


def make_member(
    *,
    mindbody_client_id: str = "1",
    dahua_user_id: str = "1",
    first_name: str = "John",
    last_name: str = "Doe",
    email: str | None = "john@example.com",
    is_active_in_mindbody: bool = True,
    is_active_in_dahua: bool = True,
    has_face_photo: bool = True,
    face_photo_source: str | None = "mindbody",
    is_manual: bool = False,
) -> SyncedMember:
    return SyncedMember(
        mindbody_client_id=mindbody_client_id,
        dahua_user_id=dahua_user_id,
        card_no=f"MB{int(mindbody_client_id) if mindbody_client_id.isdigit() else 1:08d}",
        first_name=first_name,
        last_name=last_name,
        email=email,
        is_active_in_mindbody=is_active_in_mindbody,
        is_active_in_dahua=is_active_in_dahua,
        has_face_photo=has_face_photo,
        face_photo_source=face_photo_source,
        is_manual=is_manual,
    )


def make_sync_log(
    *,
    sync_type: str = "full_poll",
    action: str = "enroll",
    mindbody_client_id: str | None = "1",
    member_name: str | None = "John Doe",
    success: bool = True,
    error_message: str | None = None,
) -> SyncLog:
    return SyncLog(
        sync_type=sync_type,
        action=action,
        mindbody_client_id=mindbody_client_id,
        member_name=member_name,
        success=success,
        error_message=error_message,
    )
