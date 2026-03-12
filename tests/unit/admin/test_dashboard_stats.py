from __future__ import annotations

from app.admin.dashboard import _get_stats
from tests.helpers.factories import make_device, make_member


def test_get_stats_counts_members_and_devices(db_session) -> None:
    db_session.add_all(
        [
            make_member(mindbody_client_id="1", dahua_user_id="1", is_active_in_dahua=True, has_face_photo=True),
            make_member(mindbody_client_id="2", dahua_user_id="2", is_active_in_dahua=True, has_face_photo=False),
            make_member(mindbody_client_id="3", dahua_user_id="3", is_active_in_dahua=False, has_face_photo=False),
            make_device(name="Gate 1", host="10.0.0.1", status="online", is_enabled=True),
            make_device(name="Gate 2", host="10.0.0.2", status="offline", is_enabled=True),
            make_device(name="Gate 3", host="10.0.0.3", status="online", is_enabled=False),
        ]
    )
    db_session.commit()

    stats = _get_stats(db_session)

    assert stats == {
        "total_members": 3,
        "active_members": 2,
        "missing_photos": 1,
        "devices_total": 2,
        "devices_online": 1,
    }
