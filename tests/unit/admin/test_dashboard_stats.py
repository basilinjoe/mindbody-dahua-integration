from __future__ import annotations

from app.admin.dashboard import _get_stats
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership
from tests.helpers.factories import make_device


def test_get_stats_counts_members_and_devices(db_session) -> None:
    # Three MindBody clients; two have active memberships
    c1 = MindBodyClient(mindbody_id="1", first_name="Alice", last_name="A", active=True)
    c2 = MindBodyClient(mindbody_id="2", first_name="Bob", last_name="B", active=True)
    c3 = MindBodyClient(mindbody_id="3", first_name="Carol", last_name="C", active=False)
    db_session.add_all([c1, c2, c3])
    db_session.flush()

    db_session.add(MindBodyMembership(mindbody_client_id="1", membership_id="m1", is_active=True))
    db_session.add(MindBodyMembership(mindbody_client_id="2", membership_id="m2", is_active=True))

    db_session.add_all(
        [
            make_device(name="Gate 1", host="10.0.0.1", status="online", is_enabled=True),
            make_device(name="Gate 2", host="10.0.0.2", status="offline", is_enabled=True),
            make_device(name="Gate 3", host="10.0.0.3", status="online", is_enabled=False),
        ]
    )
    db_session.commit()

    stats = _get_stats(db_session)

    assert stats["total_members"] == 3
    assert stats["active_members"] == 2
    assert stats["devices_total"] == 2
    assert stats["devices_online"] == 1
