from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.admin.dashboard import _get_device_rows, _get_mb_breakdown, _get_recent_queue, _get_stats
from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
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


def test_get_stats_includes_failed_24h(db_session) -> None:
    recent = DahuaSyncQueue(
        run_id="r1", device_id=1, mindbody_client_id="1",
        action="enroll", status="failed",
        created_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1),
    )
    old = DahuaSyncQueue(
        run_id="r2", device_id=1, mindbody_client_id="2",
        action="enroll", status="failed",
        created_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=25),
    )
    db_session.add_all([recent, old])
    db_session.commit()

    stats = _get_stats(db_session)

    assert stats["failed_24h"] == 1


def test_get_stats_active_members_pct_zero_when_no_members(db_session) -> None:
    stats = _get_stats(db_session)
    assert stats["active_members_pct"] == 0


def test_get_stats_active_members_pct(db_session) -> None:
    c1 = MindBodyClient(mindbody_id="1", first_name="A", last_name="B", active=True)
    c2 = MindBodyClient(mindbody_id="2", first_name="C", last_name="D", active=True)
    db_session.add_all([c1, c2])
    db_session.flush()
    db_session.add(MindBodyMembership(mindbody_client_id="1", membership_id="m1", is_active=True))
    db_session.commit()

    stats = _get_stats(db_session)

    assert stats["active_members_pct"] == 50


def test_get_stats_success_rate_pct_all_success(db_session) -> None:
    from datetime import timedelta
    db_session.add_all([
        DahuaSyncQueue(
            run_id="r", device_id=1, mindbody_client_id="1",
            action="enroll", status="success",
            created_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1),
        ),
        DahuaSyncQueue(
            run_id="r", device_id=1, mindbody_client_id="2",
            action="enroll", status="failed",
            created_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1),
        ),
    ])
    db_session.commit()
    stats = _get_stats(db_session)
    assert stats["success_rate_pct"] == 50


def test_get_stats_success_rate_pct_no_items_defaults_100(db_session) -> None:
    stats = _get_stats(db_session)
    assert stats["success_rate_pct"] == 100


def test_get_recent_queue_returns_rows_with_device_name(db_session) -> None:
    device = DahuaDevice(
        name="Male Gate A", host="10.0.0.1", port=80,
        username="admin", password="pass", door_ids="0",
    )
    db_session.add(device)
    db_session.flush()

    item = DahuaSyncQueue(
        run_id="r1", device_id=device.id, mindbody_client_id="42",
        action="enroll", status="success",
    )
    db_session.add(item)
    db_session.commit()

    rows = _get_recent_queue(db_session)

    assert len(rows) == 1
    queue_item, device_name = rows[0]
    assert queue_item.mindbody_client_id == "42"
    assert device_name == "Male Gate A"


def test_get_recent_queue_device_name_none_when_device_missing(db_session) -> None:
    item = DahuaSyncQueue(
        run_id="r1", device_id=9999, mindbody_client_id="1",
        action="enroll", status="pending",
    )
    db_session.add(item)
    db_session.commit()

    rows = _get_recent_queue(db_session)

    assert len(rows) == 1
    _, device_name = rows[0]
    assert device_name is None


def test_get_recent_queue_limited_to_10(db_session) -> None:
    db_session.add_all([
        DahuaSyncQueue(run_id="r", device_id=1, mindbody_client_id=str(i),
                       action="enroll", status="success")
        for i in range(15)
    ])
    db_session.commit()

    rows = _get_recent_queue(db_session)

    assert len(rows) == 10


def test_get_mb_breakdown_counts_gender_and_subscriptions(db_session) -> None:
    clients = [
        MindBodyClient(mindbody_id="1", first_name="A", last_name="B", active=True, gender="Male"),
        MindBodyClient(mindbody_id="2", first_name="C", last_name="D", active=True, gender="Female"),
        MindBodyClient(mindbody_id="3", first_name="E", last_name="F", active=True, gender="Female"),
        MindBodyClient(mindbody_id="4", first_name="G", last_name="H", active=True, gender=None),
    ]
    db_session.add_all(clients)
    db_session.flush()
    db_session.add(MindBodyMembership(mindbody_client_id="1", membership_id="m1", is_active=True))
    db_session.add(MindBodyMembership(mindbody_client_id="2", membership_id="m2", is_active=False))
    db_session.commit()

    bd = _get_mb_breakdown(db_session)

    assert bd["male_count"] == 1
    assert bd["female_count"] == 2
    assert bd["male_pct"] == 25   # 1/4 total
    assert bd["female_pct"] == 50  # 2/4 total
    assert bd["active_sub_count"] == 1
    assert bd["no_sub_count"] == 3


def test_get_device_rows_includes_queue_counts(db_session) -> None:
    d = DahuaDevice(
        name="Gate A", host="10.0.0.1", port=80,
        username="admin", password="pass", door_ids="0",
        is_enabled=True, status="online", gate_type="male",
    )
    db_session.add(d)
    db_session.flush()

    db_session.add_all([
        DahuaSyncQueue(run_id="r", device_id=d.id, mindbody_client_id="1",
                       action="enroll", status="pending"),
        DahuaSyncQueue(run_id="r", device_id=d.id, mindbody_client_id="2",
                       action="enroll", status="failed",
                       created_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1)),
        DahuaSyncQueue(run_id="r", device_id=d.id, mindbody_client_id="3",
                       action="enroll", status="failed",
                       created_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=25)),
    ])
    db_session.commit()

    rows = _get_device_rows(db_session)

    assert len(rows) == 1
    row = rows[0]
    assert row["device"].name == "Gate A"
    assert row["pending"] == 1
    assert row["failed_24h"] == 1


def test_get_device_rows_excludes_disabled(db_session) -> None:
    db_session.add(DahuaDevice(
        name="Disabled", host="10.0.0.2", port=80,
        username="admin", password="pass", door_ids="0",
        is_enabled=False,
    ))
    db_session.commit()

    rows = _get_device_rows(db_session)
    assert rows == []
