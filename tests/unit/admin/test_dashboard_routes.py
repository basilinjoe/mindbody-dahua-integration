from __future__ import annotations

from tests.helpers.factories import make_member, make_sync_log


def test_dashboard_page_renders_for_logged_in_user(logged_in_client, db_session) -> None:
    db_session.add(make_member(mindbody_client_id="10", dahua_user_id="10"))
    db_session.add(make_sync_log(mindbody_client_id="10", member_name="John Doe"))
    db_session.commit()

    response = logged_in_client.get("/admin/")

    assert response.status_code == 200
    assert "John Doe" in response.text


def test_dashboard_stats_partial_renders(logged_in_client, db_session) -> None:
    db_session.add(make_member(mindbody_client_id="11", dahua_user_id="11", is_active_in_dahua=True))
    db_session.commit()

    response = logged_in_client.get("/admin/partials/stats")

    assert response.status_code == 200
    assert "Total Members" in response.text
