from __future__ import annotations

from app.models.mindbody_client import MindBodyClient


def test_dashboard_page_renders_for_logged_in_user(logged_in_client, db_session) -> None:
    db_session.add(
        MindBodyClient(mindbody_id="10", first_name="John", last_name="Doe", active=True)
    )
    db_session.commit()

    response = logged_in_client.get("/admin/")

    assert response.status_code == 200
    assert "Total Members" in response.text


def test_dashboard_stats_partial_renders(logged_in_client, db_session) -> None:
    db_session.add(
        MindBodyClient(mindbody_id="11", first_name="Jane", last_name="Doe", active=True)
    )
    db_session.commit()

    response = logged_in_client.get("/admin/partials/stats")

    assert response.status_code == 200
    assert "Total Members" in response.text
