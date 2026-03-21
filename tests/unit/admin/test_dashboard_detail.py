from __future__ import annotations

from fastapi.testclient import TestClient


def test_dashboard_root_returns_html(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_stats_partial_returns_html(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/partials/stats")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
