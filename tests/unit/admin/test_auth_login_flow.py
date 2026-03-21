from __future__ import annotations

from fastapi.testclient import TestClient

from app.config import Settings


def test_login_page_renders(client: TestClient) -> None:
    resp = client.get("/admin/login")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_login_success_sets_cookie(client: TestClient, settings: Settings) -> None:
    resp = client.post(
        "/admin/login",
        data={"username": settings.admin_username, "password": settings.admin_password},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "session" in resp.cookies


def test_login_failure_returns_error(client: TestClient) -> None:
    resp = client.post(
        "/admin/login",
        data={"username": "wrong", "password": "wrong"},
        follow_redirects=False,
    )
    # Should return login page with error, not redirect
    assert resp.status_code == 200
    assert "Invalid" in resp.text or "invalid" in resp.text.lower()


def test_logout_clears_cookie(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/logout", follow_redirects=False)
    assert resp.status_code == 303
    assert "/admin/login" in resp.headers.get("location", "")
