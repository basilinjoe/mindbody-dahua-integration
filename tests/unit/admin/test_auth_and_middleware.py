from __future__ import annotations

from itsdangerous import URLSafeTimedSerializer


def test_protected_admin_route_redirects_without_session(client) -> None:
    resp = client.get("/admin/", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/login"


def test_login_success_sets_session_cookie_and_redirects(client) -> None:
    resp = client.post(
        "/admin/login",
        data={"username": "admin", "password": "changeme"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/"
    assert "session=" in resp.headers.get("set-cookie", "")


def test_login_failure_returns_error(client) -> None:
    resp = client.post(
        "/admin/login",
        data={"username": "admin", "password": "wrong-password"},
        follow_redirects=False,
    )
    assert resp.status_code == 200
    assert "Invalid username or password" in resp.text


def test_bad_signature_cookie_is_rejected(client) -> None:
    bad_token = URLSafeTimedSerializer("different-secret").dumps({"username": "admin"})
    client.cookies.set("session", bad_token)

    resp = client.get("/admin/", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/login"


def test_logout_clears_cookie(client) -> None:
    resp = client.get("/admin/logout", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/login"
    assert "session=" in resp.headers.get("set-cookie", "")
