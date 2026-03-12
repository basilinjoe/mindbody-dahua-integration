from __future__ import annotations

from app.models.admin_user import AdminUser


def test_admin_user_password_hash_and_verify() -> None:
    user = AdminUser(username="admin")
    user.set_password("super-secret")

    assert user.password_hash != "super-secret"
    assert user.verify_password("super-secret") is True
    assert user.verify_password("wrong") is False
