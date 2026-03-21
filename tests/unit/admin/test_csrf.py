"""Tests for CSRF token generation and validation."""

from __future__ import annotations

from app.admin.csrf import generate_csrf_token, validate_csrf_token

_SECRET = "test-secret-key-for-csrf"


def test_generate_and_validate_csrf_token() -> None:
    token = generate_csrf_token(_SECRET)
    assert isinstance(token, str)
    assert validate_csrf_token(token, _SECRET)


def test_invalid_csrf_token_rejected() -> None:
    assert not validate_csrf_token("totally-bogus-token", _SECRET)


def test_empty_csrf_token_rejected() -> None:
    assert not validate_csrf_token("", _SECRET)


def test_wrong_secret_rejects_token() -> None:
    token = generate_csrf_token(_SECRET)
    assert not validate_csrf_token(token, "different-secret")
