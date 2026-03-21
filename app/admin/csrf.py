"""CSRF protection for admin routes using itsdangerous HMAC tokens."""

from __future__ import annotations

import secrets

from itsdangerous import BadSignature, URLSafeTimedSerializer

# Token validity: 8 hours (matches default session expiry)
_CSRF_MAX_AGE = 8 * 3600


def generate_csrf_token(secret_key: str, session_id: str | None = None) -> str:
    """Generate a signed CSRF token bound to the session."""
    s = URLSafeTimedSerializer(secret_key, salt="csrf-token")
    payload = session_id or secrets.token_hex(16)
    return s.dumps(payload)


def validate_csrf_token(token: str, secret_key: str) -> bool:
    """Return True if the CSRF token is valid and not expired."""
    if not token:
        return False
    s = URLSafeTimedSerializer(secret_key, salt="csrf-token")
    try:
        s.loads(token, max_age=_CSRF_MAX_AGE)
        return True
    except BadSignature:
        return False
