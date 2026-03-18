from __future__ import annotations

import hashlib
import hmac

from app.utils.hmac_verify import verify_mindbody_signature


def test_verify_signature_returns_false_when_key_not_configured() -> None:
    """No key means fail closed — reject all requests."""
    assert verify_mindbody_signature(b"payload", "anything", "") is False


def test_verify_signature_valid_hmac() -> None:
    body = b'{"eventId":"client.updated"}'
    key = "secret-key"
    signature = hmac.new(key.encode("utf-8"), body, hashlib.sha256).hexdigest()
    assert verify_mindbody_signature(body, signature, key) is True


def test_verify_signature_invalid_hmac() -> None:
    body = b'{"eventId":"client.updated"}'
    assert verify_mindbody_signature(body, "bad-signature", "secret-key") is False
