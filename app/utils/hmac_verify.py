from __future__ import annotations

import hashlib
import hmac


def verify_mindbody_signature(body: bytes, signature: str, signature_key: str) -> bool:
    """
    Verify MindBody webhook HMAC-SHA256 signature.

    MindBody includes the signature in the X-Mindbody-Signature header.
    The signature_key is provided when creating a webhook subscription.
    """
    if not signature_key:
        return False  # Fail closed — no key means no valid signature
    expected = hmac.new(
        signature_key.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
