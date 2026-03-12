from __future__ import annotations

import httpx
import pytest
from tenacity import wait_none

from app.utils import retry as retry_utils


def test_mindbody_retry_retries_on_timeout() -> None:
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise httpx.TimeoutException("transient")
        return "ok"

    wrapped = retry_utils.mindbody_retry(flaky)
    wrapped.retry.wait = wait_none()

    assert wrapped() == "ok"
    assert attempts["count"] == 3


def test_mindbody_retry_stops_after_max_attempts() -> None:
    attempts = {"count": 0}

    def always_timeout():
        attempts["count"] += 1
        raise httpx.TimeoutException("still failing")

    wrapped = retry_utils.mindbody_retry(always_timeout)
    wrapped.retry.wait = wait_none()

    with pytest.raises(httpx.TimeoutException):
        wrapped()
    assert attempts["count"] == 3


def test_mindbody_retry_does_not_retry_non_retriable_exception() -> None:
    attempts = {"count": 0}

    def fail_fast():
        attempts["count"] += 1
        raise ValueError("bad input")

    wrapped = retry_utils.mindbody_retry(fail_fast)
    wrapped.retry.wait = wait_none()

    with pytest.raises(ValueError):
        wrapped()
    assert attempts["count"] == 1


def test_dahua_retry_retries_connect_errors() -> None:
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 5:
            raise httpx.ConnectError("not reachable")
        return True

    wrapped = retry_utils.dahua_retry(flaky)
    wrapped.retry.wait = wait_none()

    assert wrapped() is True
    assert attempts["count"] == 5
