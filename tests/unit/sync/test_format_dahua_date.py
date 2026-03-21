"""Tests for _format_dahua_date including malformed date logging."""

from __future__ import annotations

from app.sync.tasks import _format_dahua_date


def test_valid_iso_date_converts() -> None:
    assert _format_dahua_date("2026-01-15T10:30:00Z") == "2026-01-15 10:30:00"


def test_valid_iso_with_offset() -> None:
    assert _format_dahua_date("2026-06-01T00:00:00+00:00") == "2026-06-01 00:00:00"


def test_none_returns_none() -> None:
    assert _format_dahua_date(None) is None


def test_empty_string_returns_none() -> None:
    assert _format_dahua_date("") is None


def test_malformed_date_returns_none_and_logs(caplog) -> None:
    result = _format_dahua_date("not-a-date")
    assert result is None
    assert "Malformed date" in caplog.text


def test_invalid_date_values_returns_none(caplog) -> None:
    result = _format_dahua_date("2026-13-45T99:99:99Z")
    assert result is None
    assert "Malformed date" in caplog.text
