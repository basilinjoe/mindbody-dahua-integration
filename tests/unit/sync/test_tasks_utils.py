from __future__ import annotations

from app.sync.tasks import _make_card_no, _make_dahua_user_id


class TestMakeDahuaUserId:
    def test_numeric_string(self) -> None:
        assert _make_dahua_user_id("00123") == "123"

    def test_plain_number(self) -> None:
        assert _make_dahua_user_id("42") == "42"

    def test_non_numeric(self) -> None:
        assert _make_dahua_user_id("abc-123") == "abc-123"

    def test_empty_string(self) -> None:
        assert _make_dahua_user_id("") == ""


class TestMakeCardNo:
    def test_numeric(self) -> None:
        assert _make_card_no("100") == "MB00000100"

    def test_large_number(self) -> None:
        assert _make_card_no("12345678") == "MB12345678"

    def test_non_numeric_hashed(self) -> None:
        result = _make_card_no("abc")
        assert result.startswith("MB")
        assert len(result) == 10

    def test_leading_zeros(self) -> None:
        assert _make_card_no("007") == "MB00000007"
