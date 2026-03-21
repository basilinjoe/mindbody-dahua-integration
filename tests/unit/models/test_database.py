from __future__ import annotations

from app.models.database import _to_async_url


class TestToAsyncUrl:
    def test_psycopg2_url(self) -> None:
        url = "postgresql+psycopg2://user:pass@host/db"
        assert _to_async_url(url) == "postgresql+asyncpg://user:pass@host/db"

    def test_plain_postgresql_url(self) -> None:
        url = "postgresql://user:pass@host/db"
        assert _to_async_url(url) == "postgresql+asyncpg://user:pass@host/db"

    def test_already_async_url(self) -> None:
        url = "postgresql+asyncpg://user:pass@host/db"
        assert _to_async_url(url) == url

    def test_sqlite_url_unchanged(self) -> None:
        url = "sqlite+aiosqlite:///"
        assert _to_async_url(url) == url
