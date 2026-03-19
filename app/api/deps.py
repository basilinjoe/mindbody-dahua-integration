from __future__ import annotations

from fastapi import Request

from app.models.database import get_async_db  # noqa: F401 — re-exported for Depends()


def get_sync_engine(request: Request):
    return request.app.state.sync_engine


def get_settings(request: Request):
    return request.app.state.settings
