from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from app.models.database import get_db


def get_sync_engine(request: Request):
    return request.app.state.sync_engine


def get_settings(request: Request):
    return request.app.state.settings


def get_db_session(request: Request) -> Session:
    return next(get_db())
