from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class SyncLog(Base):
    __tablename__ = "sync_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sync_type: Mapped[str] = mapped_column(
        String(32), nullable=False
    )  # full_poll / webhook / manual
    action: Mapped[str] = mapped_column(
        String(32), nullable=False
    )  # enroll / activate / deactivate / photo_upload / remove
    mindbody_client_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    member_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    device_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    device_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
