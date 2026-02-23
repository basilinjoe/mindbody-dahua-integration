from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class SyncedMember(Base):
    __tablename__ = "synced_members"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mindbody_client_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    dahua_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    card_no: Mapped[str] = mapped_column(String(32), nullable=False)
    first_name: Mapped[str] = mapped_column(String(128), default="")
    last_name: Mapped[str] = mapped_column(String(128), default="")
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_active_in_mindbody: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active_in_dahua: Mapped[bool] = mapped_column(Boolean, default=False)
    has_face_photo: Mapped[bool] = mapped_column(Boolean, default=False)
    face_photo_source: Mapped[str | None] = mapped_column(String(32), nullable=True)  # mindbody / manual
    is_manual: Mapped[bool] = mapped_column(Boolean, default=False)  # manually added (not from MindBody)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def __repr__(self) -> str:
        return f"<SyncedMember {self.full_name} ({self.mindbody_client_id})>"
