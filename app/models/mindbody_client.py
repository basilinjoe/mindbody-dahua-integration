from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.database import Base

if TYPE_CHECKING:
    from app.models.mindbody_membership import MindBodyMembership


class MindBodyClient(Base):
    __tablename__ = "mindbody_clients"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mindbody_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    unique_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str] = mapped_column(String(128), default="")
    last_name: Mapped[str] = mapped_column(String(128), default="")
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    mobile_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    home_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    work_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=False)
    birth_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    gender: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at_mb: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_modified_at_mb: Mapped[str | None] = mapped_column(String(64), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    last_fetched_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    memberships: Mapped[list[MindBodyMembership]] = relationship(
        "MindBodyMembership",
        primaryjoin="MindBodyClient.mindbody_id == foreign(MindBodyMembership.mindbody_client_id)",
        lazy="selectin",
    )

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def __repr__(self) -> str:
        return f"<MindBodyClient {self.full_name} ({self.mindbody_id})>"
