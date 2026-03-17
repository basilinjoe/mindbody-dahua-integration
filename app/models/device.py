from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class DahuaDevice(Base):
    __tablename__ = "dahua_devices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    host: Mapped[str] = mapped_column(String(256), nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=80)
    username: Mapped[str] = mapped_column(String(64), default="admin")
    password: Mapped[str] = mapped_column(String(256), nullable=False)
    door_ids: Mapped[str] = mapped_column(String(64), default="0")
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    gate_type: Mapped[str] = mapped_column(String(32), default="all")
    # "male" | "female" | "all" — routing key for gender-based sync
    enable_integration: Mapped[bool] = mapped_column(Boolean, default=True)
    # When False, this device is excluded from all Prefect sync operations
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(
        String(16), default="unknown"
    )  # online / offline / error / unknown
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return f"<DahuaDevice {self.name} ({self.host})>"
