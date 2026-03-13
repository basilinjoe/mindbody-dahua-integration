from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class MemberDeviceEnrollment(Base):
    """Tracks which Dahua device each SyncedMember is enrolled on."""

    __tablename__ = "member_device_enrollments"
    __table_args__ = (
        UniqueConstraint("synced_member_id", "device_id", name="uq_member_device"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    synced_member_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("synced_members.id", ondelete="CASCADE"), nullable=False
    )
    device_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("dahua_devices.id", ondelete="CASCADE"), nullable=False
    )
    dahua_user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    # Stored for quick lookup — avoids joining synced_members on every deactivation
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    enrolled_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<MemberDeviceEnrollment member={self.synced_member_id} device={self.device_id} active={self.is_active}>"
