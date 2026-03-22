from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class MindBodyMembership(Base):
    __tablename__ = "mindbody_memberships"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    mindbody_client_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    membership_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    membership_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    start_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expiration_date: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    last_synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("mindbody_client_id", "membership_id", name="uq_mb_client_membership"),
    )

    def __repr__(self) -> str:
        return (
            f"<MindBodyMembership {self.membership_name!r} "
            f"client={self.mindbody_client_id} active={self.is_active}>"
        )
