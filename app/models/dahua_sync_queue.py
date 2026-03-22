from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class DahuaSyncQueue(Base):
    __tablename__ = "dahua_sync_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    device_id: Mapped[int] = mapped_column(Integer, nullable=False)
    mindbody_client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(
        String(16), nullable=False
    )  # enroll | deactivate | reactivate
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending"
    )  # pending | success | failed

    # Execution context
    member_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON — enroll only
    dahua_user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    enrollment_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Result
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (Index("ix_dahua_sync_queue_run_status", "run_id", "status"),)

    def __repr__(self) -> str:
        return (
            f"<DahuaSyncQueue id={self.id} run={self.run_id!r} "
            f"action={self.action!r} status={self.status!r} device={self.device_id}>"
        )
