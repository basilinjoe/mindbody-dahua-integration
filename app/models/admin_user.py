from __future__ import annotations

from datetime import datetime

from passlib.hash import bcrypt
from sqlalchemy import Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class AdminUser(Base):
    __tablename__ = "admin_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def set_password(self, raw_password: str) -> None:
        self.password_hash = bcrypt.hash(raw_password)

    def verify_password(self, raw_password: str) -> bool:
        return bcrypt.verify(raw_password, self.password_hash)
