from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.admin_user import AdminUser


async def get_by_username(db: AsyncSession, username: str) -> AdminUser | None:
    """Return an active AdminUser by username, or None if not found / inactive."""
    result = await db.execute(
        select(AdminUser).where(
            AdminUser.username == username,
            AdminUser.is_active.is_(True),
        )
    )
    return result.scalar_one_or_none()
