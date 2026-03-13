from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class MemberDeviceEnrollmentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    synced_member_id: int
    device_id: int
    dahua_user_id: str
    is_active: bool
    enrolled_at: datetime
    deactivated_at: datetime | None = None
