from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SyncedMemberRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    mindbody_client_id: str
    dahua_user_id: str
    card_no: str
    first_name: str
    last_name: str
    email: str | None = None
    gender: str | None = None
    is_active_in_mindbody: bool
    is_active_in_dahua: bool
    has_face_photo: bool
    face_photo_source: str | None = None
    is_manual: bool
    last_synced_at: datetime | None = None
    created_at: datetime
