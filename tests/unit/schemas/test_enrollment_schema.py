from __future__ import annotations

from datetime import datetime

from app.schemas.enrollment import MemberDeviceEnrollmentRead


class TestMemberDeviceEnrollmentRead:
    def test_from_attributes(self) -> None:
        class FakeEnrollment:
            id = 42
            synced_member_id = 10
            device_id = 5
            dahua_user_id = "100"
            is_active = True
            enrolled_at = datetime(2026, 1, 15, 10, 0, 0)
            deactivated_at = None

        e = MemberDeviceEnrollmentRead.model_validate(FakeEnrollment())
        assert e.id == 42
        assert e.synced_member_id == 10
        assert e.dahua_user_id == "100"
        assert e.is_active is True
        assert e.deactivated_at is None

    def test_with_deactivated_at(self) -> None:
        class FakeEnrollment:
            id = 1
            synced_member_id = 2
            device_id = 3
            dahua_user_id = "200"
            is_active = False
            enrolled_at = datetime(2026, 1, 1)
            deactivated_at = datetime(2026, 2, 1)

        e = MemberDeviceEnrollmentRead.model_validate(FakeEnrollment())
        assert e.is_active is False
        assert e.deactivated_at == datetime(2026, 2, 1)
