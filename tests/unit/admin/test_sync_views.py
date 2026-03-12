from __future__ import annotations

from tests.helpers.factories import make_sync_log


def test_sync_logs_filters_by_type_action_and_status(logged_in_client, db_session) -> None:
    db_session.add_all(
        [
            make_sync_log(sync_type="full_poll", action="enroll", success=True, mindbody_client_id="1", member_name="Ana"),
            make_sync_log(sync_type="webhook", action="deactivate", success=False, mindbody_client_id="2", member_name="Ben"),
            make_sync_log(sync_type="manual", action="reactivate", success=True, mindbody_client_id="3", member_name="Cara"),
        ]
    )
    db_session.commit()

    response = logged_in_client.get("/admin/sync?sync_type=webhook&action=deactivate&status=failed")

    assert response.status_code == 200
    assert "Ben" in response.text
    assert "Ana" not in response.text
    assert "Cara" not in response.text


def test_manual_trigger_schedules_full_sync(logged_in_client, app) -> None:
    calls: list[str] = []

    async def fake_full_sync() -> None:
        calls.append("full_sync")

    app.state.sync_engine.full_sync = fake_full_sync

    response = logged_in_client.post("/admin/sync/trigger", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/sync"
    assert calls == ["full_sync"]


def test_pause_and_resume_sync_routes(logged_in_client, app) -> None:
    events: list[str] = []

    class SchedulerSpy:
        @property
        def is_sync_paused(self) -> bool:
            return False

        def pause_sync(self) -> None:
            events.append("pause")

        def resume_sync(self) -> None:
            events.append("resume")

        def stop(self) -> None:
            return None

    app.state.scheduler = SchedulerSpy()

    pause_response = logged_in_client.post("/admin/sync/pause", follow_redirects=False)
    resume_response = logged_in_client.post("/admin/sync/resume", follow_redirects=False)

    assert pause_response.status_code == 303
    assert resume_response.status_code == 303
    assert events == ["pause", "resume"]
