from __future__ import annotations

from types import SimpleNamespace

from app.api.deps import get_settings, get_sync_engine


def test_dependency_helpers_return_app_state_objects(settings) -> None:
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(sync_engine="engine", settings=settings))
    )

    assert get_sync_engine(request) == "engine"
    assert get_settings(request) is settings
