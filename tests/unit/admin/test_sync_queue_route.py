from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_sync_queue_list_no_filters(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/sync-queue")
    assert resp.status_code == 200


def test_sync_queue_list_with_filters(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue",
        params={"action": "enroll", "status": "pending"},
    )
    assert resp.status_code == 200
