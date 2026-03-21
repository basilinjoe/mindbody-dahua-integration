from __future__ import annotations

from fastapi.testclient import TestClient


def test_sync_queue_list_no_filters(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/sync-queue")
    assert resp.status_code == 200


def test_sync_queue_list_with_action_filter(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue",
        params={"action": "enroll"},
    )
    assert resp.status_code == 200


def test_sync_queue_list_with_status_filter(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue",
        params={"status": "pending"},
    )
    assert resp.status_code == 200


def test_sync_queue_list_with_run_id_filter(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue",
        params={"run_id": "some-run-id"},
    )
    assert resp.status_code == 200


def test_sync_queue_list_with_device_id_filter(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue",
        params={"device_id": 1},
    )
    assert resp.status_code == 200


def test_sync_queue_list_with_all_filters(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get(
        "/admin/sync-queue",
        params={
            "run_id": "run-1",
            "action": "deactivate",
            "status": "failed",
            "device_id": 2,
        },
    )
    assert resp.status_code == 200
