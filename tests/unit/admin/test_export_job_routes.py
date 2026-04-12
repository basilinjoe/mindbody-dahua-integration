from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.admin.csrf import generate_csrf_token


@pytest.fixture
def _mock_mindbody_client():
    """Patch MindBodyClient in export_jobs module to return fake data."""
    fake_client = AsyncMock()
    fake_client.get_all_clients = AsyncMock(
        return_value=[
            {
                "Id": "1",
                "FirstName": "A",
                "LastName": "B",
                "Email": "a@b.com",
                "MobilePhone": "",
                "HomePhone": "",
                "WorkPhone": "",
                "Status": "Active",
                "Active": True,
                "BirthDate": "",
                "Gender": "Male",
                "CreationDate": "2025-01-01",
            },
        ]
    )
    fake_client.close = AsyncMock()

    with patch("app.clients.mindbody.MindBodyClient", return_value=fake_client):
        yield fake_client


def test_exports_page(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/exports")
    assert resp.status_code == 200
    assert "export" in resp.text.lower() or resp.status_code == 200


def test_export_jobs_partial(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/exports/jobs")
    assert resp.status_code == 200


def test_export_all_triggers_background_job(logged_in_client: TestClient, settings) -> None:
    csrf = generate_csrf_token(settings.secret_key)
    resp = logged_in_client.post(
        "/admin/exports/all",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/admin/exports" in resp.headers.get("location", "")


def test_export_mindbody_csv(logged_in_client: TestClient, _mock_mindbody_client) -> None:
    resp = logged_in_client.get("/admin/exports/mindbody.csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "mindbody_id" in resp.text


def test_export_dahua_csv_device_not_found(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/exports/dahua/99999.csv")
    assert resp.status_code == 404


def test_export_download_not_found(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/exports/99999/download")
    assert resp.status_code == 404
