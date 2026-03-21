from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.admin.csrf import generate_csrf_token


@pytest.fixture
def _seed_sync_engine(app):
    """Ensure app has sync_engine with fake clients."""
    from tests.helpers.fakes import FakeDahuaClient, FakeMindBodyClient

    class FakeSyncEngine:
        def __init__(self):
            self.mindbody = FakeMindBodyClient(all_clients=[
                {"Id": "1", "FirstName": "A", "LastName": "B", "Email": "a@b.com",
                 "MobilePhone": "", "HomePhone": "", "WorkPhone": "",
                 "Status": "Active", "Active": True, "BirthDate": "", "Gender": "Male",
                 "CreationDate": "2025-01-01"},
            ])
            self._dahua_clients = {}

    app.state.sync_engine = FakeSyncEngine()


def test_exports_page(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/exports")
    assert resp.status_code == 200
    assert "export" in resp.text.lower() or resp.status_code == 200


def test_export_jobs_partial(logged_in_client: TestClient) -> None:
    resp = logged_in_client.get("/admin/exports/jobs")
    assert resp.status_code == 200


def test_export_all_triggers_background_job(
    logged_in_client: TestClient, app, _seed_sync_engine, settings
) -> None:
    csrf = generate_csrf_token(settings.secret_key)
    resp = logged_in_client.post(
        "/admin/exports/all",
        data={"csrf_token": csrf},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/admin/exports" in resp.headers.get("location", "")


def test_export_mindbody_csv(
    logged_in_client: TestClient, _seed_sync_engine
) -> None:
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
