from __future__ import annotations

import csv
import io
import logging
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response

from app.models.device import DahuaDevice
from app.models.export_job import ExportJob, ExportStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/exports")

EXPORTS_DIR = Path(__file__).resolve().parent.parent / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# CSV builder helpers
# ---------------------------------------------------------------------------


def _build_mindbody_csv(clients: list[dict]) -> str:
    fieldnames = [
        "mindbody_id",
        "first_name",
        "last_name",
        "email",
        "mobile_phone",
        "home_phone",
        "work_phone",
        "status",
        "active",
        "birth_date",
        "gender",
        "created_at",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for c in clients:
        writer.writerow(
            {
                "mindbody_id": c.get("Id", ""),
                "first_name": c.get("FirstName", ""),
                "last_name": c.get("LastName", ""),
                "email": c.get("Email", ""),
                "mobile_phone": c.get("MobilePhone", ""),
                "home_phone": c.get("HomePhone", ""),
                "work_phone": c.get("WorkPhone", ""),
                "status": c.get("Status", ""),
                "active": c.get("Active", ""),
                "birth_date": c.get("BirthDate", ""),
                "gender": c.get("Gender", ""),
                "created_at": c.get("CreationDate", ""),
            }
        )
    return buf.getvalue()


def _build_dahua_csv(users: list[dict]) -> str:
    fieldnames = [
        "user_id",
        "card_name",
        "card_no",
        "card_status",
        "card_type",
        "valid_date_start",
        "valid_date_end",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for u in users:
        writer.writerow(
            {
                "user_id": u.get("UserID", ""),
                "card_name": u.get("CardName", ""),
                "card_no": u.get("CardNo", ""),
                "card_status": u.get("CardStatus", ""),
                "card_type": u.get("CardType", ""),
                "valid_date_start": u.get("ValidDateStart", ""),
                "valid_date_end": u.get("ValidDateEnd", ""),
            }
        )
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Background task
# ---------------------------------------------------------------------------


async def _run_export_job(job_id: int, db_session_factory, sync_engine) -> None:
    db = db_session_factory()
    try:
        job = db.query(ExportJob).get(job_id)
        job.status = ExportStatus.running
        job.started_at = datetime.now(UTC)
        db.commit()
    finally:
        db.close()

    db = db_session_factory()
    try:
        buffers: dict[str, str] = {}

        # MindBody
        clients = await sync_engine.mindbody.get_all_clients()
        buffers["mindbody_users.csv"] = _build_mindbody_csv(clients)

        # All enabled Dahua devices
        sync_engine.refresh_devices(db)
        for device_id, dahua_client in list(sync_engine._dahua_clients.items()):
            device = db.query(DahuaDevice).get(device_id)
            device_name = device.name if device else str(device_id)
            users = await dahua_client.get_all_users()
            safe_name = re.sub(r"[^\w\-]", "_", device_name)
            buffers[f"{safe_name}_users.csv"] = _build_dahua_csv(users)

        # Write ZIP
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        file_name = f"export_{timestamp}_{job_id}.zip"
        zip_path = EXPORTS_DIR / file_name
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for name, content in buffers.items():
                zf.writestr(name, content)

        job = db.query(ExportJob).get(job_id)
        job.status = ExportStatus.complete
        job.zip_path = str(zip_path)
        job.file_name = file_name
        job.finished_at = datetime.now(UTC)
        db.commit()
        logger.info("Export job %d complete: %s", job_id, file_name)

    except Exception as exc:
        logger.exception("Export job %d failed", job_id)
        db.rollback()
        fresh = db_session_factory()
        try:
            job = fresh.query(ExportJob).get(job_id)
            if job:
                job.status = ExportStatus.failed
                job.error_msg = str(exc)
                job.finished_at = datetime.now(UTC)
                fresh.commit()
        finally:
            fresh.close()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
async def exports_page(request: Request):
    """Dedicated exports page."""
    db = request.app.state.db_session_factory()
    try:
        jobs = db.query(ExportJob).order_by(ExportJob.created_at.desc()).limit(20).all()
        devices = db.query(DahuaDevice).filter_by(is_enabled=True).order_by(DahuaDevice.name).all()
        return request.app.state.templates.TemplateResponse(
            "exports/index.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "exports",
                "jobs": jobs,
                "devices": devices,
            },
        )
    finally:
        db.close()


@router.post("/all")
async def export_all(request: Request, background_tasks: BackgroundTasks):
    """Trigger a background export of all sources (MindBody + all Dahua devices)."""
    db = request.app.state.db_session_factory()
    try:
        job = ExportJob(status=ExportStatus.pending)
        db.add(job)
        db.commit()
        db.refresh(job)
        job_id = job.id
    finally:
        db.close()

    background_tasks.add_task(
        _run_export_job,
        job_id,
        request.app.state.db_session_factory,
        request.app.state.sync_engine,
    )
    return RedirectResponse(url="/admin/exports", status_code=303)


@router.get("/jobs", response_class=HTMLResponse)
async def export_jobs_partial(request: Request):
    """HTMX partial — returns the export jobs status panel."""
    db = request.app.state.db_session_factory()
    try:
        jobs = db.query(ExportJob).order_by(ExportJob.created_at.desc()).limit(20).all()
        return request.app.state.templates.TemplateResponse(
            "partials/export_jobs.html",
            {"request": request, "jobs": jobs},
        )
    finally:
        db.close()


@router.get("/mindbody.csv")
async def export_mindbody_csv(request: Request):
    """Download all MindBody clients as CSV."""
    engine = request.app.state.sync_engine
    clients = await engine.mindbody.get_all_clients()
    content = _build_mindbody_csv(clients)
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=mindbody_users.csv"},
    )


@router.get("/dahua/{device_id}.csv")
async def export_dahua_csv(request: Request, device_id: int):
    """Download all users from a single Dahua device as CSV."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).filter_by(id=device_id, is_enabled=True).first()
        if not device:
            return Response(content="Device not found or not enabled", status_code=404)
        device_name = device.name
    finally:
        db.close()

    engine = request.app.state.sync_engine
    engine.refresh_devices(db := request.app.state.db_session_factory())
    db.close()

    dahua_client = engine._dahua_clients.get(device_id)
    if not dahua_client:
        return Response(content="Device client not available", status_code=404)

    users = await dahua_client.get_all_users()
    content = _build_dahua_csv(users)
    safe_name = re.sub(r"[^\w\-]", "_", device_name)
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={safe_name}_users.csv"},
    )


@router.get("/{job_id}/download")
async def export_download(request: Request, job_id: int):
    """Download the ZIP for a completed export job."""
    db = request.app.state.db_session_factory()
    try:
        job = db.query(ExportJob).get(job_id)
        if not job or job.status != ExportStatus.complete or not job.zip_path:
            return Response(content="Export not ready", status_code=404)
        path = Path(job.zip_path)
        if not path.exists():
            return Response(content="Export file has expired", status_code=410)
        return FileResponse(
            path=str(path),
            media_type="application/zip",
            filename=job.file_name or path.name,
        )
    finally:
        db.close()
