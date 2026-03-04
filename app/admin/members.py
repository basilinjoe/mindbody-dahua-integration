from __future__ import annotations

import csv
import io
import logging
import re
import uuid

from fastapi import APIRouter, BackgroundTasks, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.sync_log import SyncLog
from app.utils.photo import process_photo_for_dahua

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/members")


@router.get("", response_class=HTMLResponse)
async def member_list(
    request: Request,
    search: str = "",
    filter: str = "",
    offset: int = 0,
):
    db = request.app.state.db_session_factory()
    page_size = 25
    try:
        q = db.query(SyncedMember)
        if search:
            q = q.filter(
                (SyncedMember.first_name.ilike(f"%{search}%"))
                | (SyncedMember.last_name.ilike(f"%{search}%"))
                | (SyncedMember.email.ilike(f"%{search}%"))
            )
        if filter == "active":
            q = q.filter_by(is_active_in_dahua=True)
        elif filter == "inactive":
            q = q.filter_by(is_active_in_dahua=False)
        elif filter == "missing_photo":
            q = q.filter_by(has_face_photo=False, is_active_in_dahua=True)
        elif filter == "manual":
            q = q.filter_by(is_manual=True)

        total = q.count()
        members = q.order_by(SyncedMember.last_name).offset(offset).limit(page_size).all()
        devices = db.query(DahuaDevice).filter_by(is_enabled=True).order_by(DahuaDevice.name).all()

        return request.app.state.templates.TemplateResponse(
            "members/list.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "members",
                "members": members,
                "total": total,
                "search": search,
                "filter": filter,
                "offset": offset,
                "page_size": page_size,
                "devices": devices,
            },
        )
    finally:
        db.close()


@router.get("/add", response_class=HTMLResponse)
async def member_add_form(request: Request):
    db = request.app.state.db_session_factory()
    try:
        devices = db.query(DahuaDevice).filter_by(is_enabled=True).order_by(DahuaDevice.name).all()
        return request.app.state.templates.TemplateResponse(
            "members/add.html",
            {"request": request, "session_user": request.state.user, "active_page": "members", "error": None, "devices": devices},
        )
    finally:
        db.close()


@router.post("/add")
async def member_add_submit(
    request: Request,
    first_name: str = Form(...),
    last_name: str = Form(...),
    email: str = Form(""),
    member_id: str = Form(...),
    photo: UploadFile | None = File(None),
    captured_photo: str = Form(""),
):
    db = request.app.state.db_session_factory()
    templates = request.app.state.templates
    devices = db.query(DahuaDevice).filter_by(is_enabled=True).order_by(DahuaDevice.name).all()
    ctx = {"request": request, "session_user": request.state.user, "active_page": "members", "devices": devices}

    try:
        # Check for duplicate
        exists = db.query(SyncedMember).filter_by(mindbody_client_id=member_id).first()
        if exists:
            return templates.TemplateResponse("members/add.html", {**ctx, "error": f"Member ID '{member_id}' already exists"})

        photo_b64 = None
        if captured_photo:
            # Photo captured from device camera (base64 data URI)
            import base64 as b64mod
            raw = captured_photo.split(",", 1)[-1] if "," in captured_photo else captured_photo
            photo_bytes = b64mod.b64decode(raw)
            photo_b64 = process_photo_for_dahua(photo_bytes, request.app.state.settings.photo_max_size_kb)
        elif photo and photo.size and photo.size > 0:
            photo_bytes = await photo.read()
            photo_b64 = process_photo_for_dahua(photo_bytes, request.app.state.settings.photo_max_size_kb)

        engine = request.app.state.sync_engine
        member = await engine.enroll_manual_member(
            first_name=first_name,
            last_name=last_name,
            email=email or None,
            manual_id=member_id,
            photo_base64=photo_b64,
            db=db,
        )
        db.commit()
        return RedirectResponse(url=f"/admin/members/{member.id}", status_code=303)
    except Exception as e:
        db.rollback()
        logger.exception("Failed to add manual member")
        return templates.TemplateResponse("members/add.html", {**ctx, "error": str(e)})
    finally:
        db.close()


@router.get("/export/mindbody.csv")
async def export_mindbody_csv(request: Request):
    """Download all MindBody clients as CSV."""
    engine = request.app.state.sync_engine
    clients = await engine.mindbody.get_all_clients()

    fieldnames = [
        "mindbody_id", "first_name", "last_name", "email",
        "mobile_phone", "home_phone", "work_phone",
        "status", "active", "birth_date", "gender", "created_at", "photo_url",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for c in clients:
        writer.writerow({
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
            "photo_url": c.get("PhotoUrl", ""),
        })

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=mindbody_users.csv"},
    )


@router.get("/export/dahua/{device_id}.csv")
async def export_dahua_csv(request: Request, device_id: int):
    """Download all users from a single Dahua device as CSV."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).filter_by(id=device_id, is_enabled=True).first()
        if not device:
            from fastapi.responses import Response
            return Response(content="Device not found or not enabled", status_code=404)
        device_name = device.name
    finally:
        db.close()

    engine = request.app.state.sync_engine
    engine.refresh_devices(db := request.app.state.db_session_factory())
    db.close()

    dahua_client = engine._dahua_clients.get(device_id)
    if not dahua_client:
        from fastapi.responses import Response
        return Response(content="Device client not available", status_code=404)

    users = await dahua_client.get_all_users()

    fieldnames = [
        "user_id", "card_name", "card_no", "card_status",
        "card_type", "valid_date_start", "valid_date_end",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for u in users:
        writer.writerow({
            "user_id": u.get("UserID", ""),
            "card_name": u.get("CardName", ""),
            "card_no": u.get("CardNo", ""),
            "card_status": u.get("CardStatus", ""),
            "card_type": u.get("CardType", ""),
            "valid_date_start": u.get("ValidDateStart", ""),
            "valid_date_end": u.get("ValidDateEnd", ""),
        })

    safe_name = re.sub(r"[^\w\-]", "_", device_name)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={safe_name}_users.csv"},
    )


@router.get("/{member_id}", response_class=HTMLResponse)
async def member_detail(request: Request, member_id: int):
    db = request.app.state.db_session_factory()
    try:
        member = db.query(SyncedMember).get(member_id)
        if not member:
            return RedirectResponse(url="/admin/members", status_code=303)

        logs = (
            db.query(SyncLog)
            .filter_by(mindbody_client_id=member.mindbody_client_id)
            .order_by(SyncLog.created_at.desc())
            .limit(20)
            .all()
        )

        return request.app.state.templates.TemplateResponse(
            "members/detail.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "members",
                "member": member,
                "logs": logs,
            },
        )
    finally:
        db.close()


@router.post("/{member_id}/photo")
async def member_upload_photo(request: Request, member_id: int, photo: UploadFile = File(...)):
    db = request.app.state.db_session_factory()
    try:
        member = db.query(SyncedMember).get(member_id)
        if not member:
            return RedirectResponse(url="/admin/members", status_code=303)

        photo_bytes = await photo.read()
        b64 = process_photo_for_dahua(photo_bytes, request.app.state.settings.photo_max_size_kb)
        if not b64:
            return RedirectResponse(url=f"/admin/members/{member_id}", status_code=303)

        engine = request.app.state.sync_engine
        success = await engine.upload_photo_to_all_devices(member, b64)

        member.has_face_photo = success
        member.face_photo_source = "manual" if success else member.face_photo_source
        engine._log(db, "manual", "photo_upload", member.mindbody_client_id, member.full_name, success)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Photo upload failed for member %d", member_id)
    finally:
        db.close()

    return RedirectResponse(url=f"/admin/members/{member_id}", status_code=303)


@router.post("/{member_id}/activate")
async def member_activate(request: Request, member_id: int):
    db = request.app.state.db_session_factory()
    try:
        member = db.query(SyncedMember).get(member_id)
        if member:
            engine = request.app.state.sync_engine
            engine.refresh_devices(db)
            import asyncio
            results = await asyncio.gather(
                *[dc.update_user_status(member.dahua_user_id, 0) for dc in engine.get_dahua_clients()],
                return_exceptions=True,
            )
            if any(r is True for r in results):
                member.is_active_in_dahua = True
                engine._log(db, "manual", "reactivate", member.mindbody_client_id, member.full_name, True)
            db.commit()
    except Exception:
        db.rollback()
        logger.exception("Activate failed for member %d", member_id)
    finally:
        db.close()
    return RedirectResponse(url=f"/admin/members/{member_id}", status_code=303)


@router.post("/{member_id}/deactivate")
async def member_deactivate(request: Request, member_id: int):
    db = request.app.state.db_session_factory()
    try:
        member = db.query(SyncedMember).get(member_id)
        if member:
            engine = request.app.state.sync_engine
            engine.refresh_devices(db)
            import asyncio
            results = await asyncio.gather(
                *[dc.update_user_status(member.dahua_user_id, 4) for dc in engine.get_dahua_clients()],
                return_exceptions=True,
            )
            if any(r is True for r in results):
                member.is_active_in_dahua = False
                engine._log(db, "manual", "deactivate", member.mindbody_client_id, member.full_name, True)
            db.commit()
    except Exception:
        db.rollback()
        logger.exception("Deactivate failed for member %d", member_id)
    finally:
        db.close()
    return RedirectResponse(url=f"/admin/members/{member_id}", status_code=303)


@router.post("/{member_id}/sync")
async def member_force_sync(request: Request, member_id: int, background_tasks: BackgroundTasks):
    db = request.app.state.db_session_factory()
    try:
        member = db.query(SyncedMember).get(member_id)
        if member and not member.is_manual:
            engine = request.app.state.sync_engine
            background_tasks.add_task(engine.sync_single_member, member.mindbody_client_id, "manual")
    finally:
        db.close()
    return RedirectResponse(url=f"/admin/members/{member_id}", status_code=303)
