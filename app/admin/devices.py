from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse

from app.clients.dahua import DahuaClient
from app.models.device import DahuaDevice
from app.models.member import SyncedMember

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/devices")


@router.get("", response_class=HTMLResponse)
async def device_list(request: Request):
    db = request.app.state.db_session_factory()
    try:
        devices = db.query(DahuaDevice).order_by(DahuaDevice.name).all()
        return request.app.state.templates.TemplateResponse(
            "devices/list.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "devices",
                "devices": devices,
            },
        )
    finally:
        db.close()


@router.get("/add", response_class=HTMLResponse)
async def device_add_form(request: Request):
    return request.app.state.templates.TemplateResponse(
        "devices/form.html",
        {"request": request, "session_user": request.state.user, "active_page": "devices", "device": None, "error": None},
    )


@router.post("/add")
async def device_add_submit(
    request: Request,
    name: str = Form(...),
    host: str = Form(...),
    port: int = Form(80),
    username: str = Form("admin"),
    password: str = Form(...),
    door_ids: str = Form("0"),
    is_enabled: str = Form(""),
):
    db = request.app.state.db_session_factory()
    try:
        device = DahuaDevice(
            name=name,
            host=host.strip(),
            port=port,
            username=username,
            password=password,
            door_ids=door_ids.strip(),
            is_enabled=is_enabled == "1",
        )
        db.add(device)
        db.commit()
        logger.info("Added device: %s (%s)", name, host)
        return RedirectResponse(url="/admin/devices", status_code=303)
    except Exception as e:
        db.rollback()
        return request.app.state.templates.TemplateResponse(
            "devices/form.html",
            {"request": request, "session_user": request.state.user, "active_page": "devices", "device": None, "error": str(e)},
        )
    finally:
        db.close()


@router.get("/{device_id}/edit", response_class=HTMLResponse)
async def device_edit_form(request: Request, device_id: int):
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)
        return request.app.state.templates.TemplateResponse(
            "devices/form.html",
            {"request": request, "session_user": request.state.user, "active_page": "devices", "device": device, "error": None},
        )
    finally:
        db.close()


@router.post("/{device_id}/edit")
async def device_edit_submit(
    request: Request,
    device_id: int,
    name: str = Form(...),
    host: str = Form(...),
    port: int = Form(80),
    username: str = Form("admin"),
    password: str = Form(""),
    door_ids: str = Form("0"),
    is_enabled: str = Form(""),
):
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)

        device.name = name
        device.host = host.strip()
        device.port = port
        device.username = username
        if password:
            device.password = password
        device.door_ids = door_ids.strip()
        device.is_enabled = is_enabled == "1"
        db.commit()
        logger.info("Updated device: %s (%s)", name, host)
        return RedirectResponse(url="/admin/devices", status_code=303)
    except Exception as e:
        db.rollback()
        return request.app.state.templates.TemplateResponse(
            "devices/form.html",
            {"request": request, "session_user": request.state.user, "active_page": "devices", "device": device, "error": str(e)},
        )
    finally:
        db.close()


@router.delete("/{device_id}")
async def device_delete(request: Request, device_id: int):
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if device:
            db.delete(device)
            db.commit()
            logger.info("Deleted device: %s", device.name)
    except Exception:
        db.rollback()
        logger.exception("Failed to delete device %d", device_id)
    finally:
        db.close()
    return RedirectResponse(url="/admin/devices", status_code=303)


@router.post("/{device_id}/health")
async def device_health_check(request: Request, device_id: int):
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)

        client = DahuaClient(
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
            door_ids=device.door_ids,
        )
        try:
            online = await client.health_check()
            device.status = "online" if online else "offline"
            if online:
                device.last_seen_at = datetime.now(timezone.utc)
            db.commit()
        finally:
            await client.close()
    except Exception:
        db.rollback()
        logger.exception("Health check failed for device %d", device_id)
    finally:
        db.close()

    return RedirectResponse(url="/admin/devices", status_code=303)


@router.post("/{device_id}/open-door")
async def device_open_door(request: Request, device_id: int):
    """Remotely open a gate/door."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)

        # Get door_id from query param, default to first configured door
        door_id = int(request.query_params.get("door", device.door_ids.split(",")[0].strip()))

        client = DahuaClient(
            host=device.host, port=device.port,
            username=device.username, password=device.password,
            door_ids=device.door_ids,
        )
        try:
            ok = await client.open_door(door_id)
            if ok:
                logger.info("Opened door %d on %s", door_id, device.name)
            else:
                logger.warning("Failed to open door %d on %s", door_id, device.name)
        finally:
            await client.close()
    finally:
        db.close()
    return RedirectResponse(url="/admin/devices", status_code=303)


@router.post("/{device_id}/close-door")
async def device_close_door(request: Request, device_id: int):
    """Remotely close a gate/door."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)

        door_id = int(request.query_params.get("door", device.door_ids.split(",")[0].strip()))

        client = DahuaClient(
            host=device.host, port=device.port,
            username=device.username, password=device.password,
            door_ids=device.door_ids,
        )
        try:
            ok = await client.close_door(door_id)
            if ok:
                logger.info("Closed door %d on %s", door_id, device.name)
            else:
                logger.warning("Failed to close door %d on %s", door_id, device.name)
        finally:
            await client.close()
    finally:
        db.close()
    return RedirectResponse(url="/admin/devices", status_code=303)


@router.get("/{device_id}/users", response_class=HTMLResponse)
async def device_users(request: Request, device_id: int):
    """List all users stored on a specific Dahua device."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)

        # Build set of synced UserIDs for cross-reference
        synced = db.query(SyncedMember.dahua_user_id).all()
        synced_ids = {row[0] for row in synced}

        users: list[dict] = []
        error = None

        client = DahuaClient(
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
            door_ids=device.door_ids,
        )
        try:
            users = await client.get_all_users()
        except Exception as e:
            error = f"Could not connect to device: {e}"
            logger.exception("Failed to fetch users from device %d", device_id)
        finally:
            await client.close()

        return request.app.state.templates.TemplateResponse(
            "devices/users.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "devices",
                "device": device,
                "users": users,
                "synced_ids": synced_ids,
                "error": error,
            },
        )
    finally:
        db.close()
