from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from app.clients.dahua import DahuaClient
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient

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
        {
            "request": request,
            "session_user": request.state.user,
            "active_page": "devices",
            "device": None,
            "error": None,
        },
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
    gate_type: str = Form("all"),
    enable_integration: str = Form(""),
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
            gate_type=gate_type if gate_type in ("male", "female", "all") else "all",
            enable_integration=enable_integration == "1",
        )
        db.add(device)
        db.commit()
        logger.info("Added device: %s (%s)", name, host)
        return RedirectResponse(url="/admin/devices", status_code=303)
    except Exception as e:
        db.rollback()
        return request.app.state.templates.TemplateResponse(
            "devices/form.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "devices",
                "device": None,
                "error": str(e),
            },
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
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "devices",
                "device": device,
                "error": None,
            },
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
    gate_type: str = Form("all"),
    enable_integration: str = Form(""),
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
        device.gate_type = gate_type if gate_type in ("male", "female", "all") else "all"
        device.enable_integration = enable_integration == "1"
        db.commit()
        logger.info("Updated device: %s (%s)", name, host)
        return RedirectResponse(url="/admin/devices", status_code=303)
    except Exception as e:
        db.rollback()
        return request.app.state.templates.TemplateResponse(
            "devices/form.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "devices",
                "device": device,
                "error": str(e),
            },
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
                device.last_seen_at = datetime.now(UTC)
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
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
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
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
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

        # Build set of known MindBody IDs for cross-reference
        synced_ids = {row[0] for row in db.query(MindBodyClient.mindbody_id).all()}

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


@router.get("/{device_id}/users/{user_id}", response_class=HTMLResponse)
async def device_user_detail(request: Request, device_id: int, user_id: str):
    """Show details of a single user on a device."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return RedirectResponse(url="/admin/devices", status_code=303)

        # Look up MindBody client by user_id (which equals mindbody_id for integer IDs)
        synced_member = db.query(MindBodyClient).filter_by(mindbody_id=user_id).first()

        user = None
        error = None
        device_face_photo = None
        client = DahuaClient(
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
            door_ids=device.door_ids,
        )
        try:
            user = await client.get_user(user_id)
            if user:
                try:
                    device_face_photo = await client.get_face_photo(user_id)
                except Exception:
                    pass  # non-critical
        except Exception as e:
            error = f"Could not connect to device: {e}"
            logger.exception("Failed to fetch user %s from device %d", user_id, device_id)
        finally:
            await client.close()

        if not user and not error:
            error = f"User '{user_id}' not found on this device"

        return request.app.state.templates.TemplateResponse(
            "devices/user_detail.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "devices",
                "device": device,
                "user": user,
                "synced_member": synced_member,
                "device_face_photo": device_face_photo,
                "error": error,
            },
        )
    finally:
        db.close()


@router.post("/{device_id}/users/{user_id}/activate")
async def device_user_activate(request: Request, device_id: int, user_id: str):
    """Set a user's card status to Normal (0) on this device."""
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
            ok = await client.update_user_status(user_id, 0)
            if ok:
                logger.info("Activated user %s on %s", user_id, device.name)
            else:
                logger.warning("Failed to activate user %s on %s", user_id, device.name)
        finally:
            await client.close()
    finally:
        db.close()
    return RedirectResponse(url=f"/admin/devices/{device_id}/users/{user_id}", status_code=303)


@router.post("/{device_id}/users/{user_id}/freeze")
async def device_user_freeze(request: Request, device_id: int, user_id: str):
    """Set a user's card status to Frozen (4) on this device."""
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
            ok = await client.update_user_status(user_id, 4)
            if ok:
                logger.info("Froze user %s on %s", user_id, device.name)
            else:
                logger.warning("Failed to freeze user %s on %s", user_id, device.name)
        finally:
            await client.close()
    finally:
        db.close()
    return RedirectResponse(url=f"/admin/devices/{device_id}/users/{user_id}", status_code=303)


@router.post("/{device_id}/users/{user_id}/delete")
async def device_user_delete(request: Request, device_id: int, user_id: str):
    """Remove a user completely from this device."""
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
            ok = await client.remove_user(user_id)
            if ok:
                logger.info("Deleted user %s from %s", user_id, device.name)
            else:
                logger.warning("Failed to delete user %s from %s", user_id, device.name)
        finally:
            await client.close()
    finally:
        db.close()
    return RedirectResponse(url=f"/admin/devices/{device_id}/users", status_code=303)


@router.post("/{device_id}/snapshot")
async def device_snapshot(request: Request, device_id: int):
    """Capture a snapshot from the device camera. Returns JSON with base64 image."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device:
            return JSONResponse({"error": "Device not found"}, status_code=404)

        client = DahuaClient(
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
            door_ids=device.door_ids,
        )
        try:
            image_bytes = await client.capture_snapshot()
            if not image_bytes:
                return JSONResponse(
                    {"error": "Failed to capture snapshot from device"}, status_code=502
                )
            b64 = base64.b64encode(image_bytes).decode("ascii")
            return JSONResponse({"image": f"data:image/jpeg;base64,{b64}"})
        finally:
            await client.close()
    finally:
        db.close()
