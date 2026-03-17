from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient

router = APIRouter(prefix="/sync-queue")


@router.get("", response_class=HTMLResponse)
async def sync_queue_list(
    request: Request,
    run_id: str = "",
    action: str = "",
    status: str = "",
    device_id: int = 0,
    offset: int = 0,
):
    db = request.app.state.db_session_factory()
    page_size = 50
    try:
        # Join queue → mindbody_clients (for name) and → dahua_devices (for device name)
        q = (
            db.query(DahuaSyncQueue, MindBodyClient, DahuaDevice)
            .outerjoin(
                MindBodyClient,
                MindBodyClient.mindbody_id == DahuaSyncQueue.mindbody_client_id,
            )
            .outerjoin(DahuaDevice, DahuaDevice.id == DahuaSyncQueue.device_id)
        )

        if run_id:
            q = q.filter(DahuaSyncQueue.run_id == run_id)
        if action:
            q = q.filter(DahuaSyncQueue.action == action)
        if status:
            q = q.filter(DahuaSyncQueue.status == status)
        if device_id:
            q = q.filter(DahuaSyncQueue.device_id == device_id)

        total = q.count()
        rows = q.order_by(DahuaSyncQueue.created_at.desc()).offset(offset).limit(page_size).all()

        # Flatten into dicts for the template
        items = [
            {
                "id": queue_item.id,
                "run_id": queue_item.run_id,
                "mindbody_client_id": queue_item.mindbody_client_id,
                "client_name": client.full_name if client else "—",
                "device_id": queue_item.device_id,
                "device_name": device.name if device else str(queue_item.device_id),
                "action": queue_item.action,
                "status": queue_item.status,
                "error_message": queue_item.error_message,
                "created_at": queue_item.created_at,
                "processed_at": queue_item.processed_at,
            }
            for queue_item, client, device in rows
        ]

        # All devices for the filter dropdown
        devices = (
            db.query(DahuaDevice)
            .filter(DahuaDevice.is_enabled.is_(True))
            .order_by(DahuaDevice.name)
            .all()
        )

        return request.app.state.templates.TemplateResponse(
            request,
            "sync_queue/list.html",
            {
                "session_user": request.state.user,
                "active_page": "sync_queue",
                "items": items,
                "total": total,
                "offset": offset,
                "page_size": page_size,
                "run_id_filter": run_id,
                "action_filter": action,
                "status_filter": status,
                "device_id_filter": device_id,
                "devices": devices,
            },
        )
    finally:
        db.close()
