from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
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
    db: AsyncSession = Depends(get_async_db),
):
    page_size = 50

    q = (
        select(DahuaSyncQueue, MindBodyClient, DahuaDevice)
        .outerjoin(
            MindBodyClient,
            MindBodyClient.mindbody_id == DahuaSyncQueue.mindbody_client_id,
        )
        .outerjoin(DahuaDevice, DahuaDevice.id == DahuaSyncQueue.device_id)
    )

    if run_id:
        q = q.where(DahuaSyncQueue.run_id == run_id)
    if action:
        q = q.where(DahuaSyncQueue.action == action)
    if status:
        q = q.where(DahuaSyncQueue.status == status)
    if device_id:
        q = q.where(DahuaSyncQueue.device_id == device_id)

    total_result = await db.execute(
        select(DahuaSyncQueue.id)
        .outerjoin(
            MindBodyClient,
            MindBodyClient.mindbody_id == DahuaSyncQueue.mindbody_client_id,
        )
        .outerjoin(DahuaDevice, DahuaDevice.id == DahuaSyncQueue.device_id)
        .where(
            *(
                [DahuaSyncQueue.run_id == run_id] if run_id else []
                + ([DahuaSyncQueue.action == action] if action else [])
                + ([DahuaSyncQueue.status == status] if status else [])
                + ([DahuaSyncQueue.device_id == device_id] if device_id else [])
            )
        )
    )
    total = len(total_result.all())

    rows_result = await db.execute(
        q.order_by(DahuaSyncQueue.created_at.desc()).offset(offset).limit(page_size)
    )
    rows = rows_result.all()

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

    devices_result = await db.execute(
        select(DahuaDevice)
        .where(DahuaDevice.is_enabled.is_(True))
        .order_by(DahuaDevice.name)
    )
    devices = list(devices_result.scalars().all())

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
