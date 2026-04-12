from __future__ import annotations

import csv
import io
import json
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.clients.dahua import DahuaClient
from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient
from app.services import devices as devices_svc
from app.services import queue as queue_svc
from app.sync.tasks import _make_card_no, _make_dahua_user_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sync-queue")


# ---------------------------------------------------------------------------
# Shared query builder
# ---------------------------------------------------------------------------


def _build_filtered_query(run_id: str, action: str, status: str, device_id: int, flow_type: str):
    """Return a select() with joins + filters applied (no ordering/pagination)."""
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
    if flow_type:
        q = q.where(DahuaSyncQueue.flow_type == flow_type)
    return q


def _row_to_dict(queue_item: DahuaSyncQueue, client, device) -> dict:
    return {
        "id": queue_item.id,
        "run_id": queue_item.run_id,
        "flow_type": queue_item.flow_type,
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


# ---------------------------------------------------------------------------
# List view
# ---------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
async def sync_queue_list(
    request: Request,
    run_id: str = "",
    action: str = "",
    status: str = "",
    device_id: int = 0,
    flow_type: str = "",
    offset: int = 0,
    db: AsyncSession = Depends(get_async_db),
):
    page_size = 50
    q = _build_filtered_query(run_id, action, status, device_id, flow_type)

    # Count total matching rows using the same filter builder
    count_q = _build_filtered_query(run_id, action, status, device_id, flow_type).with_only_columns(
        DahuaSyncQueue.id
    )
    total_result = await db.execute(count_q)
    total = len(total_result.all())

    rows_result = await db.execute(
        q.order_by(DahuaSyncQueue.created_at.desc()).offset(offset).limit(page_size)
    )
    items = [_row_to_dict(*row) for row in rows_result.all()]

    devices_result = await db.execute(
        select(DahuaDevice).where(DahuaDevice.is_enabled.is_(True)).order_by(DahuaDevice.name)
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
            "flow_type_filter": flow_type,
            "devices": devices,
        },
    )


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

_CSV_FIELDS = [
    "id",
    "run_id",
    "mindbody_client_id",
    "client_name",
    "device_name",
    "action",
    "status",
    "error_message",
    "created_at",
    "processed_at",
]


@router.get("/export.csv")
async def sync_queue_export_csv(
    run_id: str = "",
    action: str = "",
    status: str = "",
    device_id: int = 0,
    flow_type: str = "",
    db: AsyncSession = Depends(get_async_db),
):
    q = _build_filtered_query(run_id, action, status, device_id, flow_type)
    rows_result = await db.execute(q.order_by(DahuaSyncQueue.created_at.desc()))
    items = [_row_to_dict(*row) for row in rows_result.all()]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for item in items:
        writer.writerow(
            {
                **item,
                "created_at": item["created_at"].isoformat() if item["created_at"] else "",
                "processed_at": item["processed_at"].isoformat() if item["processed_at"] else "",
            }
        )

    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sync_queue.csv"},
    )


# ---------------------------------------------------------------------------
# Push single item
# ---------------------------------------------------------------------------


async def _execute_push(item: DahuaSyncQueue, db: AsyncSession) -> tuple[bool, str | None]:
    """Execute a single queue item against its Dahua device. Returns (success, error_msg)."""
    device = await devices_svc.get_by_id(db, item.device_id)
    if device is None:
        return False, f"Device {item.device_id} not found"

    client = DahuaClient(
        host=device.host,
        port=device.port,
        username=device.username,
        password=device.password,
        door_ids=device.door_ids,
    )
    try:
        if item.action == "enroll":
            member = json.loads(item.member_snapshot or "{}")
            client_id = str(member.get("Id", ""))
            user_id = _make_dahua_user_id(client_id)
            card_no = _make_card_no(client_id)
            first_name = member.get("FirstName", "")
            last_name = member.get("LastName", "")
            card_name = f"{first_name} {last_name}".strip() or f"Member-{client_id}"
            success = await client.add_user(
                user_id=user_id,
                card_name=card_name,
                card_no=card_no,
                valid_start=member.get("valid_start"),
                valid_end=member.get("valid_end"),
            )
        elif item.action == "deactivate":
            success = await client.update_user_status(item.dahua_user_id, card_status=4)
        elif item.action == "reactivate":
            success = await client.update_user_status(item.dahua_user_id, card_status=0)
        elif item.action == "update":
            snapshot = json.loads(item.member_snapshot or "{}")
            success = await client.update_user(
                item.dahua_user_id,
                card_name=snapshot.get("card_name"),
                valid_start=snapshot.get("valid_start"),
                valid_end=snapshot.get("valid_end"),
            )
        else:
            return False, f"Unknown action: {item.action!r}"

        if success:
            return True, None
        return False, "Device returned failure"
    except Exception as exc:
        return False, str(exc)
    finally:
        await client.close()


@router.post("/{item_id}/push")
async def sync_queue_push_item(
    request: Request,
    item_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    result = await db.execute(select(DahuaSyncQueue).where(DahuaSyncQueue.id == item_id))
    item = result.scalar_one_or_none()
    if item is None:
        return Response(content="Queue item not found", status_code=404)
    if item.status == "success":
        return Response(content="Item already succeeded", status_code=400)

    success, error_msg = await _execute_push(item, db)
    new_status = "success" if success else "failed"
    await queue_svc.mark_item(db, item_id, new_status, error_msg)

    logger.info(
        "Manual push item=%d action=%s device=%d → %s%s",
        item_id,
        item.action,
        item.device_id,
        new_status,
        f" ({error_msg})" if error_msg else "",
    )

    is_htmx = request.headers.get("HX-Request") == "true"
    if is_htmx:
        # Reload the item to get updated processed_at
        await db.refresh(item)
        client_result = await db.execute(
            select(MindBodyClient).where(MindBodyClient.mindbody_id == item.mindbody_client_id)
        )
        mb_client = client_result.scalar_one_or_none()
        device_result = await db.execute(
            select(DahuaDevice).where(DahuaDevice.id == item.device_id)
        )
        device = device_result.scalar_one_or_none()

        row = _row_to_dict(item, mb_client, device)
        return request.app.state.templates.TemplateResponse(
            request,
            "sync_queue/_row.html",
            {"item": row},
        )

    return Response(status_code=303, headers={"Location": "/admin/sync-queue"})
