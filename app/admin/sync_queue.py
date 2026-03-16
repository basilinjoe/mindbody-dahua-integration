from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.models.dahua_sync_queue import DahuaSyncQueue

router = APIRouter(prefix="/sync-queue")


@router.get("", response_class=HTMLResponse)
async def sync_queue_list(
    request: Request,
    run_id: str = "",
    action: str = "",
    status: str = "",
    offset: int = 0,
):
    db = request.app.state.db_session_factory()
    page_size = 50
    try:
        q = db.query(DahuaSyncQueue)
        if run_id:
            q = q.filter(DahuaSyncQueue.run_id == run_id)
        if action:
            q = q.filter(DahuaSyncQueue.action == action)
        if status:
            q = q.filter(DahuaSyncQueue.status == status)

        total = q.count()
        items = q.order_by(DahuaSyncQueue.created_at.desc()).offset(offset).limit(page_size).all()

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
            },
        )
    finally:
        db.close()
