from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.models.sync_log import SyncLog

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync")


@router.get("", response_class=HTMLResponse)
async def sync_logs(
    request: Request,
    sync_type: str = "",
    action: str = "",
    status: str = "",
    offset: int = 0,
):
    db = request.app.state.db_session_factory()
    page_size = 50
    try:
        q = db.query(SyncLog)
        if sync_type:
            q = q.filter(SyncLog.sync_type == sync_type)
        if action:
            q = q.filter(SyncLog.action == action)
        if status == "success":
            q = q.filter(SyncLog.success.is_(True))
        elif status == "failed":
            q = q.filter(SyncLog.success.is_(False))

        total = q.count()
        logs = q.order_by(SyncLog.created_at.desc()).offset(offset).limit(page_size).all()

        scheduler = request.app.state.scheduler
        return request.app.state.templates.TemplateResponse(
            "sync/logs.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "sync",
                "logs": logs,
                "total": total,
                "offset": offset,
                "page_size": page_size,
                "sync_type": sync_type,
                "action_filter": action,
                "status_filter": status,
                "sync_paused": scheduler.is_sync_paused,
            },
        )
    finally:
        db.close()


@router.post("/trigger")
async def trigger_full_sync(request: Request, background_tasks: BackgroundTasks):
    engine = request.app.state.sync_engine
    background_tasks.add_task(engine.full_sync)
    logger.info("Manual full sync triggered by admin")
    return RedirectResponse(url="/admin/sync", status_code=303)


@router.post("/pause")
async def pause_sync(request: Request):
    request.app.state.scheduler.pause_sync()
    logger.info("Sync paused by admin")
    return RedirectResponse(url="/admin/sync", status_code=303)


@router.post("/resume")
async def resume_sync(request: Request):
    request.app.state.scheduler.resume_sync()
    logger.info("Sync resumed by admin")
    return RedirectResponse(url="/admin/sync", status_code=303)
