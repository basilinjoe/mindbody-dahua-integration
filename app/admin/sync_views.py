from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from prefect.deployments import run_deployment

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

        # Scheduling is now managed by Prefect — pause/resume via Prefect UI
        return request.app.state.templates.TemplateResponse(
            request,
            "sync/logs.html",
            {
                "session_user": request.state.user,
                "active_page": "sync",
                "logs": logs,
                "total": total,
                "offset": offset,
                "page_size": page_size,
                "sync_type": sync_type,
                "action_filter": action,
                "status_filter": status,
                "sync_paused": False,
            },
        )
    finally:
        db.close()


@router.post("/trigger")
async def trigger_full_sync(request: Request, background_tasks: BackgroundTasks):
    try:
        await run_deployment("sync-integration/full", timeout=0)  # non-blocking
        logger.info("Manual full sync triggered via Prefect deployment")
    except Exception:
        logger.exception("Failed to trigger full sync via Prefect")
    return RedirectResponse(url="/admin/sync", status_code=303)



@router.post("/pause")
async def pause_sync(request: Request):
    # Scheduling is now managed by Prefect — use Prefect UI to pause/resume schedules
    logger.info("Sync pause requested (scheduling managed by Prefect)")
    return RedirectResponse(url="/admin/sync", status_code=303)


@router.post("/resume")
async def resume_sync(request: Request):
    # Scheduling is now managed by Prefect — use Prefect UI to pause/resume schedules
    logger.info("Sync resume requested (scheduling managed by Prefect)")
    return RedirectResponse(url="/admin/sync", status_code=303)
