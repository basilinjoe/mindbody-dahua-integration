from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.sync_log import SyncLog

router = APIRouter()


def _get_stats(db) -> dict:
    total = db.query(SyncedMember).count()
    active = db.query(SyncedMember).filter_by(is_active_in_dahua=True).count()
    missing = db.query(SyncedMember).filter_by(has_face_photo=False, is_active_in_dahua=True).count()
    devices_total = db.query(DahuaDevice).filter_by(is_enabled=True).count()
    devices_online = db.query(DahuaDevice).filter_by(is_enabled=True, status="online").count()
    return {
        "total_members": total,
        "active_members": active,
        "missing_photos": missing,
        "devices_total": devices_total,
        "devices_online": devices_online,
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db = request.app.state.db_session_factory()
    try:
        stats = _get_stats(db)
        recent_logs = db.query(SyncLog).order_by(SyncLog.created_at.desc()).limit(10).all()
        return request.app.state.templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "session_user": request.state.user,
                "active_page": "dashboard",
                "stats": stats,
                "recent_logs": recent_logs,
            },
        )
    finally:
        db.close()


@router.get("/partials/stats", response_class=HTMLResponse)
async def stats_partial(request: Request):
    """HTMX partial: refreshes the stats cards."""
    db = request.app.state.db_session_factory()
    try:
        stats = _get_stats(db)
        return request.app.state.templates.TemplateResponse(
            request,
            "partials/stats.html",
            {"stats": stats},
        )
    finally:
        db.close()
