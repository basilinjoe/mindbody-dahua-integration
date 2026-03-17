from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership

router = APIRouter()


def _get_stats(db) -> dict:
    total_members = db.query(MindBodyClient).count()
    active_membership_subq = (
        db.query(MindBodyMembership.id)
        .filter(MindBodyMembership.mindbody_client_id == MindBodyClient.mindbody_id)
        .filter(MindBodyMembership.is_active.is_(True))
        .correlate(MindBodyClient)
        .exists()
    )
    active_members = (
        db.query(MindBodyClient)
        .filter(MindBodyClient.active.is_(True))
        .filter(active_membership_subq)
        .count()
    )
    active_members_pct = round(active_members * 100 / total_members) if total_members else 0
    pending_queue = db.query(DahuaSyncQueue).filter_by(status="pending").count()
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=24)
    failed_24h = (
        db.query(DahuaSyncQueue)
        .filter(DahuaSyncQueue.status == "failed", DahuaSyncQueue.created_at >= cutoff)
        .count()
    )
    total_queue_24h = (
        db.query(DahuaSyncQueue)
        .filter(DahuaSyncQueue.created_at >= cutoff, DahuaSyncQueue.status != "pending")
        .count()
    )
    success_rate_pct = (
        round((total_queue_24h - failed_24h) * 100 / total_queue_24h)
        if total_queue_24h else 100
    )
    devices_total = db.query(DahuaDevice).filter_by(is_enabled=True).count()
    devices_online = db.query(DahuaDevice).filter_by(is_enabled=True, status="online").count()
    return {
        "total_members": total_members,
        "active_members": active_members,
        "active_members_pct": active_members_pct,
        "pending_queue": pending_queue,
        "failed_24h": failed_24h,
        "success_rate_pct": success_rate_pct,
        "devices_total": devices_total,
        "devices_online": devices_online,
    }


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db = request.app.state.db_session_factory()
    try:
        stats = _get_stats(db)
        recent_queue = (
            db.query(DahuaSyncQueue).order_by(DahuaSyncQueue.created_at.desc()).limit(10).all()
        )
        return request.app.state.templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "session_user": request.state.user,
                "active_page": "dashboard",
                "stats": stats,
                "recent_queue": recent_queue,
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
