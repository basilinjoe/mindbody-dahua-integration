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


def _get_recent_queue(db) -> list[tuple]:
    """Return last 10 queue items with resolved device name.

    Each element is a (DahuaSyncQueue, device_name: str | None) tuple.
    """
    rows = (
        db.query(DahuaSyncQueue, DahuaDevice.name)
        .outerjoin(DahuaDevice, DahuaDevice.id == DahuaSyncQueue.device_id)
        .order_by(DahuaSyncQueue.created_at.desc())
        .limit(10)
        .all()
    )
    return rows


def _get_mb_breakdown(db) -> dict:
    """Gender split and subscription breakdown for MindBody clients."""
    total = db.query(MindBodyClient).count()

    male_count = db.query(MindBodyClient).filter(MindBodyClient.gender == "Male").count()
    female_count = db.query(MindBodyClient).filter(MindBodyClient.gender == "Female").count()

    active_sub_subq = (
        db.query(MindBodyMembership.id)
        .filter(MindBodyMembership.mindbody_client_id == MindBodyClient.mindbody_id)
        .filter(MindBodyMembership.is_active.is_(True))
        .correlate(MindBodyClient)
        .exists()
    )
    active_sub_count = db.query(MindBodyClient).filter(active_sub_subq).count()
    no_sub_count = total - active_sub_count

    male_pct = round(male_count * 100 / total) if total else 0
    female_pct = round(female_count * 100 / total) if total else 0
    active_sub_pct = round(active_sub_count * 100 / total) if total else 0

    return {
        "total": total,
        "male_count": male_count,
        "female_count": female_count,
        "male_pct": male_pct,
        "female_pct": female_pct,
        "active_sub_count": active_sub_count,
        "no_sub_count": no_sub_count,
        "active_sub_pct": active_sub_pct,
    }


def _get_device_rows(db) -> list[dict]:
    """Return enabled devices with their pending and 24h-failed queue counts."""
    devices = (
        db.query(DahuaDevice)
        .filter_by(is_enabled=True)
        .order_by(DahuaDevice.name)
        .all()
    )
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=24)
    rows = []
    for device in devices:
        pending = (
            db.query(DahuaSyncQueue)
            .filter_by(device_id=device.id, status="pending")
            .count()
        )
        failed_24h = (
            db.query(DahuaSyncQueue)
            .filter(
                DahuaSyncQueue.device_id == device.id,
                DahuaSyncQueue.status == "failed",
                DahuaSyncQueue.created_at >= cutoff,
            )
            .count()
        )
        rows.append({"device": device, "pending": pending, "failed_24h": failed_24h})
    return rows


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db = request.app.state.db_session_factory()
    try:
        stats = _get_stats(db)
        recent_queue = _get_recent_queue(db)
        mb_breakdown = _get_mb_breakdown(db)
        device_rows = _get_device_rows(db)
        return request.app.state.templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "session_user": request.state.user,
                "active_page": "dashboard",
                "stats": stats,
                "recent_queue": recent_queue,
                "mb_breakdown": mb_breakdown,
                "device_rows": device_rows,
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
