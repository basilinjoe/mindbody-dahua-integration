from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from sqlalchemy import case, func

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership

router = APIRouter()


def _pct(count: int, total: int, *, default: int = 0) -> int:
    return round(count * 100 / total) if total else default


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
    active_members_pct = _pct(active_members, total_members)
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
    success_rate_pct = _pct(total_queue_24h - failed_24h, total_queue_24h, default=100)
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

    male_pct = _pct(male_count, total)
    female_pct = _pct(female_count, total)
    active_sub_pct = _pct(active_sub_count, total)

    return {
        "total": total,
        "male_count": male_count,
        "female_count": female_count,
        "male_pct": male_pct,
        "female_pct": female_pct,
        "active_sub_count": active_sub_count,
        "no_sub_count": no_sub_count,
        "active_sub_pct": active_sub_pct,
        "no_sub_pct": 100 - active_sub_pct,
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
    device_ids = [d.id for d in devices]
    counts = (
        db.query(
            DahuaSyncQueue.device_id,
            func.count(case((DahuaSyncQueue.status == "pending", 1))).label("pending"),
            func.count(
                case((
                    (DahuaSyncQueue.status == "failed")
                    & (DahuaSyncQueue.created_at >= cutoff),
                    1,
                ))
            ).label("failed_24h"),
        )
        .filter(DahuaSyncQueue.device_id.in_(device_ids))
        .group_by(DahuaSyncQueue.device_id)
        .all()
    )
    counts_by_device = {row.device_id: row for row in counts}
    return [
        {
            "device": device,
            "pending": (counts_by_device[device.id].pending if device.id in counts_by_device else 0),
            "failed_24h": (counts_by_device[device.id].failed_24h if device.id in counts_by_device else 0),
        }
        for device in devices
    ]


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
