from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.services import dashboard as dashboard_svc

router = APIRouter()


def _pct(count: int, total: int, *, default: int = 0) -> int:
    return round(count * 100 / total) if total else default


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
):
    stats = await dashboard_svc.get_stats(db)
    # Add derived percentages expected by template
    stats["active_members_pct"] = _pct(stats["active_members"], stats["total_members"])
    stats["devices_total"] = stats["total_devices"]
    stats["devices_online"] = stats["online_devices"]
    stats["failed_24h"] = stats["failed_queue"]
    stats["success_rate_pct"] = 100

    recent_queue = await dashboard_svc.get_recent_queue(db)
    mb_breakdown = await dashboard_svc.get_mb_breakdown(db)
    device_rows = await dashboard_svc.get_device_rows(db)
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


@router.get("/partials/stats", response_class=HTMLResponse)
async def stats_partial(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
):
    """HTMX partial: refreshes the stats cards."""
    stats = await dashboard_svc.get_stats(db)
    stats["active_members_pct"] = _pct(stats["active_members"], stats["total_members"])
    stats["devices_total"] = stats["total_devices"]
    stats["devices_online"] = stats["online_devices"]
    stats["failed_24h"] = stats["failed_queue"]
    stats["success_rate_pct"] = 100
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/stats.html",
        {"stats": stats},
    )
