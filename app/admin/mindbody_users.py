from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import asc, desc

from app.models.mindbody_client import MindBodyClient

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mindbody-users")

_SORTABLE = {
    "mindbody_id": MindBodyClient.mindbody_id,
    "first_name": MindBodyClient.first_name,
    "last_name": MindBodyClient.last_name,
    "email": MindBodyClient.email,
    "mobile_phone": MindBodyClient.mobile_phone,
    "status": MindBodyClient.status,
    "active": MindBodyClient.active,
    "birth_date": MindBodyClient.birth_date,
    "gender": MindBodyClient.gender,
    "created_at_mb": MindBodyClient.created_at_mb,
    "last_modified_at_mb": MindBodyClient.last_modified_at_mb,
}


@router.get("", response_class=HTMLResponse)
async def mindbody_user_list(
    request: Request,
    search: str = "",
    sort: str = "last_name",
    order: str = "asc",
    offset: int = 0,
):
    db = request.app.state.db_session_factory()
    page_size = 50
    try:
        q = db.query(MindBodyClient)
        if search:
            q = q.filter(
                MindBodyClient.first_name.ilike(f"%{search}%")
                | MindBodyClient.last_name.ilike(f"%{search}%")
                | MindBodyClient.email.ilike(f"%{search}%")
            )

        sort_col = _SORTABLE.get(sort, MindBodyClient.last_name)
        q = q.order_by(asc(sort_col) if order == "asc" else desc(sort_col))

        total = q.count()
        clients = q.offset(offset).limit(page_size).all()

        # Last refresh timestamp
        latest = db.query(MindBodyClient.last_fetched_at).order_by(
            MindBodyClient.last_fetched_at.desc()
        ).first()
        last_fetched_at = latest[0] if latest else None

        return request.app.state.templates.TemplateResponse(
            "mindbody/list.html",
            {
                "request": request,
                "session_user": request.state.user,
                "active_page": "mindbody_users",
                "clients": clients,
                "total": total,
                "search": search,
                "sort": sort,
                "order": order,
                "offset": offset,
                "page_size": page_size,
                "last_fetched_at": last_fetched_at,
            },
        )
    finally:
        db.close()


@router.post("/refresh")
async def refresh_mindbody_users(request: Request):
    """Fetch all MindBody clients and upsert into the local DB."""
    db = request.app.state.db_session_factory()
    try:
        engine = request.app.state.sync_engine
        clients = await engine.mindbody.get_all_clients()
        now = datetime.now(timezone.utc)

        for c in clients:
            mid = str(c.get("Id", "")).strip()
            if not mid:
                continue
            existing = db.query(MindBodyClient).filter_by(mindbody_id=mid).first()
            if existing:
                existing.unique_id = c.get("UniqueId")
                existing.first_name = c.get("FirstName", "")
                existing.last_name = c.get("LastName", "")
                existing.email = c.get("Email")
                existing.mobile_phone = c.get("MobilePhone")
                existing.home_phone = c.get("HomePhone")
                existing.work_phone = c.get("WorkPhone")
                existing.status = c.get("Status")
                existing.active = bool(c.get("Active", False))
                existing.birth_date = c.get("BirthDate")
                existing.gender = c.get("Gender")
                existing.created_at_mb = c.get("CreationDate")
                existing.last_modified_at_mb = c.get("LastModifiedDateTime")
                existing.photo_url = c.get("PhotoUrl")
                existing.last_fetched_at = now
            else:
                db.add(MindBodyClient(
                    mindbody_id=mid,
                    unique_id=c.get("UniqueId"),
                    first_name=c.get("FirstName", ""),
                    last_name=c.get("LastName", ""),
                    email=c.get("Email"),
                    mobile_phone=c.get("MobilePhone"),
                    home_phone=c.get("HomePhone"),
                    work_phone=c.get("WorkPhone"),
                    status=c.get("Status"),
                    active=bool(c.get("Active", False)),
                    birth_date=c.get("BirthDate"),
                    gender=c.get("Gender"),
                    created_at_mb=c.get("CreationDate"),
                    last_modified_at_mb=c.get("LastModifiedDateTime"),
                    photo_url=c.get("PhotoUrl"),
                    last_fetched_at=now,
                ))

        db.commit()
        logger.info("Refreshed %d MindBody clients into DB", len(clients))
    except Exception:
        db.rollback()
        logger.exception("Failed to refresh MindBody clients")
    finally:
        db.close()

    return RedirectResponse(url="/admin/mindbody-users", status_code=303)
