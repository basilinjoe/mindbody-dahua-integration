from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import asc, desc

from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership
from app.sync.mindbody_client_service import (
    get_last_fetched_at,
    refresh_mindbody_clients,
    upsert_mindbody_clients,
)

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
    filter_active: str = "",
    filter_status: str = "",
    filter_gender: str = "",
    filter_has_membership: str = "",
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
        if filter_active == "active":
            q = q.filter(MindBodyClient.active.is_(True))
        elif filter_active == "inactive":
            q = q.filter(MindBodyClient.active.is_(False))
        if filter_status:
            q = q.filter(MindBodyClient.status == filter_status)
        if filter_gender:
            q = q.filter(MindBodyClient.gender == filter_gender)
        if filter_has_membership == "yes":
            active_mb_ids = (
                db.query(MindBodyMembership.mindbody_client_id)
                .filter(MindBodyMembership.is_active.is_(True))
                .distinct()
            )
            q = q.filter(MindBodyClient.mindbody_id.in_(active_mb_ids))
        elif filter_has_membership == "no":
            active_mb_ids = (
                db.query(MindBodyMembership.mindbody_client_id)
                .filter(MindBodyMembership.is_active.is_(True))
                .distinct()
            )
            q = q.filter(~MindBodyClient.mindbody_id.in_(active_mb_ids))

        sort_col = _SORTABLE.get(sort, MindBodyClient.last_name)
        q = q.order_by(asc(sort_col) if order == "asc" else desc(sort_col))

        total = q.count()
        clients = q.offset(offset).limit(page_size).all()

        # Distinct values for filter dropdowns
        statuses = [
            r[0] for r in db.query(MindBodyClient.status).distinct().order_by(MindBodyClient.status).all()
            if r[0]
        ]
        genders = [
            r[0] for r in db.query(MindBodyClient.gender).distinct().order_by(MindBodyClient.gender).all()
            if r[0]
        ]

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
                "filter_active": filter_active,
                "filter_status": filter_status,
                "filter_gender": filter_gender,
                "filter_has_membership": filter_has_membership,
                "statuses": statuses,
                "genders": genders,
            },
        )
    finally:
        db.close()


@router.post("/refresh")
async def refresh_mindbody_users(request: Request, full: bool = False):
    """Refresh the local MindBody client cache.

    By default performs an incremental fetch (only clients modified since the
    last refresh).  Pass ?full=true to force a complete re-fetch and replace.
    """
    db = request.app.state.db_session_factory()
    try:
        engine = request.app.state.sync_engine
        last_fetched = None if full else get_last_fetched_at(db)

        if last_fetched:
            clients = await engine.mindbody.get_all_clients(modified_since=last_fetched)
            upsert_mindbody_clients(db, clients)
        else:
            clients = await engine.mindbody.get_all_clients()
            refresh_mindbody_clients(db, clients)

        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Failed to refresh MindBody clients")
    finally:
        db.close()

    return RedirectResponse(url="/admin/mindbody-users", status_code=303)
