from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import asc, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_async_db
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership
from app.services import members as members_svc

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
    db: AsyncSession = Depends(get_async_db),
):
    page_size = 50

    q = select(MindBodyClient).options(selectinload(MindBodyClient.memberships))
    if search:
        q = q.where(
            MindBodyClient.first_name.ilike(f"%{search}%")
            | MindBodyClient.last_name.ilike(f"%{search}%")
            | MindBodyClient.email.ilike(f"%{search}%")
        )
    if filter_active == "active":
        q = q.where(MindBodyClient.active.is_(True))
    elif filter_active == "inactive":
        q = q.where(MindBodyClient.active.is_(False))
    if filter_status:
        q = q.where(MindBodyClient.status == filter_status)
    if filter_gender:
        q = q.where(MindBodyClient.gender == filter_gender)
    if filter_has_membership == "yes":
        active_mb_subq = (
            select(MindBodyMembership.mindbody_client_id)
            .where(MindBodyMembership.is_active.is_(True))
            .distinct()
            .scalar_subquery()
        )
        q = q.where(MindBodyClient.mindbody_id.in_(active_mb_subq))
    elif filter_has_membership == "no":
        active_mb_subq = (
            select(MindBodyMembership.mindbody_client_id)
            .where(MindBodyMembership.is_active.is_(True))
            .distinct()
            .scalar_subquery()
        )
        q = q.where(MindBodyClient.mindbody_id.not_in(active_mb_subq))

    sort_col = _SORTABLE.get(sort, MindBodyClient.last_name)
    q = q.order_by(asc(sort_col) if order == "asc" else desc(sort_col))

    total_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = total_result.scalar() or 0

    clients_result = await db.execute(q.offset(offset).limit(page_size))
    clients = list(clients_result.scalars().all())

    statuses_result = await db.execute(
        select(MindBodyClient.status).distinct().order_by(MindBodyClient.status)
    )
    statuses = [r[0] for r in statuses_result.all() if r[0]]

    genders_result = await db.execute(
        select(MindBodyClient.gender).distinct().order_by(MindBodyClient.gender)
    )
    genders = [r[0] for r in genders_result.all() if r[0]]

    last_fetched_at = await members_svc.get_last_fetched_at(db)

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


@router.post("/refresh")
async def refresh_mindbody_users(
    request: Request,
    full: bool = False,
    db: AsyncSession = Depends(get_async_db),
):
    """Refresh the local MindBody client cache."""
    from app.clients.mindbody import MindBodyClient as _MindBodyClient

    mb = _MindBodyClient(settings=request.app.state.settings)
    try:
        last_fetched = None if full else await members_svc.get_last_fetched_at(db)
        clients = await mb.get_all_clients(modified_since=last_fetched)
        await members_svc.upsert_batch(db, clients)
    except Exception:
        logger.exception("Failed to refresh MindBody clients")
    finally:
        await mb.close()

    return RedirectResponse(url="/admin/mindbody-users", status_code=303)
