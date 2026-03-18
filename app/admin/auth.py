from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.models.admin_user import AdminUser

logger = logging.getLogger(__name__)
router = APIRouter()


def _serializer(request: Request) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(request.app.state.settings.secret_key)


def get_current_user(request: Request) -> str | None:
    """Read the session cookie and return the username, or None."""
    token = request.cookies.get("session")
    if not token:
        return None
    settings = request.app.state.settings
    s = URLSafeTimedSerializer(settings.secret_key)
    try:
        data = s.loads(token, max_age=settings.session_expire_hours * 3600)
        return data.get("username")
    except (BadSignature, SignatureExpired):
        return None


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "login.html", {"request": request, "session_user": None, "error": None}
    )


@router.post("/login")
async def login_submit(request: Request):
    templates = request.app.state.templates
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")

    db = request.app.state.db_session_factory()
    try:
        user = db.query(AdminUser).filter_by(username=username, is_active=True).first()
        if user and user.verify_password(password):
            s = _serializer(request)
            token = s.dumps({"username": username})
            response = RedirectResponse(url="/admin/", status_code=303)
            response.set_cookie(
                "session",
                token,
                httponly=True,
                samesite="lax",
                secure=request.app.state.settings.secure_cookies,
                max_age=request.app.state.settings.session_expire_hours * 3600,
            )
            logger.info("Admin login: %s", username)
            return response
    finally:
        db.close()

    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "session_user": None, "error": "Invalid username or password"},
    )


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie("session")
    return response
