from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.admin.auth import get_current_user
from app.admin.auth import router as auth_router
from app.admin.csrf import validate_csrf_token
from app.admin.dashboard import router as dashboard_router
from app.admin.devices import router as devices_router
from app.admin.export_jobs import router as export_jobs_router
from app.admin.mindbody_users import router as mindbody_users_router
from app.admin.sync_queue import router as sync_queue_router

logger = logging.getLogger(__name__)

admin_router = APIRouter(prefix="/admin")

# Include auth routes (login/logout) without auth middleware
admin_router.include_router(auth_router)

# Protected routes
admin_router.include_router(dashboard_router)
admin_router.include_router(export_jobs_router)
admin_router.include_router(mindbody_users_router)
admin_router.include_router(devices_router)
admin_router.include_router(sync_queue_router)

# Routes exempt from CSRF validation (login submits credentials, not session-bound actions)
_CSRF_EXEMPT_PATHS = {"/admin/login"}


class AdminAuthMiddleware(BaseHTTPMiddleware):
    """Middleware that protects all /admin/* routes except /admin/login.

    Also enforces CSRF token validation on state-changing requests (POST, DELETE).
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/admin") and path not in ("/admin/login", "/admin/logout"):
            user = get_current_user(request)
            if not user:
                return RedirectResponse(url="/admin/login", status_code=303)
            request.state.user = user

            # CSRF validation on state-changing methods
            if request.method in ("POST", "DELETE") and path not in _CSRF_EXEMPT_PATHS:
                secret_key = request.app.state.settings.secret_key
                form = await request.form()
                csrf_token = form.get("csrf_token", "") or request.query_params.get(
                    "csrf_token", ""
                )
                if not validate_csrf_token(csrf_token, secret_key):
                    logger.warning(
                        "CSRF validation failed for %s %s (user=%s)", request.method, path, user
                    )
                    return HTMLResponse(
                        "CSRF token invalid or expired. Please go back and try again.",
                        status_code=403,
                    )
        else:
            request.state.user = None
        return await call_next(request)
