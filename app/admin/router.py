from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.admin.auth import get_current_user, router as auth_router
from app.admin.dashboard import router as dashboard_router
from app.admin.devices import router as devices_router
from app.admin.export_jobs import router as export_jobs_router
from app.admin.members import router as members_router
from app.admin.mindbody_users import router as mindbody_users_router
from app.admin.sync_views import router as sync_router

admin_router = APIRouter(prefix="/admin")

# Include auth routes (login/logout) without auth middleware
admin_router.include_router(auth_router)

# Protected routes
admin_router.include_router(dashboard_router)
admin_router.include_router(members_router)
admin_router.include_router(export_jobs_router)
admin_router.include_router(mindbody_users_router)
admin_router.include_router(devices_router)
admin_router.include_router(sync_router)


class AdminAuthMiddleware(BaseHTTPMiddleware):
    """Middleware that protects all /admin/* routes except /admin/login."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith("/admin") and path not in ("/admin/login", "/admin/logout"):
            user = get_current_user(request)
            if not user:
                return RedirectResponse(url="/admin/login", status_code=303)
            request.state.user = user
        else:
            request.state.user = None
        return await call_next(request)
