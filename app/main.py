from __future__ import annotations

import json
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, sessionmaker
from starlette.templating import Jinja2Templates

import app.models.database as _db
from app.admin.csrf import generate_csrf_token
from app.admin.router import AdminAuthMiddleware, admin_router
from app.api.router import api_router
from app.clients.mindbody import MindBodyClient
from app.config import Settings
from app.models.admin_user import AdminUser
from app.models.database import Base, init_async_db
from app.models.device import DahuaDevice
from app.models.export_job import ExportJob, ExportStatus  # noqa: F401 — registers table

BASE_DIR = Path(__file__).resolve().parent


def _build_lifespan(
    *,
    settings: Settings | None = None,
    db_session_factory_override: sessionmaker[Session] | None = None,
    mindbody_client_factory: Callable[[Settings], MindBodyClient] | None = None,
):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app_settings = settings or Settings()

        # Logging
        logging.basicConfig(
            level=getattr(logging, app_settings.log_level.upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        logger = logging.getLogger("app")
        logger.info("Starting MindBody-Dahua Gate Sync")

        # Async database — all routes and Prefect tasks
        async_session_factory = init_async_db(app_settings.database_url)

        # Create tables (idempotent; skipped if migrations handle DDL)
        async with _db.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Seed and recover using a single async session
        async with async_session_factory() as db:
            await _seed_admin(db, app_settings)
            await _seed_devices(db, app_settings)
            await _recover_stuck_export_jobs(db)

        # Templates — inject csrf_token() global for all forms
        templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
        templates.env.globals["csrf_token"] = lambda: generate_csrf_token(app_settings.secret_key)

        # Store on app.state for route handlers
        app.state.settings = app_settings
        app.state.templates = templates

        # Legacy: expose sync factory for health check route (if injected by tests)
        if db_session_factory_override is not None:
            app.state.db_session_factory = db_session_factory_override

        yield

        logger.info("Shutdown complete")

    return lifespan


async def _seed_admin(db: AsyncSession, settings: Settings) -> None:
    result = await db.execute(select(AdminUser).limit(1))
    if result.scalar_one_or_none() is None:
        admin = AdminUser(username=settings.admin_username)
        admin.set_password(settings.admin_password)
        db.add(admin)
        await db.commit()
        logging.getLogger("app").info("Seeded admin user: %s", settings.admin_username)


async def _seed_devices(db: AsyncSession, settings: Settings) -> None:
    logger = logging.getLogger("app")
    devices_to_seed: list[dict] = []

    # 1) Parse DAHUA_DEVICES JSON array (preferred for multiple devices)
    if settings.dahua_devices:
        try:
            parsed = json.loads(settings.dahua_devices)
            if isinstance(parsed, list):
                devices_to_seed.extend(parsed)
            else:
                logger.warning("DAHUA_DEVICES must be a JSON array, ignoring")
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in DAHUA_DEVICES: %s", e)

    # 2) Fall back to single DAHUA_DEFAULT_* vars (backward-compatible)
    if not devices_to_seed and settings.dahua_default_host:
        devices_to_seed.append(
            {
                "name": "Default Device",
                "host": settings.dahua_default_host,
                "port": settings.dahua_default_port,
                "username": settings.dahua_default_username,
                "password": settings.dahua_default_password,
                "door_ids": settings.dahua_default_door_ids,
            }
        )

    if not devices_to_seed:
        return

    for entry in devices_to_seed:
        host = entry.get("host", "").strip()
        if not host:
            continue
        existing = await db.execute(select(DahuaDevice).where(DahuaDevice.host == host))
        if existing.scalar_one_or_none() is None:
            device = DahuaDevice(
                name=entry.get("name", host),
                host=host,
                port=int(entry.get("port", 80)),
                username=entry.get("username", "admin"),
                password=entry.get("password", ""),
                door_ids=entry.get("door_ids", "0"),
                is_enabled=bool(entry.get("is_enabled", True)),
                gate_type=entry.get("gate_type", "all"),
                enable_integration=bool(entry.get("enable_integration", True)),
            )
            db.add(device)
            logger.info("Seeded Dahua device: %s (%s)", device.name, host)
    await db.commit()


async def _recover_stuck_export_jobs(db: AsyncSession) -> None:
    result = await db.execute(
        select(ExportJob).where(ExportJob.status.in_([ExportStatus.pending, ExportStatus.running]))
    )
    stuck = list(result.scalars().all())
    for job in stuck:
        job.status = ExportStatus.failed
        job.error_msg = "Server restarted during export"
    if stuck:
        await db.commit()
        logging.getLogger("app").info("Reset %d stuck export job(s) to failed", len(stuck))


def create_app(
    *,
    settings: Settings | None = None,
    db_session_factory_override: sessionmaker[Session] | None = None,
    mindbody_client_factory: Callable[[Settings], MindBodyClient] | None = None,
) -> FastAPI:
    app = FastAPI(
        title="MindBody-Dahua Gate Sync",
        version="1.0.0",
        lifespan=_build_lifespan(
            settings=settings,
            db_session_factory_override=db_session_factory_override,
            mindbody_client_factory=mindbody_client_factory,
        ),
    )

    # Middleware
    app.add_middleware(AdminAuthMiddleware)

    # Static files
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    # Routers
    app.include_router(api_router)
    app.include_router(admin_router)
    return app


app = create_app()
