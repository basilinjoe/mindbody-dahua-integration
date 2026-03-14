from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Callable

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session, sessionmaker
from starlette.templating import Jinja2Templates

from app.admin.router import AdminAuthMiddleware, admin_router
from app.api.router import api_router
from app.clients.mindbody import MindBodyClient
from app.config import Settings
from app.models.admin_user import AdminUser
from app.models.database import init_async_db, init_db
from app.models.device import DahuaDevice
from app.models.export_job import ExportJob, ExportStatus  # noqa: F401 — registers table
from app.sync.engine import SyncEngine

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

        # Database (sync — FastAPI routes)
        db_session_factory = db_session_factory_override or init_db(app_settings.database_url)

        # Async database — Prefect tasks
        init_async_db(app_settings.database_url)

        # Seed admin user on first boot
        _seed_admin(db_session_factory, app_settings)

        # Seed Dahua devices from env config
        _seed_devices(db_session_factory, app_settings)

        # Reset any export jobs that were stuck in-flight when the server last stopped
        _recover_stuck_export_jobs(db_session_factory)

        # MindBody client — still needed by SyncEngine (export/member routes use it)
        mb_factory = mindbody_client_factory or MindBodyClient
        mb_client = mb_factory(app_settings)

        # Sync engine (still needed by export/member routes that use Dahua clients)
        sync_engine = SyncEngine(mb_client, db_session_factory, app_settings)

        # Templates
        templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

        # Store on app.state for route handlers
        app.state.settings = app_settings
        app.state.db_session_factory = db_session_factory
        app.state.sync_engine = sync_engine
        app.state.templates = templates

        yield

        # Cleanup
        await mb_client.close()
        logger.info("Shutdown complete")

    return lifespan


def _seed_admin(db_session_factory, settings: Settings) -> None:
    db = db_session_factory()
    try:
        if db.query(AdminUser).count() == 0:
            admin = AdminUser(username=settings.admin_username)
            admin.set_password(settings.admin_password)
            db.add(admin)
            db.commit()
            logging.getLogger("app").info("Seeded admin user: %s", settings.admin_username)
    finally:
        db.close()


def _seed_devices(db_session_factory, settings: Settings) -> None:
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
        devices_to_seed.append({
            "name": "Default Device",
            "host": settings.dahua_default_host,
            "port": settings.dahua_default_port,
            "username": settings.dahua_default_username,
            "password": settings.dahua_default_password,
            "door_ids": settings.dahua_default_door_ids,
        })

    if not devices_to_seed:
        return

    db = db_session_factory()
    try:
        for entry in devices_to_seed:
            host = entry.get("host", "").strip()
            if not host:
                continue
            exists = db.query(DahuaDevice).filter_by(host=host).first()
            if not exists:
                device = DahuaDevice(
                    name=entry.get("name", host),
                    host=host,
                    port=int(entry.get("port", 80)),
                    username=entry.get("username", "admin"),
                    password=entry.get("password", ""),
                    door_ids=entry.get("door_ids", "0"),
                    is_enabled=bool(entry.get("is_enabled", True)),
                    gate_type=entry.get("gate_type", "all"),
                    enable_integration=bool(entry.get("enable_integration", False)),
                )
                db.add(device)
                logger.info("Seeded Dahua device: %s (%s)", device.name, host)
        db.commit()
    finally:
        db.close()


def _recover_stuck_export_jobs(db_session_factory) -> None:
    db = db_session_factory()
    try:
        stuck = (
            db.query(ExportJob)
            .filter(ExportJob.status.in_([ExportStatus.pending, ExportStatus.running]))
            .all()
        )
        for job in stuck:
            job.status = ExportStatus.failed
            job.error_msg = "Server restarted during export"
        if stuck:
            db.commit()
            logging.getLogger("app").info("Reset %d stuck export job(s) to failed", len(stuck))
    finally:
        db.close()


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
