from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from app.admin.router import AdminAuthMiddleware, admin_router
from app.api.router import api_router
from app.clients.mindbody import MindBodyClient
from app.config import Settings
from app.models.admin_user import AdminUser
from app.models.database import init_db
from app.models.device import DahuaDevice
from app.sync.engine import SyncEngine
from app.sync.scheduler import SyncScheduler

BASE_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()

    # Logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("app")
    logger.info("Starting MindBody-Dahua Gate Sync")

    # Database
    db_session_factory = init_db(settings.database_url)

    # Seed admin user on first boot
    _seed_admin(db_session_factory, settings)

    # Seed Dahua devices from env config
    _seed_devices(db_session_factory, settings)

    # MindBody client
    mb_client = MindBodyClient(settings)

    # Sync engine
    sync_engine = SyncEngine(mb_client, db_session_factory, settings)

    # Templates
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

    # Store on app.state for route handlers
    app.state.settings = settings
    app.state.db_session_factory = db_session_factory
    app.state.sync_engine = sync_engine
    app.state.templates = templates

    # Scheduler
    scheduler = SyncScheduler(
        sync_engine,
        sync_interval_min=settings.sync_interval_minutes,
        health_interval_min=settings.device_health_interval_minutes,
    )
    scheduler.start()

    yield

    # Cleanup
    scheduler.stop()
    await mb_client.close()
    logger.info("Shutdown complete")


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
                )
                db.add(device)
                logger.info("Seeded Dahua device: %s (%s)", device.name, host)
        db.commit()
    finally:
        db.close()


app = FastAPI(
    title="MindBody-Dahua Gate Sync",
    version="1.0.0",
    lifespan=lifespan,
)

# Middleware
app.add_middleware(AdminAuthMiddleware)

# Static files
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Routers
app.include_router(api_router)
app.include_router(admin_router)
