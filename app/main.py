from __future__ import annotations

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

    # Seed default Dahua device if configured
    _seed_default_device(db_session_factory, settings)

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


def _seed_default_device(db_session_factory, settings: Settings) -> None:
    if not settings.dahua_default_host:
        return
    db = db_session_factory()
    try:
        exists = db.query(DahuaDevice).filter_by(host=settings.dahua_default_host).first()
        if not exists:
            device = DahuaDevice(
                name="Default Device",
                host=settings.dahua_default_host,
                port=settings.dahua_default_port,
                username=settings.dahua_default_username,
                password=settings.dahua_default_password,
                door_ids=settings.dahua_default_door_ids,
                is_enabled=True,
            )
            db.add(device)
            db.commit()
            logging.getLogger("app").info("Seeded default Dahua device: %s", settings.dahua_default_host)
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
