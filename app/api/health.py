from __future__ import annotations

from fastapi import APIRouter, Request
from sqlalchemy import text

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"status": "ok"}


def _check_database(db_session_factory) -> bool:
    db = None
    try:
        db = db_session_factory()
        db.execute(text("SELECT 1"))  # noqa: S608
        return True
    except Exception:
        return False
    finally:
        if db is not None:
            db.close()


async def _check_dahua_devices(sync_engine) -> bool:
    try:
        clients = sync_engine.get_dahua_clients()
        if not clients:
            return False
        return await clients[0].health_check()
    except Exception:
        return False


@router.get("/health/ready")
async def readiness(request: Request):
    """Readiness probe — checks DB and at least one Dahua device."""
    checks: dict = {"database": False, "dahua_devices": False}

    checks["database"] = _check_database(request.app.state.db_session_factory)
    checks["dahua_devices"] = await _check_dahua_devices(request.app.state.sync_engine)

    ready = all(checks.values())
    return {"status": "ready" if ready else "not_ready", "checks": checks}
