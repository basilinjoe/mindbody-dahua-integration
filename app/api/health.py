from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request):
    """Readiness probe — checks DB and at least one Dahua device."""
    checks: dict = {"database": False, "dahua_devices": False}
    try:
        db_factory = request.app.state.db_session_factory
        db = db_factory()
        db.execute("SELECT 1")  # noqa: S608
        db.close()
        checks["database"] = True
    except Exception:
        pass

    try:
        engine = request.app.state.sync_engine
        clients = engine.get_dahua_clients()
        if clients:
            ok = await clients[0].health_check()
            checks["dahua_devices"] = ok
    except Exception:
        pass

    ready = all(checks.values())
    return {"status": "ready" if ready else "not_ready", "checks": checks}
