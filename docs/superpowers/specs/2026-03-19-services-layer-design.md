# Services Layer Refactor — Design Spec

**Date:** 2026-03-19
**Status:** Approved

---

## Problem

Database logic is scattered across three locations:

- `app/sync/tasks.py` — Prefect async tasks with inline query logic (15+ tasks, 515 lines)
- `app/sync/mindbody_client_service.py` — One-off sync service for MindBody clients
- `app/admin/*.py` — Admin routes with inline `db.query(...)` calls and manual session lifecycle

This causes:
- **Poor testability** — DB logic embedded in Prefect tasks and route handlers requires Prefect context or a live HTTP app to test
- **Duplication** — Same query patterns repeated across admin routes and sync tasks
- **Poor discoverability** — No single place to find "how is X queried or written?"

---

## Goal

Consolidate all database read/write logic into `app/services/`, one file per domain. All code that touches the DB calls into `app/services/`. Sessions flow in from the caller — services never create their own sessions.

---

## Architecture

### New folder

```
app/services/
├── __init__.py
├── members.py        — upsert_batch, load_active, get_last_fetched_at
├── memberships.py    — upsert_batch, load_windows
├── devices.py        — list_all, list_by_gate_type, get_by_id, update_status
├── queue.py          — write_batch, load_pending, mark_item
├── export_jobs.py    — create, get, list_all, update
├── dashboard.py      — get_stats, get_recent_queue, get_mb_breakdown, get_device_rows
└── admin_users.py    — get_by_username
```

### Function signature convention

Every service function takes `db: AsyncSession` as its first argument and is `async`. All functions migrated from `mindbody_client_service.py` (which were sync) must be rewritten as async using `await db.execute(select(...))` style — the SQLAlchemy 2.0 async API.

```python
# app/services/queue.py
async def load_pending(db: AsyncSession, run_id: str) -> list[DahuaSyncQueue]: ...
async def mark_item(db: AsyncSession, item_id: int, status: str, error: str | None = None) -> None: ...
async def write_batch(db: AsyncSession, run_id: str, items: list[dict]) -> int: ...
```

No classes. No session creation inside service functions. Pure async functions.

---

## Session Layer Changes

### Remove sync engine entirely

`app/models/database.py` currently maintains two parallel engines (sync + async). The sync engine, `init_db()`, `SessionLocal`, and `get_db()` are removed. Only the async engine remains.

### Session acquisition

`get_async_db` is an async generator (used with `Depends` in FastAPI). Prefect tasks cannot use `async with get_async_db()` directly since it is a generator, not a context manager. Prefect tasks must use the factory directly:

```python
# FastAPI — via Depends
async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

# Prefect tasks — direct factory usage (unchanged pattern)
@task
async def load_pending_queue_items(run_id: str) -> list[DahuaSyncQueue]:
    async with AsyncSessionLocal() as db:
        return await services.queue.load_pending(db, run_id)
```

`AsyncSessionLocal` is imported directly from `app.models.database` in `tasks.py`. The `_get_async_session_factory()` helper is replaced by the direct `AsyncSessionLocal` reference.

### FastAPI routes — switch to async + Depends

```python
# Before
async def list_devices(request: Request):
    db = request.app.state.db_session_factory()
    try:
        items = db.query(DahuaDevice).all()
    finally:
        db.close()

# After
async def list_devices(db: AsyncSession = Depends(get_async_db)):
    items = await services.devices.list_all(db)
```

### app/main.py — startup helpers converted to async

`main.py` contains three startup helpers that currently use the sync session factory:
- `_seed_admin(db_session_factory)` — seeds default admin user
- `_seed_devices(db_session_factory)` — seeds devices from env
- `_recover_stuck_export_jobs(db_session_factory)` — resets stuck jobs

All three must be rewritten to take `AsyncSession` and use the async ORM API (`await db.execute(select(...))`, `db.add(...)`, `await db.commit()`). They are called inside the async lifespan context manager, so they receive a session opened from `AsyncSessionLocal`.

The `init_db()` call and `db_session_factory` on `app.state` are removed. Only `init_async_db()` remains.

---

## File Migration Map

| Source | Moves to | Notes |
|---|---|---|
| `sync/mindbody_client_service.py` | `services/members.py` | Rewrite as async |
| `sync/tasks.py` — member/membership queries | `services/members.py`, `services/memberships.py` | Already async |
| `sync/tasks.py` — device queries | `services/devices.py` | Already async |
| `sync/tasks.py` — queue queries | `services/queue.py` | Already async |
| `sync/flows/health.py` — inline device update | `services/devices.py` | Already async |
| `admin/devices.py` — inline queries | `services/devices.py` | Rewrite as async |
| `admin/mindbody_users.py` — inline queries | `services/members.py` | Rewrite as async |
| `admin/sync_queue.py` — inline queries | `services/queue.py` | Rewrite as async |
| `admin/export_jobs.py` — DB-only queries | `services/export_jobs.py` | Rewrite as async (see note) |
| `admin/auth.py` — inline query | `services/admin_users.py` | Rewrite as async |
| `admin/dashboard.py` — inline stats queries | `services/dashboard.py` | Rewrite as async; kept together since queries span multiple domains |
| `main.py` — `_seed_admin`, `_seed_devices`, `_recover_stuck_export_jobs` | Stay in `main.py` | Rewrite as async |

### Note on `admin/export_jobs.py`

`export_jobs.py` has two separate concerns:

1. **Route handlers** use `request.app.state.sync_engine` to call `sync_engine.mindbody.get_all_clients()` and `sync_engine._dahua_clients`. This is a higher-level orchestration object, not a raw DB session — these references remain unchanged and are out of scope for this refactor.

2. **`_run_export_job`** is a background task function that also acquires a session via `request.app.state.db_session_factory`. Since `db_session_factory` is removed from `app.state` as part of this refactor, `_run_export_job` must be updated to acquire its session via `AsyncSessionLocal()` directly (the same pattern used by Prefect tasks). Its DB queries (job status updates) move to `services/export_jobs.py`.

---

## Testing

Service functions take a plain `AsyncSession`, so they are testable with a real test DB session and no Prefect context or HTTP app required.

**Shared fixture (`tests/conftest.py`):**
```python
@pytest.fixture
async def db(async_engine):
    async with AsyncSession(async_engine) as session:
        yield session
        await session.rollback()
```

**Example service test:**
```python
# tests/services/test_queue.py
async def test_load_pending(db: AsyncSession):
    db.add(DahuaSyncQueue(run_id="abc", status="pending", action="enroll", ...))
    await db.commit()

    items = await services.queue.load_pending(db, run_id="abc")

    assert len(items) == 1
    assert items[0].action == "enroll"
```

Prefect task tests remain integration-level (verify the task calls the right service). Admin route tests only need to verify response codes and template rendering.

---

## What Does NOT Change

- `app/models/` — ORM model definitions stay as-is
- `app/clients/` — HTTP clients (MindBody API, Dahua) are not DB services
- `app/sync/tasks.py` — File stays, tasks become thin wrappers (not deleted)
- `app/sync/flows/` — Flow orchestration logic unchanged
- Prefect deployment config in `worker.py` — unchanged
- `sync_engine` references in `admin/export_jobs.py` — out of scope

---

## Files Deleted

- `app/sync/mindbody_client_service.py` — fully absorbed into `services/members.py`

---

## Build Sequence

Steps that remove symbols must update all importers in the same step to avoid `ImportError` at module load time.

1. Create `app/services/` with all six files (extract and rewrite logic from current locations)
2. Rewrite `main.py` startup helpers (`_seed_admin`, `_seed_devices`, `_recover_stuck_export_jobs`) as async; remove `init_db()` and `app.state.db_session_factory`
3. Update `app/models/database.py` and `app/api/deps.py` in one step — remove sync engine, `get_db`, expose `AsyncSessionLocal` directly; update `deps.py` to remove sync dep
4. Convert all `app/admin/*.py` routes to async + `Depends(get_async_db)`
5. Hollow out `app/sync/tasks.py` tasks — replace inline queries with `AsyncSessionLocal` + service calls
6. Update `app/sync/flows/health.py` inline query to call `services.devices.update_status`
7. Delete `app/sync/mindbody_client_service.py`
8. Add `tests/services/` with service-level tests
