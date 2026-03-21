# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run app locally
uvicorn app.main:app --reload

# Run all tests
pytest

# Run a single test file
pytest tests/unit/clients/test_dahua.py

# Run a single test by name
pytest -k "test_enroll_success"

# Run tests with coverage (CI threshold: 85%)
pytest --cov=app --cov-report=term-missing --cov-fail-under=85

# Lint and format
ruff check .
ruff format .

# Fix lint issues automatically
ruff check --fix .

# Docker (full stack: postgres, prefect, fastapi, worker)
docker-compose up --build

# Prefect worker (separate process, needed for sync flows)
python -m app.sync.worker
```

## Architecture

**MindBody ↔ Dahua Gate Sync**: Syncs fitness membership status from MindBody's API to Dahua ASI7214S face recognition speed gates. Active members get face-enrolled with gate access; inactive members are frozen (CardStatus=4, preserving face data for reactivation).

### Three-Phase Sync (Prefect Flows)

The core sync logic lives in `app/sync/flows/integration.py` and follows fetch → plan → push:

1. **Fetch**: `app/clients/mindbody.py` pulls clients + memberships via MindBody API v6 (token auth, auto-refresh). Data is upserted into local DB via `app/services/members.py` and `app/services/memberships.py`.
2. **Plan**: Compares local DB state against each Dahua device → generates `enroll`, `deactivate`, `reactivate`, or `update_window` actions written to `dahua_sync_queue`.
3. **Push** (`app/sync/flows/dahua_push.py`): Executes queued actions against devices via `app/clients/dahua.py` (HTTP CGI protocol, not REST). Per-device concurrency limits (max 2).

Additionally, `app/sync/flows/health.py` runs device health checks on a separate schedule.

### Gender-Based Gate Routing

`DahuaDevice.gate_type` is "all", "male", or "female". Members route to matching gates based on their MindBody gender field.

### Web Layer

- **Admin UI** (`app/admin/`): Jinja2 + HTMX + Tailwind. Session auth via signed cookies (`AdminAuthMiddleware`). Dashboard, member management, device CRUD, sync log viewer, export jobs.
- **API** (`app/api/`): Health endpoints (`/health`, `/health/ready`) and MindBody webhook receiver (`/webhooks/mindbody`).

### Services Layer

`app/services/` contains thin async query wrappers over SQLAlchemy ORM. All DB access from routes and sync tasks goes through services — never direct model queries in route handlers.

### App Startup

`app/main.py` uses a lifespan context manager that: initializes the async DB engine, creates tables, seeds the admin user and Dahua devices from env vars, and recovers stuck export jobs.

## Testing

- **No external DB needed**: Tests use in-memory SQLite via aiosqlite
- **Async by default**: `asyncio_mode = auto` in pytest.ini — no need to mark async tests
- **HTTP mocking**: Uses `respx` library (not `responses` or `unittest.mock` for HTTP)
- **Fake clients**: `tests/helpers/fakes.py` has `FakeMindBodyClient` and `FakeDahuaClient`
- **Factories**: `tests/helpers/factories.py` for creating model instances
- **Network tests**: `@pytest.mark.network` marker — excluded from CI

## Ruff Configuration

- Line length: 100
- Target: Python 3.12
- Enabled rule sets: E, W, F, I, UP, B, N
- `E501` ignored (formatter handles line length)
- `B008` ignored (FastAPI uses function calls in default args)
- isort knows `app` as first-party
