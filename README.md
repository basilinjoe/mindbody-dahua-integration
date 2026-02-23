# MindBody ↔ Dahua Gate Sync

Syncs MindBody fitness membership status with Dahua ASI7214S face recognition speed gates. Active members are automatically enrolled with face photos and granted gate access; inactive/expired members are frozen on the devices.

## Features

- **Automatic sync** — Scheduled polling (every 30 min) + real-time MindBody webhooks
- **Multi-device** — Manage multiple Dahua gates from a single admin UI
- **Face enrollment** — Auto-pulls photos from MindBody profiles, with manual upload fallback
- **Freeze/reactivate** — Deactivated members keep their face data for instant reactivation
- **Admin dashboard** — Member management, device health monitoring, sync logs

## Quick Start

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your MindBody API credentials and Dahua device details
```

### 2. Run with Docker

```bash
docker-compose up --build
```

### 3. Run locally

```bash
uv venv && source .venv/Scripts/activate  # or .venv/bin/activate on Linux/Mac
uv pip install -r requirements.txt
uvicorn app.main:app --reload
```

### 4. Open Admin UI

Navigate to `http://localhost:8000/admin/login`

Default credentials: `admin` / `changeme` (change via `ADMIN_USERNAME` and `ADMIN_PASSWORD` in `.env`)

## Configuration

All settings are configured via environment variables (`.env` file):

| Variable | Description | Default |
|---|---|---|
| `MINDBODY_API_KEY` | MindBody API key | — |
| `MINDBODY_SITE_ID` | MindBody site ID | — |
| `MINDBODY_USERNAME` | Staff username for API auth | — |
| `MINDBODY_PASSWORD` | Staff password for API auth | — |
| `MINDBODY_WEBHOOK_SIGNATURE_KEY` | HMAC key for webhook verification | — |
| `DAHUA_DEFAULT_HOST` | Default Dahua device IP (optional, can add via UI) | — |
| `DAHUA_DEFAULT_PORT` | Default device port | `80` |
| `DAHUA_DEFAULT_USERNAME` | Default device username | `admin` |
| `DAHUA_DEFAULT_PASSWORD` | Default device password | — |
| `DAHUA_DEFAULT_DOOR_IDS` | Comma-separated door indices | `0` |
| `SYNC_INTERVAL_MINUTES` | Full sync interval | `30` |
| `DEVICE_HEALTH_INTERVAL_MINUTES` | Device ping interval | `5` |
| `ADMIN_USERNAME` | Admin UI username (seeded on first boot) | `admin` |
| `ADMIN_PASSWORD` | Admin UI password (seeded on first boot) | `changeme` |
| `SECRET_KEY` | Session cookie signing key | — |

## Architecture

```
MindBody API ──────┐
  (memberships,    │     ┌──────────────┐     ┌─── Dahua Gate 1
   contracts,      ├────►│  Sync Engine │────►├─── Dahua Gate 2
   webhooks)       │     └──────┬───────┘     └─── Dahua Gate 3
                   │            │
                   │      ┌─────┴─────┐
                   │      │  SQLite   │
                   │      │ (sync DB) │
                   │      └───────────┘
                   │
  Admin UI ────────┘
  (Jinja2 + HTMX + Tailwind)
```

**Sync logic:**
- Active in MindBody + missing from device → **enroll** (add user + upload face photo)
- Inactive in MindBody + active on device → **freeze** (`CardStatus=4`, preserves face data)
- Active in MindBody + frozen on device → **reactivate** (`CardStatus=0`)

## Admin UI Pages

| Page | Path | Description |
|---|---|---|
| Dashboard | `/admin/` | Stats, recent activity, quick actions |
| Members | `/admin/members` | Searchable member list with filters |
| Member Detail | `/admin/members/{id}` | Status, photo upload, sync history |
| Add Member | `/admin/members/add` | Manual enrollment (non-MindBody) |
| Devices | `/admin/devices` | Device cards with health status |
| Add/Edit Device | `/admin/devices/add` | Connect a new Dahua gate |
| Sync Logs | `/admin/sync` | Filterable audit trail |

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness probe |
| `GET /health/ready` | Readiness probe (checks DB + device connectivity) |
| `HEAD /webhooks/mindbody` | MindBody webhook URL validation |
| `POST /webhooks/mindbody` | MindBody webhook receiver |

## Tech Stack

- **Python 3.12+** / **FastAPI** — async web framework
- **Jinja2 + HTMX + Tailwind CSS** — server-rendered admin UI
- **httpx** — async HTTP client (digest auth for Dahua)
- **SQLAlchemy** — SQLite for sync state
- **APScheduler** — periodic sync and health checks
- **Pillow** — face photo processing
- **Docker** — containerized deployment
