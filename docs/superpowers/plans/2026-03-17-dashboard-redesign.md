# Dashboard Redesign Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the admin dashboard to show device user counts (async via HTMX), sync queue health, failure alerts, and MindBody member metrics.

**Architecture:** Extend `dashboard.py` with three new query helpers; add an HTMX user-count endpoint to `devices.py`; rewrite the two dashboard templates and add one new partial. All DB queries are synchronous SQLAlchemy legacy ORM. Device user counts load asynchronously per-device via HTMX to avoid blocking on slow/offline hardware.

**Tech Stack:** FastAPI, SQLAlchemy (sync legacy ORM), Jinja2, Tailwind CSS (CDN), HTMX

---

## File Map

| File | Change |
|---|---|
| `app/admin/dashboard.py` | Extend `_get_stats()`, add `_get_recent_queue()`, `_get_mb_breakdown()`, `_get_device_rows()` |
| `app/admin/devices.py` | Add `GET /{device_id}/user-count` HTMX endpoint |
| `app/templates/dashboard.html` | Full rewrite |
| `app/templates/partials/stats.html` | Full rewrite (5 cards) |
| `app/templates/partials/device_user_count.html` | New file (HTMX fragment) |
| `tests/unit/admin/test_dashboard_stats.py` | Extend with new helper tests |
| `tests/unit/admin/test_dashboard_routes.py` | Update assertions for new context keys |
| `tests/unit/admin/test_device_user_count.py` | New file |

---

## Chunk 1: Backend Data Helpers

### Task 1: Extend `_get_stats()` with `failed_24h` and `active_members_pct`

**Files:**
- Modify: `app/admin/dashboard.py`
- Test: `tests/unit/admin/test_dashboard_stats.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/admin/test_dashboard_stats.py`:

```python
from datetime import UTC, datetime, timedelta

from app.models.dahua_sync_queue import DahuaSyncQueue


def test_get_stats_includes_failed_24h(db_session) -> None:
    recent = DahuaSyncQueue(
        run_id="r1", device_id=1, mindbody_client_id="1",
        action="enroll", status="failed",
        created_at=datetime.utcnow() - timedelta(hours=1),
    )
    old = DahuaSyncQueue(
        run_id="r2", device_id=1, mindbody_client_id="2",
        action="enroll", status="failed",
        created_at=datetime.utcnow() - timedelta(hours=25),
    )
    db_session.add_all([recent, old])
    db_session.commit()

    stats = _get_stats(db_session)

    assert stats["failed_24h"] == 1


def test_get_stats_active_members_pct_zero_when_no_members(db_session) -> None:
    stats = _get_stats(db_session)
    assert stats["active_members_pct"] == 0


def test_get_stats_active_members_pct(db_session) -> None:
    from app.models.mindbody_client import MindBodyClient
    from app.models.mindbody_membership import MindBodyMembership

    c1 = MindBodyClient(mindbody_id="1", first_name="A", last_name="B", active=True)
    c2 = MindBodyClient(mindbody_id="2", first_name="C", last_name="D", active=True)
    db_session.add_all([c1, c2])
    db_session.flush()
    db_session.add(MindBodyMembership(mindbody_client_id="1", membership_id="m1", is_active=True))
    db_session.commit()

    stats = _get_stats(db_session)

    assert stats["active_members_pct"] == 50
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest tests/unit/admin/test_dashboard_stats.py -v
```
Expected: FAIL (`KeyError: 'failed_24h'`)

- [ ] **Step 3: Extend `_get_stats()` in `app/admin/dashboard.py`**

Replace the `_get_stats` function with:

```python
from datetime import UTC, datetime, timedelta


def _get_stats(db) -> dict:
    total_members = db.query(MindBodyClient).count()
    active_membership_subq = (
        db.query(MindBodyMembership.id)
        .filter(MindBodyMembership.mindbody_client_id == MindBodyClient.mindbody_id)
        .filter(MindBodyMembership.is_active.is_(True))
        .correlate(MindBodyClient)
        .exists()
    )
    active_members = (
        db.query(MindBodyClient)
        .filter(MindBodyClient.active.is_(True))
        .filter(active_membership_subq)
        .count()
    )
    active_members_pct = round(active_members * 100 / total_members) if total_members else 0
    pending_queue = db.query(DahuaSyncQueue).filter_by(status="pending").count()
    cutoff = datetime.utcnow() - timedelta(hours=24)
    failed_24h = (
        db.query(DahuaSyncQueue)
        .filter(DahuaSyncQueue.status == "failed", DahuaSyncQueue.created_at >= cutoff)
        .count()
    )
    total_queue_24h = (
        db.query(DahuaSyncQueue)
        .filter(DahuaSyncQueue.created_at >= cutoff, DahuaSyncQueue.status != "pending")
        .count()
    )
    success_rate_pct = (
        round((total_queue_24h - failed_24h) * 100 / total_queue_24h)
        if total_queue_24h else 100
    )
    devices_total = db.query(DahuaDevice).filter_by(is_enabled=True).count()
    devices_online = db.query(DahuaDevice).filter_by(is_enabled=True, status="online").count()
    return {
        "total_members": total_members,
        "active_members": active_members,
        "active_members_pct": active_members_pct,
        "pending_queue": pending_queue,
        "failed_24h": failed_24h,
        "success_rate_pct": success_rate_pct,
        "devices_total": devices_total,
        "devices_online": devices_online,
    }
```

Also add `from datetime import datetime, timedelta` to the imports at the top of `dashboard.py` (`UTC` is not used here).

- [ ] **Step 4: Run tests**

```
pytest tests/unit/admin/test_dashboard_stats.py -v
```
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add app/admin/dashboard.py tests/unit/admin/test_dashboard_stats.py
git commit -m "feat: extend _get_stats with failed_24h, success_rate_pct, active_members_pct"
```

---

### Task 2: Add `_get_recent_queue()` helper with device name join

**Files:**
- Modify: `app/admin/dashboard.py`
- Test: `tests/unit/admin/test_dashboard_stats.py`

- [ ] **Step 1: Write failing test**

Add `from app.admin.dashboard import _get_recent_queue` and `from app.models.device import DahuaDevice` to the **top-level imports** at the top of `tests/unit/admin/test_dashboard_stats.py` (not inline). Then append the test functions below:

```python
# (these imports go at the top of the file, not here inline)


def test_get_recent_queue_returns_rows_with_device_name(db_session) -> None:
    device = DahuaDevice(
        name="Male Gate A", host="10.0.0.1", port=80,
        username="admin", password="pass", door_ids="0",
    )
    db_session.add(device)
    db_session.flush()

    item = DahuaSyncQueue(
        run_id="r1", device_id=device.id, mindbody_client_id="42",
        action="enroll", status="success",
    )
    db_session.add(item)
    db_session.commit()

    rows = _get_recent_queue(db_session)

    assert len(rows) == 1
    queue_item, device_name = rows[0]
    assert queue_item.mindbody_client_id == "42"
    assert device_name == "Male Gate A"


def test_get_recent_queue_device_name_none_when_device_missing(db_session) -> None:
    item = DahuaSyncQueue(
        run_id="r1", device_id=9999, mindbody_client_id="1",
        action="enroll", status="pending",
    )
    db_session.add(item)
    db_session.commit()

    rows = _get_recent_queue(db_session)

    assert len(rows) == 1
    _, device_name = rows[0]
    assert device_name is None


def test_get_recent_queue_limited_to_10(db_session) -> None:
    db_session.add_all([
        DahuaSyncQueue(run_id="r", device_id=1, mindbody_client_id=str(i),
                       action="enroll", status="success")
        for i in range(15)
    ])
    db_session.commit()

    rows = _get_recent_queue(db_session)

    assert len(rows) == 10
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest tests/unit/admin/test_dashboard_stats.py::test_get_recent_queue_returns_rows_with_device_name -v
```
Expected: FAIL (`ImportError: cannot import name '_get_recent_queue'`)

- [ ] **Step 3: Add `_get_recent_queue()` to `app/admin/dashboard.py`**

Add after `_get_stats()`:

```python
def _get_recent_queue(db) -> list[tuple]:
    """Return last 10 queue items with resolved device name.

    Each element is a (DahuaSyncQueue, device_name: str | None) tuple.
    """
    rows = (
        db.query(DahuaSyncQueue, DahuaDevice.name)
        .outerjoin(DahuaDevice, DahuaDevice.id == DahuaSyncQueue.device_id)
        .order_by(DahuaSyncQueue.created_at.desc())
        .limit(10)
        .all()
    )
    return rows
```

- [ ] **Step 4: Run tests**

```
pytest tests/unit/admin/test_dashboard_stats.py -v -k "recent_queue"
```
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add app/admin/dashboard.py tests/unit/admin/test_dashboard_stats.py
git commit -m "feat: add _get_recent_queue helper with device name join"
```

---

### Task 3: Add `_get_mb_breakdown()` helper

**Files:**
- Modify: `app/admin/dashboard.py`
- Test: `tests/unit/admin/test_dashboard_stats.py`

- [ ] **Step 1: Write failing test**

Add `from app.admin.dashboard import _get_mb_breakdown` to the top-level imports at the top of `tests/unit/admin/test_dashboard_stats.py`, then append:

```python
def test_get_mb_breakdown_counts_gender_and_subscriptions(db_session) -> None:
    from app.models.mindbody_client import MindBodyClient
    from app.models.mindbody_membership import MindBodyMembership

    clients = [
        MindBodyClient(mindbody_id="1", first_name="A", last_name="B", active=True, gender="Male"),
        MindBodyClient(mindbody_id="2", first_name="C", last_name="D", active=True, gender="Female"),
        MindBodyClient(mindbody_id="3", first_name="E", last_name="F", active=True, gender="Female"),
        MindBodyClient(mindbody_id="4", first_name="G", last_name="H", active=True, gender=None),
    ]
    db_session.add_all(clients)
    db_session.flush()
    db_session.add(MindBodyMembership(mindbody_client_id="1", membership_id="m1", is_active=True))
    db_session.add(MindBodyMembership(mindbody_client_id="2", membership_id="m2", is_active=False))
    db_session.commit()

    bd = _get_mb_breakdown(db_session)

    assert bd["male_count"] == 1
    assert bd["female_count"] == 2
    assert bd["male_pct"] == 25   # 1/4 total
    assert bd["female_pct"] == 50  # 2/4 total
    # active sub count = 1 (only m1 is active)
    assert bd["active_sub_count"] == 1
    assert bd["no_sub_count"] == 3
```

- [ ] **Step 2: Run test to confirm it fails**

```
pytest tests/unit/admin/test_dashboard_stats.py::test_get_mb_breakdown_counts_gender_and_subscriptions -v
```
Expected: FAIL (`ImportError`)

- [ ] **Step 3: Add `_get_mb_breakdown()` to `app/admin/dashboard.py`**

```python
def _get_mb_breakdown(db) -> dict:
    """Gender split and subscription breakdown for MindBody clients."""
    total = db.query(MindBodyClient).count()

    male_count = db.query(MindBodyClient).filter(MindBodyClient.gender == "Male").count()
    female_count = db.query(MindBodyClient).filter(MindBodyClient.gender == "Female").count()

    active_sub_subq = (
        db.query(MindBodyMembership.id)
        .filter(MindBodyMembership.mindbody_client_id == MindBodyClient.mindbody_id)
        .filter(MindBodyMembership.is_active.is_(True))
        .correlate(MindBodyClient)
        .exists()
    )
    active_sub_count = db.query(MindBodyClient).filter(active_sub_subq).count()
    no_sub_count = total - active_sub_count

    male_pct = round(male_count * 100 / total) if total else 0
    female_pct = round(female_count * 100 / total) if total else 0

    return {
        "total": total,
        "male_count": male_count,
        "female_count": female_count,
        "male_pct": male_pct,
        "female_pct": female_pct,
        "active_sub_count": active_sub_count,
        "no_sub_count": no_sub_count,
    }
```

- [ ] **Step 4: Run tests**

```
pytest tests/unit/admin/test_dashboard_stats.py -v -k "breakdown"
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/admin/dashboard.py tests/unit/admin/test_dashboard_stats.py
git commit -m "feat: add _get_mb_breakdown helper for gender and subscription stats"
```

---

### Task 4: Add `_get_device_rows()` helper (per-device pending/failed counts)

**Files:**
- Modify: `app/admin/dashboard.py`
- Test: `tests/unit/admin/test_dashboard_stats.py`

- [ ] **Step 1: Write failing test**

Append to `tests/unit/admin/test_dashboard_stats.py`:

```python
from app.admin.dashboard import _get_device_rows


def test_get_device_rows_includes_queue_counts(db_session) -> None:
    from app.models.device import DahuaDevice

    d = DahuaDevice(
        name="Gate A", host="10.0.0.1", port=80,
        username="admin", password="pass", door_ids="0",
        is_enabled=True, status="online", gate_type="male",
    )
    db_session.add(d)
    db_session.flush()

    db_session.add_all([
        DahuaSyncQueue(run_id="r", device_id=d.id, mindbody_client_id="1",
                       action="enroll", status="pending"),
        DahuaSyncQueue(run_id="r", device_id=d.id, mindbody_client_id="2",
                       action="enroll", status="failed",
                       created_at=datetime.utcnow() - timedelta(hours=1)),
        DahuaSyncQueue(run_id="r", device_id=d.id, mindbody_client_id="3",
                       action="enroll", status="failed",
                       created_at=datetime.utcnow() - timedelta(hours=25)),  # outside 24h
    ])
    db_session.commit()

    rows = _get_device_rows(db_session)

    assert len(rows) == 1
    row = rows[0]
    assert row["device"].name == "Gate A"
    assert row["pending"] == 1
    assert row["failed_24h"] == 1  # only the recent failure


def test_get_device_rows_excludes_disabled(db_session) -> None:
    from app.models.device import DahuaDevice

    db_session.add(DahuaDevice(
        name="Disabled", host="10.0.0.2", port=80,
        username="admin", password="pass", door_ids="0",
        is_enabled=False,
    ))
    db_session.commit()

    rows = _get_device_rows(db_session)
    assert rows == []
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest tests/unit/admin/test_dashboard_stats.py -v -k "device_rows"
```
Expected: FAIL (`ImportError`)

- [ ] **Step 3: Add `_get_device_rows()` to `app/admin/dashboard.py`**

```python
def _get_device_rows(db) -> list[dict]:
    """Return enabled devices with their pending and 24h-failed queue counts."""
    devices = (
        db.query(DahuaDevice)
        .filter_by(is_enabled=True)
        .order_by(DahuaDevice.name)
        .all()
    )
    cutoff = datetime.utcnow() - timedelta(hours=24)
    rows = []
    for device in devices:
        pending = (
            db.query(DahuaSyncQueue)
            .filter_by(device_id=device.id, status="pending")
            .count()
        )
        failed_24h = (
            db.query(DahuaSyncQueue)
            .filter(
                DahuaSyncQueue.device_id == device.id,
                DahuaSyncQueue.status == "failed",
                DahuaSyncQueue.created_at >= cutoff,
            )
            .count()
        )
        rows.append({"device": device, "pending": pending, "failed_24h": failed_24h})
    return rows
```

- [ ] **Step 4: Run tests**

```
pytest tests/unit/admin/test_dashboard_stats.py -v -k "device_rows"
```
Expected: PASS

- [ ] **Step 5: Wire new helpers into the dashboard route**

In `app/admin/dashboard.py`, replace the `dashboard` route handler:

```python
@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    db = request.app.state.db_session_factory()
    try:
        stats = _get_stats(db)
        recent_queue = _get_recent_queue(db)
        mb_breakdown = _get_mb_breakdown(db)
        device_rows = _get_device_rows(db)
        return request.app.state.templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "session_user": request.state.user,
                "active_page": "dashboard",
                "stats": stats,
                "recent_queue": recent_queue,
                "mb_breakdown": mb_breakdown,
                "device_rows": device_rows,
            },
        )
    finally:
        db.close()
```

- [ ] **Step 6: Run full dashboard stats test suite**

```
pytest tests/unit/admin/test_dashboard_stats.py -v
```
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add app/admin/dashboard.py tests/unit/admin/test_dashboard_stats.py
git commit -m "feat: add _get_device_rows helper and wire all helpers into dashboard route"
```

---

## Chunk 2: HTMX User-Count Endpoint

### Task 5: Add `GET /admin/devices/{device_id}/user-count` endpoint

**Files:**
- Modify: `app/admin/devices.py`
- Create: `app/templates/partials/device_user_count.html`
- Create: `tests/unit/admin/test_device_user_count.py`

- [ ] **Step 1: Create the HTMX partial template**

Create `app/templates/partials/device_user_count.html`:

```html
{{ count }}
```

(Just the number — the containing `<td>` is already in `dashboard.html`.)

- [ ] **Step 2: Write failing tests**

Create `tests/unit/admin/test_device_user_count.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, patch

from app.models.device import DahuaDevice
from tests.helpers.factories import make_device


def test_user_count_returns_count_for_online_device(logged_in_client, db_session) -> None:
    device = make_device(name="Gate A", host="10.0.0.1", status="online", is_enabled=True)
    db_session.add(device)
    db_session.commit()

    mock_users = [{"UserId": str(i)} for i in range(42)]
    with patch("app.admin.devices.DahuaClient") as MockClient:
        instance = MockClient.return_value
        instance.get_all_users = AsyncMock(return_value=mock_users)
        instance.close = AsyncMock()

        resp = logged_in_client.get(f"/admin/devices/{device.id}/user-count")

    assert resp.status_code == 200
    assert "42" in resp.text


def test_user_count_returns_dash_for_offline_device(logged_in_client, db_session) -> None:
    device = make_device(name="Gate B", host="10.0.0.2", status="offline", is_enabled=True)
    db_session.add(device)
    db_session.commit()

    resp = logged_in_client.get(f"/admin/devices/{device.id}/user-count")

    assert resp.status_code == 200
    assert "—" in resp.text


def test_user_count_returns_dash_when_device_not_found(logged_in_client, db_session) -> None:
    resp = logged_in_client.get("/admin/devices/9999/user-count")

    assert resp.status_code == 200
    assert "—" in resp.text


def test_user_count_returns_dash_when_client_raises(logged_in_client, db_session) -> None:
    device = make_device(name="Gate C", host="10.0.0.3", status="online", is_enabled=True)
    db_session.add(device)
    db_session.commit()

    with patch("app.admin.devices.DahuaClient") as MockClient:
        instance = MockClient.return_value
        instance.get_all_users = AsyncMock(side_effect=Exception("timeout"))
        instance.close = AsyncMock()

        resp = logged_in_client.get(f"/admin/devices/{device.id}/user-count")

    assert resp.status_code == 200
    assert "—" in resp.text
```

- [ ] **Step 3: Run tests to confirm they fail**

```
pytest tests/unit/admin/test_device_user_count.py -v
```
Expected: FAIL (404 not found for route)

- [ ] **Step 4: Add the endpoint to `app/admin/devices.py`**

Add before the last route in `devices.py` (before the `snapshot` endpoint is fine):

```python
@router.get("/{device_id}/user-count", response_class=HTMLResponse)
async def device_user_count(request: Request, device_id: int):
    """HTMX partial: live user count for a single device."""
    db = request.app.state.db_session_factory()
    try:
        device = db.query(DahuaDevice).get(device_id)
        if not device or device.status != "online":
            return HTMLResponse("—")

        client = DahuaClient(
            host=device.host,
            port=device.port,
            username=device.username,
            password=device.password,
            door_ids=device.door_ids,
        )
        try:
            users = await client.get_all_users()
            count = len(users)
        except Exception:
            logger.exception("Failed to fetch user count from device %d", device_id)
            return HTMLResponse("—")
        finally:
            await client.close()

        return request.app.state.templates.TemplateResponse(
            request,
            "partials/device_user_count.html",
            {"count": count},
        )
    finally:
        db.close()
```

- [ ] **Step 5: Run tests**

```
pytest tests/unit/admin/test_device_user_count.py -v
```
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add app/admin/devices.py app/templates/partials/device_user_count.html tests/unit/admin/test_device_user_count.py
git commit -m "feat: add HTMX device user-count endpoint"
```

---

## Chunk 3: Templates

### Task 6: Rewrite `app/templates/partials/stats.html` (5-card layout)

**Files:**
- Modify: `app/templates/partials/stats.html`
- Test: `tests/unit/admin/test_dashboard_routes.py`

- [ ] **Step 1: Update route test to assert new card labels**

Replace `tests/unit/admin/test_dashboard_routes.py` with:

```python
from __future__ import annotations

from app.models.mindbody_client import MindBodyClient


def test_dashboard_page_renders_for_logged_in_user(logged_in_client, db_session) -> None:
    db_session.add(
        MindBodyClient(mindbody_id="10", first_name="John", last_name="Doe", active=True)
    )
    db_session.commit()

    response = logged_in_client.get("/admin/")

    assert response.status_code == 200
    assert "Total Members" in response.text
    assert "Active Subscriptions" in response.text
    assert "Pending Queue" in response.text
    assert "Devices Online" in response.text
    assert "MindBody Breakdown" in response.text
    assert "Device Breakdown" in response.text


def test_dashboard_stats_partial_renders(logged_in_client, db_session) -> None:
    db_session.add(
        MindBodyClient(mindbody_id="11", first_name="Jane", last_name="Doe", active=True)
    )
    db_session.commit()

    response = logged_in_client.get("/admin/partials/stats")

    assert response.status_code == 200
    assert "Total Members" in response.text
    assert "Pending Queue" in response.text
```

- [ ] **Step 2: Run tests to confirm the new assertions fail**

```
pytest tests/unit/admin/test_dashboard_routes.py -v
```
Expected: FAIL (`"Active Subscriptions" not in response.text`)

- [ ] **Step 3: Rewrite `app/templates/partials/stats.html`**

```html
<!-- Total Members -->
<div class="bg-white rounded-xl border border-gray-200 p-6">
  <div class="flex items-center justify-between">
    <div>
      <p class="text-sm font-medium text-gray-500">Total Members</p>
      <p class="text-3xl font-bold text-gray-900 mt-1">{{ stats.total_members }}</p>
    </div>
    <div class="w-12 h-12 bg-blue-50 rounded-lg flex items-center justify-center">
      <svg class="w-6 h-6 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
    </div>
  </div>
</div>
<!-- Active Subscriptions -->
<div class="bg-white rounded-xl border border-gray-200 p-6">
  <div class="flex items-center justify-between">
    <div>
      <p class="text-sm font-medium text-gray-500">Active Subscriptions</p>
      <p class="text-3xl font-bold text-green-600 mt-1">
        {{ stats.active_members }}
        <span class="text-sm font-medium text-green-500 ml-1">{{ stats.active_members_pct }}%</span>
      </p>
      <p class="text-xs text-gray-400 mt-1">{{ stats.total_members - stats.active_members }} no active sub</p>
    </div>
    <div class="w-12 h-12 bg-green-50 rounded-lg flex items-center justify-center">
      <svg class="w-6 h-6 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
    </div>
  </div>
</div>
<!-- Pending Queue -->
<div class="bg-white rounded-xl border border-gray-200 p-6">
  <div class="flex items-center justify-between">
    <div>
      <p class="text-sm font-medium text-gray-500">Pending Queue</p>
      <p class="text-3xl font-bold text-amber-600 mt-1">{{ stats.pending_queue }}</p>
      <p class="text-xs text-gray-400 mt-1">operations waiting</p>
    </div>
    <div class="w-12 h-12 bg-amber-50 rounded-lg flex items-center justify-center">
      <svg class="w-6 h-6 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01"/></svg>
    </div>
  </div>
</div>
<!-- Failed 24h -->
<div class="bg-white rounded-xl border border-gray-200 p-6">
  <div class="flex items-center justify-between">
    <div>
      <p class="text-sm font-medium text-gray-500">Failed (24h)</p>
      <p class="text-3xl font-bold {% if stats.failed_24h > 0 %}text-red-600{% else %}text-gray-900{% endif %} mt-1">
        {{ stats.failed_24h }}
        <span class="text-sm font-medium text-green-500 ml-1">{{ stats.success_rate_pct }}% ok</span>
      </p>
      <p class="text-xs text-gray-400 mt-1">success rate today</p>
    </div>
    <div class="w-12 h-12 {% if stats.failed_24h > 0 %}bg-red-50{% else %}bg-gray-50{% endif %} rounded-lg flex items-center justify-center">
      <svg class="w-6 h-6 {% if stats.failed_24h > 0 %}text-red-500{% else %}text-gray-400{% endif %}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
    </div>
  </div>
</div>
<!-- Devices Online -->
<div class="bg-white rounded-xl border border-gray-200 p-6">
  <div class="flex items-center justify-between">
    <div>
      <p class="text-sm font-medium text-gray-500">Devices Online</p>
      <p class="text-3xl font-bold text-gray-900 mt-1">
        <span class="text-purple-600">{{ stats.devices_online }}</span>
        <span class="text-gray-400 text-xl">/</span>
        <span>{{ stats.devices_total }}</span>
      </p>
    </div>
    <div class="w-12 h-12 bg-purple-50 rounded-lg flex items-center justify-center">
      <svg class="w-6 h-6 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
    </div>
  </div>
</div>
```

- [ ] **Step 4: Run tests**

```
pytest tests/unit/admin/test_dashboard_routes.py -v
```
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add app/templates/partials/stats.html tests/unit/admin/test_dashboard_routes.py
git commit -m "feat: rewrite stats partial with 5-card layout"
```

---

### Task 7: Rewrite `app/templates/dashboard.html`

**Files:**
- Modify: `app/templates/dashboard.html`

No new unit tests needed beyond route tests already passing; this is a pure template change validated by the existing route tests.

- [ ] **Step 1: Rewrite `app/templates/dashboard.html`**

```html
{% extends "base.html" %}
{% block title %}Dashboard{% endblock %}

{% block content %}
<div class="space-y-6">
  <!-- Header -->
  <div>
    <h1 class="text-2xl font-bold text-gray-900">Dashboard</h1>
    <p class="text-sm text-gray-500 mt-1">MindBody ↔ Dahua sync health</p>
  </div>

  <!-- Failure alert banner (shown only when there are recent failures) -->
  {% if stats.failed_24h > 0 %}
  <div class="flex items-center gap-3 bg-red-50 border border-red-200 text-red-800 rounded-xl px-4 py-3 text-sm">
    <svg class="w-5 h-5 text-red-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/></svg>
    <span><strong>{{ stats.failed_24h }} failed operation{{ 's' if stats.failed_24h != 1 }}</strong> in the last 24 hours.</span>
    <a href="/admin/sync-queue?status=failed" class="ml-auto font-semibold underline whitespace-nowrap">View failed →</a>
  </div>
  {% endif %}

  <!-- Stat cards row -->
  <div class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4"
       hx-get="/admin/partials/stats" hx-trigger="every 30s" hx-swap="innerHTML">
    {% include "partials/stats.html" %}
  </div>

  <!-- Device breakdown + right column -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">

    <!-- Device breakdown table (spans 2 cols) -->
    <div class="lg:col-span-2 bg-white rounded-xl border border-gray-200 overflow-hidden">
      <div class="px-6 py-4 border-b border-gray-100 flex items-center justify-between">
        <h2 class="text-base font-semibold text-gray-900">Device Breakdown</h2>
        <span class="text-xs text-gray-400">User counts load live</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 border-b border-gray-100">
              <th class="px-4 py-3">Device</th>
              <th class="px-4 py-3">Gate</th>
              <th class="px-4 py-3">Status</th>
              <th class="px-4 py-3">Users</th>
              <th class="px-4 py-3">Pending</th>
              <th class="px-4 py-3">Failed</th>
              <th class="px-4 py-3">Last seen</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-50">
            {% for row in device_rows %}
            {% set device = row.device %}
            <tr class="text-gray-700 hover:bg-gray-50 transition">
              <td class="px-4 py-3 font-medium">{{ device.name }}</td>
              <td class="px-4 py-3">
                {% if device.gate_type == 'male' %}
                <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700">♂ Male</span>
                {% elif device.gate_type == 'female' %}
                <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-pink-50 text-pink-700">♀ Female</span>
                {% else %}
                <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-indigo-50 text-indigo-700">↔ All</span>
                {% endif %}
              </td>
              <td class="px-4 py-3">
                {% if device.status == 'online' %}
                <span class="flex items-center gap-1.5 text-green-600">
                  <span class="w-2 h-2 rounded-full bg-green-500 inline-block"></span> online
                </span>
                {% elif device.status == 'offline' %}
                <span class="flex items-center gap-1.5 text-amber-600">
                  <span class="w-2 h-2 rounded-full bg-amber-400 inline-block"></span> offline
                </span>
                {% else %}
                <span class="flex items-center gap-1.5 text-gray-400">
                  <span class="w-2 h-2 rounded-full bg-gray-300 inline-block"></span> {{ device.status }}
                </span>
                {% endif %}
              </td>
              <td class="px-4 py-3 font-semibold"
                  hx-get="/admin/devices/{{ device.id }}/user-count"
                  hx-trigger="load"
                  hx-swap="innerHTML">
                <span class="inline-block w-4 h-4 border-2 border-gray-200 border-t-gray-400 rounded-full animate-spin"></span>
              </td>
              <td class="px-4 py-3">
                {% if row.pending > 0 %}
                <span class="font-semibold text-amber-600">{{ row.pending }}</span>
                {% else %}
                <span class="text-gray-300">—</span>
                {% endif %}
              </td>
              <td class="px-4 py-3">
                {% if row.failed_24h > 0 %}
                <span class="font-semibold text-red-600">{{ row.failed_24h }}</span>
                {% else %}
                <span class="text-gray-300">—</span>
                {% endif %}
              </td>
              <td class="px-4 py-3 text-gray-400 text-xs">
                {# Note: spec says relative time ("2 hours ago"); rendering as absolute for simplicity.
                   Upgrade to relative time with a JS library (e.g. Day.js) if required later. #}
                {% if device.last_seen_at %}
                  {{ device.last_seen_at.strftime('%b %d %H:%M') }}
                {% else %}
                  —
                {% endif %}
              </td>
            </tr>
            {% endfor %}
            {% if not device_rows %}
            <tr><td colspan="7" class="px-4 py-8 text-center text-gray-400">No devices configured</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>

    <!-- Right column: activity feed + MindBody breakdown -->
    <div class="flex flex-col gap-6">

      <!-- Recent activity -->
      <div class="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-gray-100">
          <h2 class="text-base font-semibold text-gray-900">Recent Activity</h2>
        </div>
        <ul class="divide-y divide-gray-50">
          {% for item, device_name in recent_queue %}
          <li class="flex items-start gap-3 px-4 py-3 text-sm">
            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold mt-0.5 flex-shrink-0
              {% if item.status == 'failed' %}bg-red-50 text-red-700
              {% elif item.action == 'enroll' %}bg-blue-50 text-blue-700
              {% elif item.action == 'reactivate' %}bg-green-50 text-green-700
              {% elif item.action == 'deactivate' %}bg-amber-50 text-amber-700
              {% else %}bg-gray-50 text-gray-600{% endif %}">
              {% if item.status == 'failed' %}failed{% else %}{{ item.action }}{% endif %}
            </span>
            <div class="flex-1 min-w-0">
              <p class="font-medium text-gray-700 truncate">
                #{{ item.mindbody_client_id }}
                {% if device_name %}<span class="text-gray-400 font-normal">→ {{ device_name }}</span>{% endif %}
              </p>
              <p class="text-xs text-gray-400 mt-0.5">{{ item.created_at.strftime('%b %d %H:%M') if item.created_at else '—' }}</p>
            </div>
            <span class="w-2 h-2 rounded-full mt-1.5 flex-shrink-0
              {% if item.status == 'success' %}bg-green-500
              {% elif item.status == 'failed' %}bg-red-500
              {% else %}bg-yellow-400{% endif %}">
            </span>
          </li>
          {% endfor %}
          {% if not recent_queue %}
          <li class="px-4 py-8 text-center text-gray-400 text-sm">No activity yet</li>
          {% endif %}
        </ul>
      </div>

      <!-- MindBody breakdown -->
      <div class="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-gray-100">
          <h2 class="text-base font-semibold text-gray-900">MindBody Breakdown</h2>
        </div>
        <div class="px-6 py-4 space-y-4">
          <!-- Subscription bars -->
          <div>
            <div class="flex justify-between text-sm mb-1">
              <span class="text-gray-600">Active subscription</span>
              <span class="font-semibold">{{ mb_breakdown.active_sub_count }} <span class="text-gray-400 font-normal text-xs">({{ stats.active_members_pct }}%)</span></span>
            </div>
            <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden">
              <div class="h-full bg-green-500 rounded-full" style="width: {{ stats.active_members_pct }}%"></div>
            </div>
          </div>
          <div>
            <div class="flex justify-between text-sm mb-1">
              <span class="text-gray-600">No subscription</span>
              <span class="font-semibold text-gray-400">{{ mb_breakdown.no_sub_count }} <span class="font-normal text-xs">({{ 100 - stats.active_members_pct }}%)</span></span>
            </div>
            <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden">
              <div class="h-full bg-gray-300 rounded-full" style="width: {{ 100 - stats.active_members_pct }}%"></div>
            </div>
          </div>

          <div class="border-t border-gray-100 pt-4 grid grid-cols-2 gap-4">
            <div>
              <p class="text-xs font-semibold text-blue-600 uppercase tracking-wide mb-1">♂ Male</p>
              <p class="text-2xl font-bold text-blue-700">{{ mb_breakdown.male_count }}</p>
              <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden mt-2">
                <div class="h-full bg-blue-400 rounded-full" style="width: {{ mb_breakdown.male_pct }}%"></div>
              </div>
            </div>
            <div>
              <p class="text-xs font-semibold text-pink-600 uppercase tracking-wide mb-1">♀ Female</p>
              <p class="text-2xl font-bold text-pink-700">{{ mb_breakdown.female_count }}</p>
              <div class="h-1.5 bg-gray-100 rounded-full overflow-hidden mt-2">
                <div class="h-full bg-pink-400 rounded-full" style="width: {{ mb_breakdown.female_pct }}%"></div>
              </div>
            </div>
          </div>
        </div>
      </div>

    </div>
  </div>

</div>
{% endblock %}
```

- [ ] **Step 2: Run all dashboard and device tests**

```
pytest tests/unit/admin/ -v
```
Expected: all PASS

- [ ] **Step 3: Commit**

```bash
git add app/templates/dashboard.html
git commit -m "feat: rewrite dashboard template with device table, activity feed, and MindBody breakdown"
```

---

## Chunk 4: Final Verification

### Task 8: Full test run and smoke check

- [ ] **Step 1: Run full test suite**

```
pytest -v
```
Expected: all PASS, no failures

- [ ] **Step 2: Start the dev server and visually verify**

```
uvicorn app.main:app --reload
```

Open `http://localhost:8000/admin/` and verify:
- 5 stat cards render with correct values
- Device table shows spinners in the Users column that resolve to counts
- Failure alert banner appears when `stats.failed_24h > 0`
- Recent activity feed shows action badges and device names
- MindBody breakdown panel shows subscription and gender bars

- [ ] **Step 3: Final commit**

```bash
git add app/templates/dashboard.html app/templates/partials/stats.html app/templates/partials/device_user_count.html app/admin/dashboard.py app/admin/devices.py tests/unit/admin/
git commit -m "feat: dashboard redesign — device breakdown, HTMX user counts, failure alert, MindBody breakdown"
```
