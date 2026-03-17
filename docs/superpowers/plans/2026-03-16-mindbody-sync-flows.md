# MindBody Sync Flows Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple MindBody data fetching from Dahua integration by introducing independent client and membership sync flows that feed the local DB, with the integration flow reading exclusively from that DB.

**Architecture:** Two scheduled flows (`sync-mindbody-clients`, `sync-mindbody-memberships`) independently refresh the local DB from MindBody. Each emits a Prefect event on completion. A Prefect compound automation fires `sync-integration/full` when both events are received within the same interval window. The integration flow no longer calls the MindBody API.

**Tech Stack:** Prefect 3.6.22, SQLAlchemy 2 async, Python 3.12

---

## Chunk 1: New DB tasks

### Task 1: Add three DB-backed tasks to tasks.py

**Files:**
- Modify: `app/sync/tasks.py`
- Test: `tests/unit/sync/test_db_tasks.py`

These three tasks replace MindBody API calls inside the integration and membership flows.

- [ ] **Step 1: Write failing tests**

Create `tests/unit/sync/test_db_tasks.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

import app.sync.tasks as tasks_mod


@pytest.mark.asyncio
async def test_load_all_client_ids_from_db_returns_ids(monkeypatch):
    fake_rows = [("101",), ("202",), ("303",)]
    mock_result = MagicMock()
    mock_result.fetchall.return_value = fake_rows

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)

    mock_factory = MagicMock(return_value=mock_db)
    monkeypatch.setattr(tasks_mod, "_get_async_session_factory", lambda: mock_factory)

    result = await tasks_mod.load_all_client_ids_from_db()
    assert result == ["101", "202", "303"]


@pytest.mark.asyncio
async def test_load_active_clients_from_db_maps_fields(monkeypatch):
    from app.models.mindbody_client import MindBodyClient as MBC
    fake_client = MBC(
        mindbody_id="101",
        first_name="Alice",
        last_name="Smith",
        gender="Female",
        photo_url="https://img/alice",
        email="alice@example.com",
    )
    mock_result = MagicMock()
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = [fake_client]
    mock_result.scalars.return_value = mock_scalars

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)

    mock_factory = MagicMock(return_value=mock_db)
    monkeypatch.setattr(tasks_mod, "_get_async_session_factory", lambda: mock_factory)

    result = await tasks_mod.load_active_clients_from_db()
    assert len(result) == 1
    m = result[0]
    assert m["Id"] == "101"
    assert m["FirstName"] == "Alice"
    assert m["LastName"] == "Smith"
    assert m["Gender"] == "Female"
    assert m["PhotoUrl"] == "https://img/alice"
    assert m["Email"] == "alice@example.com"


@pytest.mark.asyncio
async def test_get_active_member_ids_from_db(monkeypatch):
    fake_rows = [("101",), ("202",)]
    mock_result = MagicMock()
    mock_result.fetchall.return_value = fake_rows

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)

    mock_factory = MagicMock(return_value=mock_db)
    monkeypatch.setattr(tasks_mod, "_get_async_session_factory", lambda: mock_factory)

    result = await tasks_mod.get_active_member_ids_from_db(["101", "202", "303"])
    assert result == {"101", "202"}
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest tests/unit/sync/test_db_tasks.py -v
```
Expected: FAIL — `load_all_client_ids_from_db`, `load_active_clients_from_db`, `get_active_member_ids_from_db` not defined.

- [ ] **Step 3: Add the three tasks to tasks.py**

Add after the `load_membership_windows` task (around line 594):

```python
@task(name="load-all-client-ids-from-db", tags=["db"])
async def load_all_client_ids_from_db() -> list[str]:
    """Return all known MindBody client IDs from the local mindbody_clients table."""
    async with _get_async_session_factory()() as db:
        result = await db.execute(select(MindBodyClientModel.mindbody_id))
        return [row[0] for row in result.fetchall()]


@task(name="load-active-clients-from-db", tags=["db"])
async def load_active_clients_from_db() -> list[dict]:
    """
    Load all active MindBody clients from the local DB, shaped as API-compatible dicts
    (keys: Id, FirstName, LastName, Gender, PhotoUrl, Email).
    """
    async with _get_async_session_factory()() as db:
        result = await db.execute(
            select(MindBodyClientModel).where(MindBodyClientModel.active.is_(True))
        )
        rows = list(result.scalars().all())
    return [
        {
            "Id": row.mindbody_id,
            "FirstName": row.first_name,
            "LastName": row.last_name,
            "Gender": row.gender,
            "PhotoUrl": row.photo_url,
            "Email": row.email,
        }
        for row in rows
    ]


@task(name="get-active-member-ids-from-db", tags=["db"])
async def get_active_member_ids_from_db(client_ids: list[str]) -> set[str]:
    """
    Return subset of client_ids that have at least one active membership in the local DB.
    Does NOT call the MindBody API.
    """
    if not client_ids:
        return set()
    async with _get_async_session_factory()() as db:
        result = await db.execute(
            select(MindBodyMembership.mindbody_client_id)
            .where(MindBodyMembership.mindbody_client_id.in_(client_ids))
            .where(MindBodyMembership.is_active.is_(True))
            .distinct()
        )
        return {row[0] for row in result.fetchall()}
```

- [ ] **Step 4: Run tests to confirm they pass**

```
pytest tests/unit/sync/test_db_tasks.py -v
```
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/unit/sync/test_db_tasks.py app/sync/tasks.py
git commit -m "feat: add load_all_client_ids_from_db, load_active_clients_from_db, get_active_member_ids_from_db tasks"
```

---

## Chunk 2: Clients sync flow

### Task 2: Create mindbody_clients.py flow

**Files:**
- Create: `app/sync/flows/mindbody_clients.py`
- Test: `tests/unit/sync/test_mindbody_clients_flow.py`

This replaces `mindbody_users.py`. The new flow is named `sync-mindbody-clients`, supports incremental fetch via `last_clients_sync_at` Prefect variable, and emits a `mindbody.clients.synced` event on completion.

- [ ] **Step 1: Write failing tests**

> **Note on test pattern:** `flow.__wrapped__` calls the raw coroutine bypassing Prefect instrumentation. Every `@task` called inside the flow **must** be monkeypatched at the module level or it will raise a Prefect context error. `Variable.aget` / `Variable.aset` must be patched (not `Variable.get` / `Variable.set`) because Prefect's async dispatch routes to the `aget`/`aset` variants when a running event loop is detected.

Create `tests/unit/sync/test_mindbody_clients_flow.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock
import pytest

import app.sync.flows.mindbody_clients as flow_mod


@pytest.mark.asyncio
async def test_clients_flow_incremental_uses_last_sync_at(monkeypatch):
    """Flow reads last_clients_sync_at and passes it as modified_after."""
    captured = {}

    async def fake_variable_aget(name, default=None):
        if name == "last_clients_sync_at":
            return "2026-03-01T00:00:00+00:00"
        return default

    async def fake_fetch_members(modified_after=None):
        captured["modified_after"] = modified_after
        return [{"Id": "1", "FirstName": "A", "LastName": "B"}]

    async def fake_upsert(members):
        return len(members)

    async def fake_variable_aset(name, value, overwrite=False):
        captured[f"set_{name}"] = value

    monkeypatch.setattr(flow_mod.Variable, "aget", staticmethod(fake_variable_aget))
    monkeypatch.setattr(flow_mod.Variable, "aset", staticmethod(fake_variable_aset))
    monkeypatch.setattr(flow_mod, "fetch_members", fake_fetch_members)
    monkeypatch.setattr(flow_mod, "upsert_mindbody_users_batch", fake_upsert)

    with patch("app.sync.flows.mindbody_clients.emit_event"):
        with patch("app.sync.flows.mindbody_clients.create_markdown_artifact", new_callable=AsyncMock):
            with patch("app.sync.flows.mindbody_clients.get_run_logger", return_value=MagicMock()):
                count = await flow_mod.sync_mindbody_clients_flow.__wrapped__()

    assert captured["modified_after"] is not None
    assert "set_last_clients_sync_at" in captured
    assert count == 1


@pytest.mark.asyncio
async def test_clients_flow_no_members_returns_zero(monkeypatch):
    async def fake_variable_aget(name, default=None):
        return default

    async def fake_fetch_members(modified_after=None):
        return []

    async def fake_variable_aset(name, value, overwrite=False):
        pass

    monkeypatch.setattr(flow_mod.Variable, "aget", staticmethod(fake_variable_aget))
    monkeypatch.setattr(flow_mod.Variable, "aset", staticmethod(fake_variable_aset))
    monkeypatch.setattr(flow_mod, "fetch_members", fake_fetch_members)

    with patch("app.sync.flows.mindbody_clients.emit_event"):
        with patch("app.sync.flows.mindbody_clients.get_run_logger", return_value=MagicMock()):
            count = await flow_mod.sync_mindbody_clients_flow.__wrapped__()

    assert count == 0
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest tests/unit/sync/test_mindbody_clients_flow.py -v
```
Expected: FAIL — module `app.sync.flows.mindbody_clients` not found.

- [ ] **Step 3: Create app/sync/flows/mindbody_clients.py**

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event
from prefect.variables import Variable

from app.sync.tasks import fetch_members, upsert_mindbody_users_batch

logger = logging.getLogger(__name__)


@flow(name="sync-mindbody-clients", log_prints=True)
async def sync_mindbody_clients_flow() -> int:
    """
    Incrementally fetch MindBody client details and upsert into the mindbody_clients table.
    Uses last_clients_sync_at Prefect variable to track the fetch window.
    Emits 'mindbody.clients.synced' event on completion.
    Returns count of rows upserted.
    """
    flow_logger = get_run_logger()

    last_sync_str = await Variable.aget("last_clients_sync_at", default=None)
    modified_after: datetime | None = None
    if last_sync_str:
        try:
            modified_after = datetime.fromisoformat(last_sync_str)
        except ValueError:
            flow_logger.warning("Invalid last_clients_sync_at: %s, doing full fetch", last_sync_str)

    run_started_at = datetime.now(timezone.utc)
    scope = "incremental" if modified_after else "full"
    flow_logger.info("MindBody client sync started (scope=%s, modified_after=%s)", scope, modified_after)

    members = await fetch_members(modified_after=modified_after)
    flow_logger.info("Fetched %d members from MindBody", len(members))

    if not members:
        flow_logger.info("No members to upsert")
        await Variable.aset("last_clients_sync_at", run_started_at.isoformat(), overwrite=True)
        emit_event(
            event="mindbody.clients.synced",
            resource={"prefect.resource.id": "mindbody.clients.sync"},
            payload={"count": 0, "scope": scope},
        )
        return 0

    count = await upsert_mindbody_users_batch(members)
    flow_logger.info("Upserted %d client records into mindbody_clients", count)

    await Variable.aset("last_clients_sync_at", run_started_at.isoformat(), overwrite=True)

    emit_event(
        event="mindbody.clients.synced",
        resource={"prefect.resource.id": "mindbody.clients.sync"},
        payload={"count": count, "scope": scope},
    )

    timestamp = run_started_at.strftime("%Y-%m-%d %H:%M")
    await create_markdown_artifact(
        key="mindbody-client-sync",
        markdown=f"## MindBody Client Sync — {timestamp} UTC\n- Scope: {scope}\n- Upserted: {count} clients",
    )

    return count
```

- [ ] **Step 4: Run tests to confirm they pass**

```
pytest tests/unit/sync/test_mindbody_clients_flow.py -v
```
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add app/sync/flows/mindbody_clients.py tests/unit/sync/test_mindbody_clients_flow.py
git commit -m "feat: add sync-mindbody-clients flow with incremental fetch and event emission"
```

---

## Chunk 3: Memberships sync flow

### Task 3: Update mindbody_memberships.py

**Files:**
- Modify: `app/sync/flows/mindbody_memberships.py`
- Test: `tests/unit/sync/test_mindbody_memberships_flow.py`

Replace `fetch_members()` (MindBody API call) with `load_all_client_ids_from_db()`. Emit `mindbody.memberships.synced` event on completion.

- [ ] **Step 1: Write failing tests**

Create `tests/unit/sync/test_mindbody_memberships_flow.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock
import pytest

import app.sync.flows.mindbody_memberships as flow_mod


@pytest.mark.asyncio
async def test_memberships_flow_loads_ids_from_db(monkeypatch):
    """Flow uses local DB client IDs, not MindBody API."""
    captured = {}

    async def fake_load_ids():
        return ["101", "202"]

    async def fake_fetch_all(client_ids):
        captured["client_ids"] = client_ids
        return {"101": [], "202": []}

    async def fake_upsert(memberships_by_client):
        return 0

    monkeypatch.setattr(flow_mod, "load_all_client_ids_from_db", fake_load_ids)
    monkeypatch.setattr(flow_mod, "fetch_all_memberships", fake_fetch_all)
    monkeypatch.setattr(flow_mod, "upsert_mindbody_memberships_batch", fake_upsert)

    with patch("app.sync.flows.mindbody_memberships.emit_event"):
        with patch("app.sync.flows.mindbody_memberships.create_markdown_artifact", new_callable=AsyncMock):
            with patch("app.sync.flows.mindbody_memberships.get_run_logger", return_value=MagicMock()):
                total = await flow_mod.sync_mindbody_memberships_flow.__wrapped__()

    assert captured["client_ids"] == ["101", "202"]
    assert total == 0


@pytest.mark.asyncio
async def test_memberships_flow_no_clients_returns_zero(monkeypatch):
    async def fake_load_ids():
        return []

    monkeypatch.setattr(flow_mod, "load_all_client_ids_from_db", fake_load_ids)

    with patch("app.sync.flows.mindbody_memberships.emit_event"):
        with patch("app.sync.flows.mindbody_memberships.get_run_logger", return_value=MagicMock()):
            total = await flow_mod.sync_mindbody_memberships_flow.__wrapped__()

    assert total == 0
```

- [ ] **Step 2: Run tests to confirm they fail**

```
pytest tests/unit/sync/test_mindbody_memberships_flow.py -v
```
Expected: FAIL — `load_all_client_ids_from_db` not imported, `emit_event` not used.

- [ ] **Step 3: Rewrite app/sync/flows/mindbody_memberships.py**

```python
from __future__ import annotations

import logging
from datetime import datetime, timezone

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.events import emit_event

from app.sync.tasks import (
    fetch_all_memberships,
    load_all_client_ids_from_db,
    upsert_mindbody_memberships_batch,
)

logger = logging.getLogger(__name__)


@flow(name="sync-mindbody-memberships", log_prints=True)
async def sync_mindbody_memberships_flow() -> int:
    """
    Fetch active memberships for all clients in the local DB and upsert into the
    mindbody_memberships table. Client IDs are loaded from mindbody_clients, not MindBody API.
    Emits 'mindbody.memberships.synced' event on completion.
    Returns total membership rows upserted.
    """
    flow_logger = get_run_logger()
    flow_logger.info("MindBody membership sync started")

    client_ids = await load_all_client_ids_from_db()
    flow_logger.info("Loaded %d client IDs from local DB", len(client_ids))

    if not client_ids:
        flow_logger.info("No client IDs in local DB — skipping membership fetch")
        emit_event(
            event="mindbody.memberships.synced",
            resource={"prefect.resource.id": "mindbody.memberships.sync"},
            payload={"count": 0},
        )
        return 0

    memberships_by_client = await fetch_all_memberships(client_ids)

    total = await upsert_mindbody_memberships_batch(memberships_by_client)
    flow_logger.info("Upserted %d membership rows into mindbody_memberships", total)

    emit_event(
        event="mindbody.memberships.synced",
        resource={"prefect.resource.id": "mindbody.memberships.sync"},
        payload={"count": total},
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    await create_markdown_artifact(
        key="mindbody-membership-sync",
        markdown=(
            f"## MindBody Membership Sync — {timestamp} UTC\n"
            f"- Clients processed: {len(client_ids)}\n"
            f"- Membership rows upserted: {total}"
        ),
    )

    return total
```

- [ ] **Step 4: Run tests to confirm they pass**

```
pytest tests/unit/sync/test_mindbody_memberships_flow.py -v
```
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add app/sync/flows/mindbody_memberships.py tests/unit/sync/test_mindbody_memberships_flow.py
git commit -m "feat: update memberships flow to load client IDs from DB and emit sync event"
```

---

## Chunk 4: Integration flow — DB-only reads

### Task 4: Strip MindBody API calls from integration.py

**Files:**
- Modify: `app/sync/flows/integration.py`
- Modify: `tests/unit/sync/test_integration_plan_group.py` (no change needed — `_plan_group` signature unchanged)

Replace `fetch_members()` + `get_active_member_ids()` with their DB equivalents. Remove the `upsert_*` calls entirely.

- [ ] **Step 1: Write a new integration flow test**

Add to `tests/unit/sync/test_integration_plan_group.py` (append, don't replace existing tests):

```python
@pytest.mark.asyncio
async def test_integration_flow_does_not_import_fetch_members():
    """Ensure integration flow no longer depends on MindBody API fetch tasks."""
    import app.sync.flows.integration as intmod
    assert not hasattr(intmod, "fetch_members"), (
        "fetch_members should not be imported in integration flow"
    )
    assert not hasattr(intmod, "get_active_member_ids"), (
        "get_active_member_ids (API version) should not be imported in integration flow"
    )
```

- [ ] **Step 2: Run test to confirm it fails**

```
pytest tests/unit/sync/test_integration_plan_group.py::test_integration_flow_does_not_import_fetch_members -v
```
Expected: FAIL

- [ ] **Step 3: Rewrite integration.py**

Replace the imports and steps 2–4 of the flow. Full new file:

```python
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable

from app.sync.flows.dahua_push import run_dahua_push
from app.sync.tasks import (
    _format_dahua_date,
    get_active_member_ids_from_db,
    load_active_clients_from_db,
    load_device_ids_by_gate_type,
    load_enrollments_for_device,
    load_membership_windows,
    write_sync_queue_batch,
)

logger = logging.getLogger(__name__)


@flow(name="sync-integration", log_prints=True)
async def sync_integration_flow(
    sync_type: str = "scheduled",
) -> None:
    """
    Main integration flow:
    1. Load all active MindBody clients from local DB
    2. Classify by gender, load devices, check active memberships (from local DB)
    3. Compute enroll/deactivate/reactivate/update_window operations → write to dahua_sync_queue
    4. Execute operations against Dahua devices (subflow)
    """
    flow_logger = get_run_logger()
    photo_max_kb = int(await Variable.aget("photo_max_size_kb", default="200"))

    run_started_at = datetime.now(timezone.utc)
    flow_logger.info("Integration sync started")

    # 1. Load all active members from local DB (no MindBody API call)
    members = await load_active_clients_from_db()
    flow_logger.info("Loaded %d active members from local DB", len(members))

    if not members:
        flow_logger.info("No active members in local DB")
        return

    # 2. Derive client IDs
    client_ids = [str(m["Id"]) for m in members if m.get("Id")]

    # 3. Classify by gender + load devices + check active memberships in parallel
    male_members = [m for m in members if (m.get("Gender") or "").lower() == "male"]
    female_members = [m for m in members if (m.get("Gender") or "").lower() == "female"]
    flow_logger.info("Classified: %d male, %d female", len(male_members), len(female_members))

    (male_device_ids, female_device_ids), active_ids = await asyncio.gather(
        asyncio.gather(
            load_device_ids_by_gate_type("male"),
            load_device_ids_by_gate_type("female"),
        ),
        get_active_member_ids_from_db(client_ids),
    )
    flow_logger.info(
        "Devices: %d male gates, %d female gates | %d / %d members active",
        len(male_device_ids), len(female_device_ids), len(active_ids), len(client_ids),
    )

    # 4. Plan operations for both groups in parallel
    run_id = str(uuid4())
    male_items, female_items = await asyncio.gather(
        _plan_group(male_members, male_device_ids, active_ids, flow_logger),
        _plan_group(female_members, female_device_ids, active_ids, flow_logger),
    )
    all_items = male_items + female_items

    flow_logger.info(
        "Planned %d operations (run_id=%s): %d enroll, %d deactivate, %d reactivate",
        len(all_items),
        run_id,
        sum(1 for i in all_items if i["action"] == "enroll"),
        sum(1 for i in all_items if i["action"] == "deactivate"),
        sum(1 for i in all_items if i["action"] == "reactivate"),
    )

    # 5. Persist the plan to dahua_sync_queue
    await write_sync_queue_batch(run_id, all_items)

    # 6. Execute the plan against Dahua devices
    push_stats = await run_dahua_push(run_id, photo_max_kb, flow_logger)

    # 7. Publish artifact
    await create_table_artifact(
        key="sync-results",
        table=[
            {"metric": "enrolled", "count": push_stats.get("enrolled", 0)},
            {"metric": "deactivated", "count": push_stats.get("deactivated", 0)},
            {"metric": "reactivated", "count": push_stats.get("reactivated", 0)},
            {"metric": "failed", "count": push_stats.get("failed", 0)},
        ],
        description=(
            f"## Full Sync — "
            f"{run_started_at.strftime('%Y-%m-%d %H:%M')} UTC  \n"
            f"run_id: `{run_id}`"
        ),
    )

    flow_logger.info("Sync complete — %s", push_stats)


async def _plan_group(
    group_members: list[dict],
    device_ids: list[int],
    active_ids: set[str],
    flow_logger,
) -> list[dict]:
    """
    Compute the set of enroll/deactivate/reactivate/update_window operations needed
    for a gender group. Returns a list of queue item dicts — does NOT execute any
    Dahua operations.
    """
    if not device_ids:
        return []

    active_group_ids = {str(m.get("Id", "")) for m in group_members if str(m.get("Id", "")) in active_ids}
    member_map = {str(m.get("Id", "")): m for m in group_members}

    all_client_ids = list({str(m.get("Id", "")) for m in group_members if m.get("Id")})
    enrollment_results: list = await asyncio.gather(
        load_membership_windows(all_client_ids),
        *[load_enrollments_for_device(device_id) for device_id in device_ids],
    )
    membership_windows = enrollment_results[0]
    device_enrollment_map = {
        device_id: enrollment_results[i + 1]
        for i, device_id in enumerate(device_ids)
    }

    items: list[dict] = []

    for device_id in device_ids:
        device_enrollments = device_enrollment_map[device_id]

        to_enroll = [member_map[cid] for cid in active_group_ids if cid not in device_enrollments]
        to_deactivate = [
            e for cid, e in device_enrollments.items() if cid not in active_ids and e.is_active
        ]
        to_reactivate = [
            e for cid, e in device_enrollments.items()
            if cid in active_group_ids and not e.is_active
        ]

        to_update_window = []
        for cid, enrollment in device_enrollments.items():
            if cid not in active_group_ids or not enrollment.is_active:
                continue
            _, expiration_date = membership_windows.get(cid, (None, None))
            new_valid_end = _format_dahua_date(expiration_date)
            if enrollment.valid_end != new_valid_end:
                to_update_window.append((cid, enrollment))

        flow_logger.info(
            "Device %d: enroll=%d deactivate=%d reactivate=%d update_window=%d",
            device_id, len(to_enroll), len(to_deactivate), len(to_reactivate), len(to_update_window),
        )

        for m in to_enroll:
            cid = str(m.get("Id", ""))
            start_date, expiration_date = membership_windows.get(cid, (None, None))
            snapshot = json.dumps({
                "Id": m.get("Id"),
                "FirstName": m.get("FirstName", ""),
                "LastName": m.get("LastName", ""),
                "Gender": m.get("Gender"),
                "PhotoUrl": m.get("PhotoUrl"),
                "Email": m.get("Email"),
                "valid_start": _format_dahua_date(start_date),
                "valid_end": _format_dahua_date(expiration_date),
            })
            items.append({
                "device_id": device_id,
                "mindbody_client_id": cid,
                "action": "enroll",
                "member_snapshot": snapshot,
                "dahua_user_id": None,
                "enrollment_id": None,
            })

        # Note: mindbody_client_id uses e.dahua_user_id intentionally — dahua_user_id is
        # derived from mindbody_client_id via _make_dahua_user_id() which produces the same
        # string for integer client IDs. This preserves the existing behavior.
        for e in to_deactivate:
            items.append({
                "device_id": device_id,
                "mindbody_client_id": e.dahua_user_id,
                "action": "deactivate",
                "member_snapshot": None,
                "dahua_user_id": e.dahua_user_id,
                "enrollment_id": e.id,
            })

        for e in to_reactivate:
            items.append({
                "device_id": device_id,
                "mindbody_client_id": e.dahua_user_id,
                "action": "reactivate",
                "member_snapshot": None,
                "dahua_user_id": e.dahua_user_id,
                "enrollment_id": e.id,
            })

        for cid, enrollment in to_update_window:
            start_date, expiration_date = membership_windows.get(cid, (None, None))
            items.append({
                "device_id": device_id,
                "mindbody_client_id": cid,
                "action": "update_window",
                "member_snapshot": json.dumps({
                    "valid_start": _format_dahua_date(start_date),
                    "valid_end": _format_dahua_date(expiration_date),
                }),
                "dahua_user_id": enrollment.dahua_user_id,
                "enrollment_id": enrollment.id,
            })

    return items
```

- [ ] **Step 4: Run all integration tests**

```
pytest tests/unit/sync/test_integration_plan_group.py -v
```
Expected: PASS (3 tests — 2 existing + 1 new)

- [ ] **Step 5: Commit**

```bash
git add app/sync/flows/integration.py tests/unit/sync/test_integration_plan_group.py
git commit -m "feat: integration flow reads from local DB instead of MindBody API"
```

---

## Chunk 5: Worker deployments and Prefect automation

### Task 5: Update worker.py with new deployments and compound automation

**Files:**
- Modify: `app/sync/worker.py`

Register `sync-mindbody-clients` and `sync-mindbody-memberships` on the interval schedule. Remove the interval schedule from `sync-integration`. Create a Prefect automation that fires integration when both sync events are received within the same interval window.

- [ ] **Step 1: Update worker.py**

Full new `main()` and updated `_setup()` and new `_ensure_integration_automation()`:

Replace the entire `worker.py` with:

```python
"""
Prefect worker entry point.

Start with:
    python -m app.sync.worker

On startup:
 1. Registers MindBodyCredentials block type with Prefect server
 2. Creates per-device concurrency limits (max 2 concurrent calls per device)
 3. Starts prefect serve() with 4 deployments:
    - sync-mindbody-clients/scheduled   (every N minutes, incremental client fetch)
    - sync-mindbody-memberships/scheduled (every N minutes, full membership refresh)
    - sync-integration/full             (no schedule — triggered by Prefect automation)
    - sync-member/default               (webhook/manual trigger)
    - device-health/scheduled           (every M minutes, health check)
 4. Registers a Prefect compound automation that fires sync-integration/full
    when both sync-mindbody-clients and sync-mindbody-memberships complete
    within the same interval window.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from prefect import aserve
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName, FlowFilter, FlowFilterName
from prefect.events.actions import RunDeployment
from prefect.events.schemas.automations import (
    Automation,
    CompoundTrigger,
    EventTrigger,
)
from prefect.variables import Variable

from app.models.database import init_async_db, init_db
from app.models.device import DahuaDevice
from app.sync.blocks import MindBodyCredentials
from app.sync.flows.health import device_health_flow
from app.sync.flows.integration import sync_integration_flow
from app.sync.flows.member import sync_member_flow
from app.sync.flows.mindbody_clients import sync_mindbody_clients_flow
from app.sync.flows.mindbody_memberships import sync_mindbody_memberships_flow

logger = logging.getLogger(__name__)

_AUTOMATION_NAME = "trigger-integration-after-data-refresh"


async def ensure_concurrency_limits(device_ids: list[int]) -> None:
    """Create per-device Prefect concurrency limits (idempotent)."""
    async with get_client() as client:
        for device_id in device_ids:
            tag = f"dahua-device-{device_id}"
            try:
                await client.create_concurrency_limit(
                    tag=tag,
                    concurrency_limit=2,
                )
                logger.info("Created concurrency limit: %s (max 2)", tag)
            except Exception:
                pass


async def _ensure_integration_automation(interval_minutes: int) -> None:
    """
    Create (or update) a compound Prefect automation that fires sync-integration/full
    when both mindbody.clients.synced AND mindbody.memberships.synced events are
    received within the same interval window.

    Idempotent: if an automation with the same name already exists it is deleted
    and recreated so schedule changes are always applied.
    """
    within_seconds = interval_minutes * 60 + 60  # interval + 1 min grace period

    async with get_client() as client:
        # Resolve the integration deployment UUID — it must already exist in Prefect server.
        # On the very first worker startup this deployment won't exist yet (aserve hasn't run).
        # The automation will be created on the next startup after the first serve registers it.
        try:
            deployments = await client.read_deployments(
                deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=["full"])),
                flow_filter=FlowFilter(name=FlowFilterName(any_=["sync-integration"])),
            )
        except Exception:
            logger.warning("Could not look up sync-integration/full deployment — skipping automation setup")
            return

        if not deployments:
            logger.warning(
                "sync-integration/full deployment not found in Prefect server yet. "
                "Automation will be created on next worker startup after deployments are registered."
            )
            return

        deployment_id = deployments[0].id
        logger.info("Found sync-integration/full deployment: %s", deployment_id)

        automation = Automation(
            name=_AUTOMATION_NAME,
            description=(
                "Trigger sync-integration/full when both MindBody data sync flows "
                "complete within the same interval window."
            ),
            trigger=CompoundTrigger(
                require="all",
                within=timedelta(seconds=within_seconds),
                triggers=[
                    EventTrigger(
                        expect={"mindbody.clients.synced"},
                        match_related={},
                    ),
                    EventTrigger(
                        expect={"mindbody.memberships.synced"},
                        match_related={},
                    ),
                ],
            ),
            actions=[
                RunDeployment(
                    source="selected",
                    deployment_id=deployment_id,
                    parameters={"sync_type": "scheduled"},
                )
            ],
            enabled=True,
        )

        # Delete existing automation with same name (idempotent update)
        try:
            existing = await client.read_automations_by_name(name=_AUTOMATION_NAME)
            for auto in existing:
                await client.delete_automation(auto.id)
                logger.info("Deleted existing automation: %s (%s)", _AUTOMATION_NAME, auto.id)
        except Exception:
            pass

        try:
            created = await client.create_automation(automation)
            logger.info("Created automation: %s (%s)", _AUTOMATION_NAME, created.id)
        except Exception:
            logger.warning("Could not create integration automation (Prefect server may not be ready)")


async def _sync_variables_from_env() -> None:
    """Set Prefect Variables from env vars when provided, otherwise leave existing values."""
    mapping = {
        "SYNC_INTERVAL_MINUTES": "sync_interval_minutes",
        "HEALTH_INTERVAL_MINUTES": "health_interval_minutes",
        "PHOTO_MAX_SIZE_KB": "photo_max_size_kb",
    }
    for env_key, var_name in mapping.items():
        value = os.environ.get(env_key)
        if value is not None:
            await Variable.aset(var_name, value, overwrite=True)
            logger.info("Prefect variable '%s' set to %s (from env)", var_name, value)


async def _create_block_from_env() -> None:
    """Create or update the MindBodyCredentials block 'production' from env vars."""
    api_key = os.environ.get("MINDBODY_API_KEY")
    site_id = os.environ.get("MINDBODY_SITE_ID")
    username = os.environ.get("MINDBODY_USERNAME")
    password = os.environ.get("MINDBODY_PASSWORD")
    base_url = os.environ.get("MINDBODY_BASE_URL", "https://api.mindbodyonline.com/public/v6")

    if not all([api_key, site_id, username, password]):
        logger.info(
            "MINDBODY_API_KEY / MINDBODY_SITE_ID / MINDBODY_USERNAME / MINDBODY_PASSWORD "
            "not all set — skipping block auto-creation (create manually via Prefect UI)"
        )
        return

    block = MindBodyCredentials(
        api_key=api_key,       # type: ignore[arg-type]
        site_id=site_id,       # type: ignore[arg-type]
        username=username,     # type: ignore[arg-type]
        password=password,     # type: ignore[arg-type]
        base_url=base_url,
    )
    await block.save("production", overwrite=True)
    logger.info("MindBodyCredentials block 'production' created/updated from env vars")


async def _setup() -> tuple[int, int]:
    """Initialise DB, register block types, set up concurrency limits."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    database_url = os.environ.get("DATABASE_URL", "sqlite:///./data/sync.db")

    sync_factory = init_db(database_url)
    db = sync_factory()
    try:
        device_ids = [
            d.id
            for d in db.query(DahuaDevice)
            .filter(DahuaDevice.is_enabled.is_(True))
            .all()
        ]
    finally:
        db.close()

    init_async_db(database_url)

    try:
        await MindBodyCredentials.register_type_and_schema()
        logger.info("MindBodyCredentials block type registered")
    except Exception as e:
        logger.warning("Could not register block type (server may not be ready): %s", e)

    await _create_block_from_env()
    await ensure_concurrency_limits(device_ids)
    await _sync_variables_from_env()

    interval = int(await Variable.aget("sync_interval_minutes", default="30"))
    health_interval = int(await Variable.aget("health_interval_minutes", default="5"))
    logger.info("Sync interval: %d min | Health interval: %d min", interval, health_interval)

    await _ensure_integration_automation(interval)

    return interval, health_interval


async def main() -> None:
    interval, health_interval = await _setup()

    await aserve(
        # Webhook/manual trigger: single member sync
        await sync_member_flow.ato_deployment(
            name="default",
            tags=["webhook"],
        ),

        # MindBody client sync — incremental, every N minutes
        await sync_mindbody_clients_flow.ato_deployment(
            name="scheduled",
            interval=timedelta(minutes=interval),
            tags=["mindbody", "clients"],
        ),

        # MindBody membership sync — every N minutes
        await sync_mindbody_memberships_flow.ato_deployment(
            name="scheduled",
            interval=timedelta(minutes=interval),
            tags=["mindbody", "memberships"],
        ),

        # Integration — no schedule, triggered by Prefect automation
        await sync_integration_flow.ato_deployment(
            name="full",
            parameters={"sync_type": "scheduled"},
            tags=["integration", "full"],
        ),

        # Scheduled health check
        await device_health_flow.ato_deployment(
            name="scheduled",
            interval=timedelta(minutes=health_interval),
            tags=["health"],
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Run the full test suite to confirm nothing is broken**

```
pytest tests/ -v --tb=short
```
Expected: all previously passing tests still pass.

- [ ] **Step 3: Commit**

```bash
git add app/sync/worker.py
git commit -m "feat: add clients/memberships deployments and Prefect compound automation for integration trigger"
```

---

## Chunk 6: Cleanup

### Task 6: Delete the now-unused mindbody_users.py

**Files:**
- Delete: `app/sync/flows/mindbody_users.py`

- [ ] **Step 1: Confirm nothing imports mindbody_users**

```bash
grep -r "mindbody_users" app/ tests/
```
Expected: no output.

- [ ] **Step 2: Delete the file**

```bash
git rm app/sync/flows/mindbody_users.py
```

- [ ] **Step 3: Run full test suite**

```
pytest tests/ -v --tb=short
```
Expected: all tests pass.

- [ ] **Step 4: Final commit**

```bash
git commit -m "chore: remove mindbody_users.py — superseded by mindbody_clients.py"
```
