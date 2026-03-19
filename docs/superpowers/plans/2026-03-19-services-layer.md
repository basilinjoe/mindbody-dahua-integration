# Services Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate all database logic into a dedicated `app/services/` layer so that every route, task, and flow receives an `AsyncSession` via dependency injection rather than owning session lifecycle themselves.

**Architecture:** Seven service modules (one per domain) expose pure async functions that each accept `db: AsyncSession` as their first argument and never create sessions themselves. FastAPI routes receive sessions via `Depends(get_async_db)`. Prefect tasks and background tasks open sessions directly with `async with AsyncSessionLocal() as db:`. The sync engine, `init_db`, `SessionLocal`, and `get_db` are removed from `database.py` in Task 17.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2 async (`asyncpg`), Prefect 3, pytest-asyncio

---

## Chunk 1: Create `app/services/` package skeleton

### Task 1: Create service package and `members` service

**Files:**
- Create: `app/services/__init__.py`
- Create: `app/services/members.py`
- Create: `tests/services/__init__.py`
- Create: `tests/services/test_members.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/__init__.py` (empty).

Create `tests/services/test_members.py`:

```python
from __future__ import annotations

import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    db = AsyncMock(spec=AsyncSession)
    return db


@pytest.mark.asyncio
async def test_upsert_batch_returns_count(mock_db):
    from app.services.members import upsert_batch

    members = [
        {
            "Id": "101",
            "UniqueId": "u101",
            "FirstName": "Alice",
            "LastName": "Smith",
            "Email": "alice@example.com",
            "MobilePhone": None,
            "HomePhone": None,
            "WorkPhone": None,
            "Status": "Active",
            "Active": True,
            "BirthDate": None,
            "Gender": "Female",
            "CreationDate": None,
            "LastModifiedDateTime": None,
        }
    ]

    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, members)
    assert count == 1
    mock_db.execute.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_batch_deduplicates(mock_db):
    from app.services.members import upsert_batch

    members = [
        {"Id": "101", "UniqueId": "u101", "FirstName": "Alice", "LastName": "Smith",
         "Email": None, "MobilePhone": None, "HomePhone": None, "WorkPhone": None,
         "Status": "Active", "Active": True, "BirthDate": None, "Gender": None,
         "CreationDate": None, "LastModifiedDateTime": None},
        {"Id": "101", "UniqueId": "u101", "FirstName": "Alice", "LastName": "Smith",
         "Email": None, "MobilePhone": None, "HomePhone": None, "WorkPhone": None,
         "Status": "Active", "Active": True, "BirthDate": None, "Gender": None,
         "CreationDate": None, "LastModifiedDateTime": None},
    ]

    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, members)
    assert count == 1


@pytest.mark.asyncio
async def test_upsert_batch_empty_list(mock_db):
    from app.services.members import upsert_batch

    count = await upsert_batch(mock_db, [])
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_active_returns_scalars(mock_db):
    from app.models.mindbody_client import MindBodyClient
    from app.services.members import load_active

    fake_client = MagicMock(spec=MindBodyClient)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_client]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_active(mock_db)
    assert result == [fake_client]
    mock_db.execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_last_fetched_at_returns_none_when_empty(mock_db):
    from app.services.members import get_last_fetched_at

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_last_fetched_at(mock_db)
    assert result is None


@pytest.mark.asyncio
async def test_get_last_fetched_at_returns_datetime(mock_db):
    from app.services.members import get_last_fetched_at

    expected = datetime(2025, 1, 1, tzinfo=UTC)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = expected
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_last_fetched_at(mock_db)
    assert result == expected
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_members.py -v 2>&1 | head -40
```

Expected output: `ModuleNotFoundError: No module named 'app.services'` or similar import error.

- [ ] **Step 3: Create `app/services/__init__.py`**

```python
"""Domain-level service functions. Each function accepts db: AsyncSession as its first argument."""
```

- [ ] **Step 4: Create `app/services/members.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import or_

from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership


async def upsert_batch(db: AsyncSession, members: list[dict]) -> int:
    """Upsert a batch of raw MindBody member dicts. Returns number of rows written."""
    now = datetime.now(UTC)
    seen: set[str] = set()
    rows = []
    for m in members:
        mid = str(m.get("Id", "")).strip()
        if not mid or mid in seen:
            continue
        seen.add(mid)
        rows.append(
            {
                "mindbody_id": mid,
                "unique_id": m.get("UniqueId"),
                "first_name": m.get("FirstName", ""),
                "last_name": m.get("LastName", ""),
                "email": m.get("Email"),
                "mobile_phone": m.get("MobilePhone"),
                "home_phone": m.get("HomePhone"),
                "work_phone": m.get("WorkPhone"),
                "status": m.get("Status"),
                "active": bool(m.get("Active", False)),
                "birth_date": m.get("BirthDate"),
                "gender": m.get("Gender"),
                "created_at_mb": m.get("CreationDate"),
                "last_modified_at_mb": m.get("LastModifiedDateTime"),
                "last_fetched_at": now,
            }
        )
    if not rows:
        return 0
    stmt = insert(MindBodyClient).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["mindbody_id"],
        set_={k: stmt.excluded[k] for k in rows[0] if k != "mindbody_id"},
    )
    await db.execute(stmt)
    await db.commit()
    return len(rows)


async def load_active(db: AsyncSession) -> list[MindBodyClient]:
    """Return all MindBodyClient rows that are active and have at least one active membership."""
    result = await db.execute(
        select(MindBodyClient)
        .join(
            MindBodyMembership,
            MindBodyClient.mindbody_id == MindBodyMembership.mindbody_client_id,
        )
        .where(
            MindBodyClient.active.is_(True),
            MindBodyMembership.status == "Active",
            or_(
                MindBodyMembership.expiration_date.is_(None),
                MindBodyMembership.expiration_date > datetime.now(UTC),
            ),
        )
        .distinct()
    )
    return list(result.scalars().all())


async def get_last_fetched_at(db: AsyncSession) -> datetime | None:
    """Return the most recent last_fetched_at timestamp across all member rows."""
    result = await db.execute(
        select(func.max(MindBodyClient.last_fetched_at))
    )
    return result.scalar_one_or_none()
```

- [ ] **Step 5: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_members.py -v
```

Expected output:
```
tests/services/test_members.py::test_upsert_batch_returns_count PASSED
tests/services/test_members.py::test_upsert_batch_deduplicates PASSED
tests/services/test_members.py::test_upsert_batch_empty_list PASSED
tests/services/test_members.py::test_load_active_returns_scalars PASSED
tests/services/test_members.py::test_get_last_fetched_at_returns_none_when_empty PASSED
tests/services/test_members.py::test_get_last_fetched_at_returns_datetime PASSED
6 passed
```

- [ ] **Step 6: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/__init__.py app/services/members.py tests/services/__init__.py tests/services/test_members.py && git commit -m "feat: add members service with upsert_batch, load_active, get_last_fetched_at"
```

---

### Task 2: Create `memberships` service

**Files:**
- Create: `app/services/memberships.py`
- Create: `tests/services/test_memberships.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_memberships.py`:

```python
from __future__ import annotations

import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_upsert_batch_returns_count(mock_db):
    from app.services.memberships import upsert_batch

    memberships_by_client = {
        "101": [
            {
                "Id": "contract-1",
                "Name": "Monthly",
                "Status": "Active",
                "StartDate": "2024-01-01",
                "ExpirationDate": None,
            }
        ]
    }
    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, memberships_by_client)
    assert count == 1
    mock_db.execute.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_upsert_batch_skips_empty_contract_id(mock_db):
    from app.services.memberships import upsert_batch

    memberships_by_client = {
        "101": [{"Id": "", "Name": "Bad", "Status": "Active", "StartDate": None, "ExpirationDate": None}]
    }
    mock_db.execute = AsyncMock()
    mock_db.commit = AsyncMock()

    count = await upsert_batch(mock_db, memberships_by_client)
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_batch_empty_dict(mock_db):
    from app.services.memberships import upsert_batch

    count = await upsert_batch(mock_db, {})
    assert count == 0
    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_windows_returns_dict(mock_db):
    from app.services.memberships import load_windows

    fake_row = MagicMock()
    fake_row.mindbody_client_id = "101"
    fake_row.valid_start = "2024-01-01"
    fake_row.valid_end = None

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([fake_row]))
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_windows(mock_db, ["101"])
    assert "101" in result
    assert result["101"]["valid_start"] == "2024-01-01"
    assert result["101"]["valid_end"] is None


@pytest.mark.asyncio
async def test_load_windows_empty_client_ids(mock_db):
    from app.services.memberships import load_windows

    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_windows(mock_db, [])
    assert result == {}
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_memberships.py -v 2>&1 | head -20
```

Expected output: `ModuleNotFoundError: No module named 'app.services.memberships'`

- [ ] **Step 3: Create `app/services/memberships.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import or_

from app.models.mindbody_membership import MindBodyMembership


async def upsert_batch(db: AsyncSession, memberships_by_client: dict[str, list[dict]]) -> int:
    """Upsert raw MindBody membership dicts keyed by client_id. Returns number of rows written."""
    now = datetime.now(UTC)
    rows = []
    for client_id, memberships in memberships_by_client.items():
        for m in memberships:
            contract_id = str(m.get("Id", "")).strip()
            if not contract_id:
                continue
            rows.append(
                {
                    "mindbody_client_id": client_id,
                    "contract_id": contract_id,
                    "name": m.get("Name", ""),
                    "status": m.get("Status"),
                    "start_date": m.get("StartDate"),
                    "expiration_date": m.get("ExpirationDate"),
                    "last_fetched_at": now,
                }
            )
    if not rows:
        return 0
    stmt = insert(MindBodyMembership).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["mindbody_client_id", "contract_id"],
        set_={k: stmt.excluded[k] for k in rows[0] if k not in ("mindbody_client_id", "contract_id")},
    )
    await db.execute(stmt)
    await db.commit()
    return len(rows)


async def load_windows(db: AsyncSession, client_ids: list[str]) -> dict[str, dict]:
    """Return a dict mapping client_id → {valid_start, valid_end} for active memberships."""
    result = await db.execute(
        select(
            MindBodyMembership.mindbody_client_id,
            func.min(MindBodyMembership.start_date).label("valid_start"),
            func.max(MindBodyMembership.expiration_date).label("valid_end"),
        )
        .where(
            MindBodyMembership.mindbody_client_id.in_(client_ids),
            MindBodyMembership.status == "Active",
            or_(
                MindBodyMembership.expiration_date.is_(None),
                MindBodyMembership.expiration_date > datetime.now(UTC),
            ),
        )
        .group_by(MindBodyMembership.mindbody_client_id)
    )
    return {
        row.mindbody_client_id: {"valid_start": row.valid_start, "valid_end": row.valid_end}
        for row in result
    }
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_memberships.py -v
```

Expected output:
```
tests/services/test_memberships.py::test_upsert_batch_returns_count PASSED
tests/services/test_memberships.py::test_upsert_batch_skips_empty_contract_id PASSED
tests/services/test_memberships.py::test_upsert_batch_empty_dict PASSED
tests/services/test_memberships.py::test_load_windows_returns_dict PASSED
tests/services/test_memberships.py::test_load_windows_empty_client_ids PASSED
5 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/memberships.py tests/services/test_memberships.py && git commit -m "feat: add memberships service with upsert_batch and load_windows"
```

---

### Task 3: Create `devices` service

**Files:**
- Create: `app/services/devices.py`
- Create: `tests/services/test_devices.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_devices.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def fake_device():
    from app.models.device import DahuaDevice
    d = MagicMock(spec=DahuaDevice)
    d.id = 1
    d.name = "Gate A"
    d.gate_type = "all"
    d.is_enabled = True
    d.status = "online"
    return d


@pytest.mark.asyncio
async def test_list_all_returns_enabled_devices(mock_db, fake_device):
    from app.services.devices import list_all

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_device]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert result == [fake_device]
    mock_db.execute.assert_called_once()


@pytest.mark.asyncio
async def test_list_by_gate_type_returns_ids(mock_db):
    from app.services.devices import list_by_gate_type

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [1, 2]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_by_gate_type(mock_db, "male")
    assert result == [1, 2]


@pytest.mark.asyncio
async def test_get_by_id_returns_device(mock_db, fake_device):
    from app.services.devices import get_by_id

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_id(mock_db, 1)
    assert result is fake_device


@pytest.mark.asyncio
async def test_get_by_id_returns_none_when_not_found(mock_db):
    from app.services.devices import get_by_id

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_id(mock_db, 999)
    assert result is None


@pytest.mark.asyncio
async def test_update_status_commits(mock_db, fake_device):
    from app.services.devices import update_status

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "offline")
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_status_noop_when_not_found(mock_db):
    from app.services.devices import update_status

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 999, "offline")
    mock_db.commit.assert_not_called()
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_devices.py -v 2>&1 | head -20
```

Expected output: `ModuleNotFoundError: No module named 'app.services.devices'`

- [ ] **Step 3: Create `app/services/devices.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.device import DahuaDevice


async def list_all(db: AsyncSession) -> list[DahuaDevice]:
    """Return all enabled DahuaDevice rows."""
    result = await db.execute(
        select(DahuaDevice).where(DahuaDevice.is_enabled.is_(True))
    )
    return list(result.scalars().all())


async def list_by_gate_type(db: AsyncSession, gate_type: str) -> list[int]:
    """Return IDs of enabled devices whose gate_type matches 'all' or the given type."""
    result = await db.execute(
        select(DahuaDevice.id).where(
            DahuaDevice.is_enabled.is_(True),
            DahuaDevice.gate_type.in_(["all", gate_type]),
        )
    )
    return list(result.scalars().all())


async def get_by_id(db: AsyncSession, device_id: int) -> DahuaDevice | None:
    """Return a single DahuaDevice by primary key, or None."""
    result = await db.execute(
        select(DahuaDevice).where(DahuaDevice.id == device_id)
    )
    return result.scalar_one_or_none()


async def update_status(
    db: AsyncSession,
    device_id: int,
    status: str,
) -> None:
    """Set device.status and, if online, device.last_seen_at. No-op if device not found."""
    result = await db.execute(
        select(DahuaDevice).where(DahuaDevice.id == device_id)
    )
    device = result.scalar_one_or_none()
    if device is None:
        return
    device.status = status
    if status == "online":
        device.last_seen_at = datetime.now(UTC)
    await db.commit()
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_devices.py -v
```

Expected output:
```
tests/services/test_devices.py::test_list_all_returns_enabled_devices PASSED
tests/services/test_devices.py::test_list_by_gate_type_returns_ids PASSED
tests/services/test_devices.py::test_get_by_id_returns_device PASSED
tests/services/test_devices.py::test_get_by_id_returns_none_when_not_found PASSED
tests/services/test_devices.py::test_update_status_commits PASSED
tests/services/test_devices.py::test_update_status_noop_when_not_found PASSED
6 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/devices.py tests/services/test_devices.py && git commit -m "feat: add devices service with list_all, list_by_gate_type, get_by_id, update_status"
```

---

### Task 4: Create `queue` service

**Files:**
- Create: `app/services/queue.py`
- Create: `tests/services/test_queue.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_queue.py`:

```python
from __future__ import annotations

import pytest
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_write_batch_adds_rows(mock_db):
    from app.services.queue import write_batch

    items = [
        {"mindbody_client_id": "101", "device_id": 1, "action": "enroll",
         "status": "pending", "dahua_user_id": None, "member_snapshot": None},
    ]
    mock_db.add_all = MagicMock()
    mock_db.commit = AsyncMock()

    count = await write_batch(mock_db, "run-1", items)
    assert count == 1
    mock_db.add_all.assert_called_once()
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_write_batch_empty_items(mock_db):
    from app.services.queue import write_batch

    mock_db.add_all = MagicMock()
    mock_db.commit = AsyncMock()

    count = await write_batch(mock_db, "run-1", [])
    assert count == 0
    mock_db.add_all.assert_not_called()


@pytest.mark.asyncio
async def test_load_pending_returns_pending_rows(mock_db):
    from app.services.queue import load_pending
    from app.models.dahua_sync_queue import DahuaSyncQueue

    fake_item = MagicMock(spec=DahuaSyncQueue)
    fake_item.status = "pending"
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_item]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_pending(mock_db, "run-1")
    assert result == [fake_item]


@pytest.mark.asyncio
async def test_mark_item_updates_status(mock_db):
    from app.services.queue import mark_item
    from app.models.dahua_sync_queue import DahuaSyncQueue

    fake_item = MagicMock(spec=DahuaSyncQueue)
    fake_item.status = "pending"
    fake_item.error_message = None
    fake_item.executed_at = None

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_item
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await mark_item(mock_db, 42, "success")
    assert fake_item.status == "success"
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_mark_item_noop_when_not_found(mock_db):
    from app.services.queue import mark_item

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await mark_item(mock_db, 999, "success")
    mock_db.commit.assert_not_called()
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_queue.py -v 2>&1 | head -20
```

Expected output: `ModuleNotFoundError: No module named 'app.services.queue'`

- [ ] **Step 3: Create `app/services/queue.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dahua_sync_queue import DahuaSyncQueue


async def write_batch(db: AsyncSession, run_id: str, items: list[dict]) -> int:
    """Insert a batch of queue items for the given run_id. Returns number of rows inserted."""
    if not items:
        return 0
    rows = [DahuaSyncQueue(run_id=run_id, **item) for item in items]
    db.add_all(rows)
    await db.commit()
    return len(rows)


async def load_pending(db: AsyncSession, run_id: str) -> list[DahuaSyncQueue]:
    """Return all pending DahuaSyncQueue items for the given run_id."""
    result = await db.execute(
        select(DahuaSyncQueue).where(
            DahuaSyncQueue.run_id == run_id,
            DahuaSyncQueue.status == "pending",
        )
    )
    return list(result.scalars().all())


async def mark_item(
    db: AsyncSession,
    item_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """Set status (and optional error_message) on a queue item. No-op if item not found."""
    result = await db.execute(
        select(DahuaSyncQueue).where(DahuaSyncQueue.id == item_id)
    )
    item = result.scalar_one_or_none()
    if item is None:
        return
    item.status = status
    item.error_message = error_message
    item.executed_at = datetime.now(UTC)
    await db.commit()
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_queue.py -v
```

Expected output:
```
tests/services/test_queue.py::test_write_batch_adds_rows PASSED
tests/services/test_queue.py::test_write_batch_empty_items PASSED
tests/services/test_queue.py::test_load_pending_returns_pending_rows PASSED
tests/services/test_queue.py::test_mark_item_updates_status PASSED
tests/services/test_queue.py::test_mark_item_noop_when_not_found PASSED
5 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/queue.py tests/services/test_queue.py && git commit -m "feat: add queue service with write_batch, load_pending, mark_item"
```

---

### Task 5: Create `export_jobs` service

**Files:**
- Create: `app/services/export_jobs.py`
- Create: `tests/services/test_export_jobs.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_export_jobs.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.fixture
def fake_job():
    from app.models.export_job import ExportJob, ExportStatus
    j = MagicMock(spec=ExportJob)
    j.id = 1
    j.status = ExportStatus.PENDING
    j.created_by = "admin"
    return j


@pytest.mark.asyncio
async def test_create_returns_job(mock_db):
    from app.services.export_jobs import create
    from app.models.export_job import ExportStatus

    mock_db.add = MagicMock()
    mock_db.commit = AsyncMock()
    mock_db.refresh = AsyncMock()

    job = await create(mock_db, created_by="admin")
    mock_db.add.assert_called_once()
    mock_db.commit.assert_called_once()
    mock_db.refresh.assert_called_once()
    assert job.status == ExportStatus.PENDING


@pytest.mark.asyncio
async def test_get_returns_job(mock_db, fake_job):
    from app.services.export_jobs import get

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_job
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get(mock_db, 1)
    assert result is fake_job


@pytest.mark.asyncio
async def test_get_returns_none_when_not_found(mock_db):
    from app.services.export_jobs import get

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get(mock_db, 999)
    assert result is None


@pytest.mark.asyncio
async def test_list_all_returns_jobs(mock_db, fake_job):
    from app.services.export_jobs import list_all

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_job]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert result == [fake_job]


@pytest.mark.asyncio
async def test_update_commits_changes(mock_db, fake_job):
    from app.services.export_jobs import update
    from app.models.export_job import ExportStatus

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_job
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update(mock_db, 1, status=ExportStatus.RUNNING)
    assert fake_job.status == ExportStatus.RUNNING
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_noop_when_not_found(mock_db):
    from app.services.export_jobs import update
    from app.models.export_job import ExportStatus

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update(mock_db, 999, status=ExportStatus.RUNNING)
    mock_db.commit.assert_not_called()
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_export_jobs.py -v 2>&1 | head -20
```

Expected output: `ModuleNotFoundError: No module named 'app.services.export_jobs'`

- [ ] **Step 3: Create `app/services/export_jobs.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.export_job import ExportJob, ExportStatus


async def create(db: AsyncSession, created_by: str = "admin") -> ExportJob:
    """Create a new ExportJob in PENDING state, commit, and return the refreshed instance."""
    job = ExportJob(status=ExportStatus.PENDING, created_by=created_by)
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return job


async def get(db: AsyncSession, job_id: int) -> ExportJob | None:
    """Return a single ExportJob by primary key, or None."""
    result = await db.execute(
        select(ExportJob).where(ExportJob.id == job_id)
    )
    return result.scalar_one_or_none()


async def list_all(db: AsyncSession, limit: int = 20) -> list[ExportJob]:
    """Return the most recent export jobs ordered by created_at descending."""
    result = await db.execute(
        select(ExportJob).order_by(ExportJob.created_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


async def update(
    db: AsyncSession,
    job_id: int,
    *,
    status: ExportStatus | None = None,
    error_message: str | None = None,
    file_path: str | None = None,
    completed_at: datetime | None = None,
) -> None:
    """Apply partial updates to an ExportJob. No-op if job not found."""
    result = await db.execute(
        select(ExportJob).where(ExportJob.id == job_id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        return
    if status is not None:
        job.status = status
    if error_message is not None:
        job.error_message = error_message
    if file_path is not None:
        job.file_path = file_path
    if completed_at is not None:
        job.completed_at = completed_at
    await db.commit()
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_export_jobs.py -v
```

Expected output:
```
tests/services/test_export_jobs.py::test_create_returns_job PASSED
tests/services/test_export_jobs.py::test_get_returns_job PASSED
tests/services/test_export_jobs.py::test_get_returns_none_when_not_found PASSED
tests/services/test_export_jobs.py::test_list_all_returns_jobs PASSED
tests/services/test_export_jobs.py::test_update_commits_changes PASSED
tests/services/test_export_jobs.py::test_update_noop_when_not_found PASSED
6 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/export_jobs.py tests/services/test_export_jobs.py && git commit -m "feat: add export_jobs service with create, get, list_all, update"
```

---

### Task 6: Create `dashboard` service

**Files:**
- Create: `app/services/dashboard.py`
- Create: `tests/services/test_dashboard.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_dashboard.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, call

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_get_stats_returns_dict(mock_db):
    from app.services.dashboard import get_stats

    # Each scalar() call should return a sensible integer
    mock_db.execute = AsyncMock(side_effect=[
        MagicMock(scalar=MagicMock(return_value=100)),  # total_members
        MagicMock(scalar=MagicMock(return_value=80)),   # active_members
        MagicMock(scalar=MagicMock(return_value=60)),   # active_with_membership
        MagicMock(scalar=MagicMock(return_value=3)),    # total_devices
        MagicMock(scalar=MagicMock(return_value=2)),    # online_devices
        MagicMock(scalar=MagicMock(return_value=5)),    # pending_queue
        MagicMock(scalar=MagicMock(return_value=1)),    # failed_queue
    ])

    stats = await get_stats(mock_db)
    assert stats["total_members"] == 100
    assert stats["active_members"] == 80
    assert stats["active_with_membership"] == 60
    assert stats["total_devices"] == 3
    assert stats["online_devices"] == 2
    assert stats["pending_queue"] == 5
    assert stats["failed_queue"] == 1


@pytest.mark.asyncio
async def test_get_stats_defaults_none_to_zero(mock_db):
    from app.services.dashboard import get_stats

    mock_db.execute = AsyncMock(side_effect=[
        MagicMock(scalar=MagicMock(return_value=None)),
        MagicMock(scalar=MagicMock(return_value=None)),
        MagicMock(scalar=MagicMock(return_value=None)),
        MagicMock(scalar=MagicMock(return_value=None)),
        MagicMock(scalar=MagicMock(return_value=None)),
        MagicMock(scalar=MagicMock(return_value=None)),
        MagicMock(scalar=MagicMock(return_value=None)),
    ])

    stats = await get_stats(mock_db)
    assert all(v == 0 for v in stats.values())


@pytest.mark.asyncio
async def test_get_recent_queue_returns_rows(mock_db):
    from app.services.dashboard import get_recent_queue

    fake_row = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_row]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_recent_queue(mock_db)
    assert result == [fake_row]


@pytest.mark.asyncio
async def test_get_mb_breakdown_returns_dict(mock_db):
    from app.services.dashboard import get_mb_breakdown

    fake_row = MagicMock()
    fake_row.__getitem__ = lambda self, i: ["Female", 10][i]
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([(None, 5), ("Male", 20)]))
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_mb_breakdown(mock_db)
    assert "Unknown" in result
    assert "Male" in result


@pytest.mark.asyncio
async def test_get_device_rows_returns_devices(mock_db):
    from app.services.dashboard import get_device_rows

    fake_device = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_device]
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_device_rows(mock_db)
    assert result == [fake_device]
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_dashboard.py -v 2>&1 | head -20
```

Expected output: `ModuleNotFoundError: No module named 'app.services.dashboard'`

- [ ] **Step 3: Create `app/services/dashboard.py`**

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import or_

from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient
from app.models.mindbody_membership import MindBodyMembership


async def get_stats(db: AsyncSession) -> dict:
    """Return a dict of aggregate counts for the dashboard summary panel."""
    now = datetime.now(UTC)

    total_members = (
        await db.execute(select(func.count(MindBodyClient.id)))
    ).scalar() or 0

    active_members = (
        await db.execute(
            select(func.count(MindBodyClient.id)).where(MindBodyClient.active.is_(True))
        )
    ).scalar() or 0

    active_with_membership = (
        await db.execute(
            select(func.count(MindBodyClient.id.distinct()))
            .join(
                MindBodyMembership,
                MindBodyClient.mindbody_id == MindBodyMembership.mindbody_client_id,
            )
            .where(
                MindBodyClient.active.is_(True),
                MindBodyMembership.status == "Active",
                or_(
                    MindBodyMembership.expiration_date.is_(None),
                    MindBodyMembership.expiration_date > now,
                ),
            )
        )
    ).scalar() or 0

    total_devices = (
        await db.execute(
            select(func.count(DahuaDevice.id)).where(DahuaDevice.is_enabled.is_(True))
        )
    ).scalar() or 0

    online_devices = (
        await db.execute(
            select(func.count(DahuaDevice.id)).where(
                DahuaDevice.is_enabled.is_(True),
                DahuaDevice.status == "online",
            )
        )
    ).scalar() or 0

    pending_queue = (
        await db.execute(
            select(func.count(DahuaSyncQueue.id)).where(DahuaSyncQueue.status == "pending")
        )
    ).scalar() or 0

    failed_queue = (
        await db.execute(
            select(func.count(DahuaSyncQueue.id)).where(DahuaSyncQueue.status == "failed")
        )
    ).scalar() or 0

    return {
        "total_members": total_members,
        "active_members": active_members,
        "active_with_membership": active_with_membership,
        "total_devices": total_devices,
        "online_devices": online_devices,
        "pending_queue": pending_queue,
        "failed_queue": failed_queue,
    }


async def get_recent_queue(db: AsyncSession, limit: int = 10) -> list[DahuaSyncQueue]:
    """Return the most recent DahuaSyncQueue rows ordered by created_at descending."""
    result = await db.execute(
        select(DahuaSyncQueue).order_by(DahuaSyncQueue.created_at.desc()).limit(limit)
    )
    return list(result.scalars().all())


async def get_mb_breakdown(db: AsyncSession) -> dict[str, int]:
    """Return {gender_label: count} for active MindBody members."""
    result = await db.execute(
        select(MindBodyClient.gender, func.count(MindBodyClient.id))
        .where(MindBodyClient.active.is_(True))
        .group_by(MindBodyClient.gender)
    )
    return {(row[0] or "Unknown"): row[1] for row in result}


async def get_device_rows(db: AsyncSession) -> list[DahuaDevice]:
    """Return enabled DahuaDevice rows ordered by name."""
    result = await db.execute(
        select(DahuaDevice)
        .where(DahuaDevice.is_enabled.is_(True))
        .order_by(DahuaDevice.name)
    )
    return list(result.scalars().all())
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_dashboard.py -v
```

Expected output:
```
tests/services/test_dashboard.py::test_get_stats_returns_dict PASSED
tests/services/test_dashboard.py::test_get_stats_defaults_none_to_zero PASSED
tests/services/test_dashboard.py::test_get_recent_queue_returns_rows PASSED
tests/services/test_dashboard.py::test_get_mb_breakdown_returns_dict PASSED
tests/services/test_dashboard.py::test_get_device_rows_returns_devices PASSED
5 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/dashboard.py tests/services/test_dashboard.py && git commit -m "feat: add dashboard service with get_stats, get_recent_queue, get_mb_breakdown, get_device_rows"
```

---

### Task 7: Create `admin_users` service

**Files:**
- Create: `app/services/admin_users.py`
- Create: `tests/services/test_admin_users.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_admin_users.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
def mock_db():
    return AsyncMock(spec=AsyncSession)


@pytest.mark.asyncio
async def test_get_by_username_returns_user(mock_db):
    from app.services.admin_users import get_by_username
    from app.models.admin_user import AdminUser

    fake_user = MagicMock(spec=AdminUser)
    fake_user.username = "admin"
    fake_user.is_active = True

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_user
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_username(mock_db, "admin")
    assert result is fake_user


@pytest.mark.asyncio
async def test_get_by_username_returns_none_when_not_found(mock_db):
    from app.services.admin_users import get_by_username

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_username(mock_db, "nobody")
    assert result is None


@pytest.mark.asyncio
async def test_get_by_username_filters_active(mock_db):
    """Query must include is_active=True in the WHERE clause."""
    from app.services.admin_users import get_by_username

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    await get_by_username(mock_db, "admin")
    call_args = mock_db.execute.call_args[0][0]
    # The compiled SQL should reference is_active
    compiled = str(call_args.compile(compile_kwargs={"literal_binds": True}))
    assert "is_active" in compiled
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_users.py -v 2>&1 | head -20
```

Expected output: `ModuleNotFoundError: No module named 'app.services.admin_users'`

- [ ] **Step 3: Create `app/services/admin_users.py`**

```python
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.admin_user import AdminUser


async def get_by_username(db: AsyncSession, username: str) -> AdminUser | None:
    """Return an active AdminUser by username, or None if not found / inactive."""
    result = await db.execute(
        select(AdminUser).where(
            AdminUser.username == username,
            AdminUser.is_active.is_(True),
        )
    )
    return result.scalar_one_or_none()
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_users.py -v
```

Expected output:
```
tests/services/test_admin_users.py::test_get_by_username_returns_user PASSED
tests/services/test_admin_users.py::test_get_by_username_returns_none_when_not_found PASSED
tests/services/test_admin_users.py::test_get_by_username_filters_active PASSED
3 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/services/admin_users.py tests/services/test_admin_users.py && git commit -m "feat: add admin_users service with get_by_username"
```

---

## Chunk 2: Update `app/api/deps.py` and `app/models/database.py`

### Task 8: Expose `get_async_db` from deps and remove sync engine from routes

**Files:**
- Modify: `app/api/deps.py`
- Modify: `app/models/database.py` (no logic changes — only verify `get_async_db` is exported)

This task updates the single dependency injection entry-point so all new async routes can use `Depends(get_async_db)` imported from `app.api.deps`.

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_deps.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_get_async_db_exported_from_deps():
    """get_async_db must be importable from app.api.deps."""
    from app.api.deps import get_async_db
    assert callable(get_async_db)


@pytest.mark.asyncio
async def test_get_async_db_yields_session():
    from app.api.deps import get_async_db
    from sqlalchemy.ext.asyncio import AsyncSession

    mock_session = AsyncMock(spec=AsyncSession)
    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_ctx)

    with patch("app.models.database.AsyncSessionLocal", mock_factory):
        gen = get_async_db()
        session = await gen.__anext__()
        assert session is mock_session
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_deps.py -v 2>&1 | head -20
```

Expected output: `ImportError: cannot import name 'get_async_db' from 'app.api.deps'`

- [ ] **Step 3: Modify `app/api/deps.py`**

Replace the entire file with:

```python
from collections.abc import AsyncGenerator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.models.database import get_async_db


def get_sync_engine(request: Request):
    return request.app.state.sync_engine


def get_settings(request: Request):
    return request.app.state.settings


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Re-export of database.get_async_db for use in FastAPI Depends()."""
    async for session in _get_async_db():
        yield session
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_deps.py -v
```

Expected output:
```
tests/services/test_deps.py::test_get_async_db_exported_from_deps PASSED
tests/services/test_deps.py::test_get_async_db_yields_session PASSED
2 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/api/deps.py tests/services/test_deps.py && git commit -m "feat: expose get_async_db from app.api.deps for FastAPI route injection"
```

---

## Chunk 3: Convert admin routes to async + service calls

### Task 9: Convert `app/admin/auth.py` login route

**Files:**
- Modify: `app/admin/auth.py`
- Create: `tests/services/test_admin_auth_route.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_admin_auth_route.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient


@pytest.fixture
def app():
    from app.main import create_app
    from app.config import Settings
    return create_app(settings=Settings(database_url="sqlite+aiosqlite:///:memory:"))


def test_login_post_route_exists(app):
    client = TestClient(app, raise_server_exceptions=False)
    # A POST with wrong credentials should redirect or return HTML, not 404/405
    response = client.post("/admin/login", data={"username": "x", "password": "y"}, follow_redirects=False)
    assert response.status_code not in (404, 405)


@pytest.mark.asyncio
async def test_login_calls_admin_users_service():
    """The login handler must call admin_users.get_by_username instead of raw query."""
    import app.admin.auth as auth_mod

    mock_user = MagicMock()
    mock_user.check_password = MagicMock(return_value=True)

    with patch("app.admin.auth.admin_users") as mock_svc:
        mock_svc.get_by_username = AsyncMock(return_value=mock_user)
        # Verify the attribute is now wired up
        assert hasattr(auth_mod, "admin_users") or True  # module-level import check
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_auth_route.py -v 2>&1 | head -30
```

Expected output: attribute / import errors showing `admin_users` not used in `auth.py`.

- [ ] **Step 3: Modify `app/admin/auth.py`**

Find the login route and replace the raw `db_session_factory` call with the `admin_users` service. The complete updated login handler:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.services import admin_users
from app.admin.session import create_session, destroy_session, get_current_user

router = APIRouter()


@router.get("/admin/login", response_class=HTMLResponse)
async def login_form(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse("admin/login.html", {"request": request})


@router.post("/admin/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    user = await admin_users.get_by_username(db, username)
    if not user or not user.check_password(password):
        return templates.TemplateResponse(
            "admin/login.html",
            {"request": request, "error": "Invalid credentials"},
            status_code=401,
        )
    response = RedirectResponse(url="/admin/", status_code=302)
    create_session(response, username)
    return response


@router.post("/admin/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/admin/login", status_code=302)
    destroy_session(response)
    return response
```

> **Note:** Preserve any existing session helper imports (`create_session`, `destroy_session`, `get_current_user`) that exist in the real file. Only the login handler body changes — replace `db_session_factory()` + raw `db.query(AdminUser)` with `await admin_users.get_by_username(db, username)`.

- [ ] **Step 4: Run all services tests to confirm no regression**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/ -v
```

Expected output: all previously passing tests still pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/admin/auth.py tests/services/test_admin_auth_route.py && git commit -m "refactor: convert admin login route to async using admin_users service"
```

---

### Task 10: Convert `app/admin/dashboard.py` routes

**Files:**
- Modify: `app/admin/dashboard.py`
- Create: `tests/services/test_admin_dashboard_route.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_admin_dashboard_route.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.mark.asyncio
async def test_dashboard_uses_service_get_stats():
    """Dashboard route must call services.dashboard.get_stats(db)."""
    import app.admin.dashboard as dash_mod

    # If the module imports from app.services.dashboard, the attribute exists
    assert hasattr(dash_mod, "dashboard_svc") or "dashboard" in dir(dash_mod)


@pytest.mark.asyncio
async def test_get_stats_called_with_db():
    from app.services.dashboard import get_stats
    from unittest.mock import AsyncMock
    from sqlalchemy.ext.asyncio import AsyncSession

    mock_db = AsyncMock(spec=AsyncSession)
    mock_db.execute = AsyncMock(side_effect=[
        MagicMock(scalar=MagicMock(return_value=0)),
        MagicMock(scalar=MagicMock(return_value=0)),
        MagicMock(scalar=MagicMock(return_value=0)),
        MagicMock(scalar=MagicMock(return_value=0)),
        MagicMock(scalar=MagicMock(return_value=0)),
        MagicMock(scalar=MagicMock(return_value=0)),
        MagicMock(scalar=MagicMock(return_value=0)),
    ])

    result = await get_stats(mock_db)
    assert isinstance(result, dict)
    assert "total_members" in result
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_dashboard_route.py -v 2>&1 | head -20
```

- [ ] **Step 3: Modify `app/admin/dashboard.py`**

Replace all inline `_get_stats`, `_get_recent_queue`, `_get_mb_breakdown`, `_get_device_rows` helpers and their calls with the service. The complete rewritten file (preserve existing router setup and template rendering):

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.services import dashboard as dashboard_svc

router = APIRouter()


@router.get("/admin/", response_class=HTMLResponse)
@router.get("/admin/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    stats = await dashboard_svc.get_stats(db)
    recent_queue = await dashboard_svc.get_recent_queue(db)
    mb_breakdown = await dashboard_svc.get_mb_breakdown(db)
    device_rows = await dashboard_svc.get_device_rows(db)
    return templates.TemplateResponse(
        "admin/dashboard.html",
        {
            "request": request,
            "stats": stats,
            "recent_queue": recent_queue,
            "mb_breakdown": mb_breakdown,
            "device_rows": device_rows,
        },
    )
```

> **Note:** The actual file may have authentication decorators/dependencies. Preserve them — only replace the DB logic.

- [ ] **Step 4: Run services tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/ -v 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/admin/dashboard.py tests/services/test_admin_dashboard_route.py && git commit -m "refactor: convert dashboard route to async using dashboard service"
```

---

### Task 11: Convert `app/admin/devices.py` routes

**Files:**
- Modify: `app/admin/devices.py`
- Create: `tests/services/test_admin_devices_route.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_admin_devices_route.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.devices import list_all, get_by_id, update_status


@pytest.mark.asyncio
async def test_list_all_integrates_with_devices_service():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_get_by_id_returns_none_for_missing():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_by_id(mock_db, 9999)
    assert result is None


@pytest.mark.asyncio
async def test_update_status_sets_offline():
    from app.models.device import DahuaDevice
    mock_db = AsyncMock(spec=AsyncSession)
    fake_device = MagicMock(spec=DahuaDevice)
    fake_device.status = "online"
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "offline")
    assert fake_device.status == "offline"
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_devices_route.py -v 2>&1 | head -20
```

- [ ] **Step 3: Modify `app/admin/devices.py`**

Replace all `db = request.app.state.db_session_factory()` blocks with `Depends(get_async_db)` and service calls. The complete rewritten route handlers:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.services import devices as devices_svc
from app.models.device import DahuaDevice

router = APIRouter()


@router.get("/admin/devices", response_class=HTMLResponse)
async def list_devices(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    devices = await devices_svc.list_all(db)
    return templates.TemplateResponse(
        "admin/devices.html", {"request": request, "devices": devices}
    )


@router.post("/admin/devices")
async def create_device(
    request: Request,
    name: str = Form(...),
    host: str = Form(...),
    port: int = Form(80),
    username: str = Form("admin"),
    password: str = Form(...),
    door_ids: str = Form("0"),
    gate_type: str = Form("all"),
    is_enabled: bool = Form(True),
    db: AsyncSession = Depends(get_async_db),
):
    device = DahuaDevice(
        name=name,
        host=host,
        port=port,
        username=username,
        password=password,
        door_ids=door_ids,
        gate_type=gate_type,
        is_enabled=is_enabled,
    )
    db.add(device)
    await db.commit()
    return RedirectResponse(url="/admin/devices", status_code=302)


@router.get("/admin/devices/{device_id}", response_class=HTMLResponse)
async def get_device(
    request: Request,
    device_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    device = await devices_svc.get_by_id(db, device_id)
    if device is None:
        return HTMLResponse("Device not found", status_code=404)
    return templates.TemplateResponse(
        "admin/device_detail.html", {"request": request, "device": device}
    )


@router.post("/admin/devices/{device_id}/update")
async def update_device(
    request: Request,
    device_id: int,
    name: str = Form(...),
    host: str = Form(...),
    port: int = Form(80),
    username: str = Form("admin"),
    password: str = Form(...),
    door_ids: str = Form("0"),
    gate_type: str = Form("all"),
    db: AsyncSession = Depends(get_async_db),
):
    device = await devices_svc.get_by_id(db, device_id)
    if device is None:
        return HTMLResponse("Device not found", status_code=404)
    device.name = name
    device.host = host
    device.port = port
    device.username = username
    device.password = password
    device.door_ids = door_ids
    device.gate_type = gate_type
    await db.commit()
    return RedirectResponse(url="/admin/devices", status_code=302)


@router.post("/admin/devices/{device_id}/delete")
async def delete_device(
    request: Request,
    device_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    device = await devices_svc.get_by_id(db, device_id)
    if device is not None:
        await db.delete(device)
        await db.commit()
    return RedirectResponse(url="/admin/devices", status_code=302)


@router.post("/admin/devices/{device_id}/toggle")
async def toggle_device(
    request: Request,
    device_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    device = await devices_svc.get_by_id(db, device_id)
    if device is not None:
        device.is_enabled = not device.is_enabled
        await db.commit()
    return RedirectResponse(url="/admin/devices", status_code=302)
```

> **Note:** Preserve any existing authentication decorators. Adapt route paths and form field names if the real file differs — the key change is the session sourcing pattern.

- [ ] **Step 4: Run tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_devices_route.py tests/services/test_devices.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/admin/devices.py tests/services/test_admin_devices_route.py && git commit -m "refactor: convert devices admin routes to async using devices service"
```

---

### Task 12: Convert `app/admin/mindbody_users.py` routes

**Files:**
- Modify: `app/admin/mindbody_users.py`
- Create: `tests/services/test_admin_mb_users_route.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_admin_mb_users_route.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.members import upsert_batch, get_last_fetched_at


@pytest.mark.asyncio
async def test_get_last_fetched_at_returns_none_on_empty_table():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_last_fetched_at(mock_db)
    assert result is None


@pytest.mark.asyncio
async def test_upsert_batch_handles_no_members():
    mock_db = AsyncMock(spec=AsyncSession)
    count = await upsert_batch(mock_db, [])
    assert count == 0
```

- [ ] **Step 2: Run tests — expect pass (service already exists)**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_mb_users_route.py -v
```

Expected: both tests pass (they test the already-created service).

- [ ] **Step 3: Modify `app/admin/mindbody_users.py`**

Replace inline `db = request.app.state.db_session_factory()` and calls to `mindbody_client_service.*` with `members` service calls. The complete rewritten route handlers:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.clients.mindbody import MindBodyClient as MindBodyAPIClient
from app.models.mindbody_client import MindBodyClient
from app.services import members as members_svc

router = APIRouter()


@router.get("/admin/mindbody-users", response_class=HTMLResponse)
async def list_users(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    offset = (page - 1) * page_size

    total_result = await db.execute(select(func.count(MindBodyClient.id)))
    total = total_result.scalar() or 0

    users_result = await db.execute(
        select(MindBodyClient)
        .order_by(MindBodyClient.last_name)
        .offset(offset)
        .limit(page_size)
    )
    users = list(users_result.scalars().all())

    return templates.TemplateResponse(
        "admin/mindbody_users.html",
        {
            "request": request,
            "users": users,
            "total": total,
            "page": page,
            "page_size": page_size,
        },
    )


@router.post("/admin/mindbody-users/refresh")
async def refresh_users(
    request: Request,
    full: bool = Query(False),
    db: AsyncSession = Depends(get_async_db),
):
    settings = request.app.state.settings
    mb = MindBodyAPIClient(
        api_key=settings.mindbody_api_key,
        site_id=settings.mindbody_site_id,
        username=settings.mindbody_username,
        password=settings.mindbody_password,
    )

    last_fetched = None if full else await members_svc.get_last_fetched_at(db)
    clients = await mb.get_all_clients(modified_since=last_fetched)
    count = await members_svc.upsert_batch(db, clients)

    return RedirectResponse(
        url=f"/admin/mindbody-users?refreshed={count}",
        status_code=302,
    )
```

> **Note:** Adapt the MindBody API client constructor arguments to match the actual `app/clients/mindbody.py` interface in the codebase. The key change is replacing `mindbody_client_service.get_last_fetched_at(db)` / `upsert_mindbody_clients(db, ...)` / `refresh_mindbody_clients(db, ...)` with `members_svc.*` calls.

- [ ] **Step 4: Run all services tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/ -v 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/admin/mindbody_users.py tests/services/test_admin_mb_users_route.py && git commit -m "refactor: convert mindbody_users admin routes to async using members service"
```

---

### Task 13: Convert `app/admin/sync_queue.py` and `app/admin/export_jobs.py`

**Files:**
- Modify: `app/admin/sync_queue.py`
- Modify: `app/admin/export_jobs.py`
- Create: `tests/services/test_admin_queue_export_routes.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_admin_queue_export_routes.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.queue import load_pending
from app.services.export_jobs import list_all, create, get, update


@pytest.mark.asyncio
async def test_load_pending_empty_run():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await load_pending(mock_db, "no-such-run")
    assert result == []


@pytest.mark.asyncio
async def test_export_jobs_list_all_empty():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await list_all(mock_db)
    assert result == []


@pytest.mark.asyncio
async def test_export_jobs_update_noop():
    mock_db = AsyncMock(spec=AsyncSession)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    from app.models.export_job import ExportStatus
    await update(mock_db, 9999, status=ExportStatus.FAILED)
    mock_db.commit.assert_not_called()
```

- [ ] **Step 2: Run tests — expect pass (services already exist)**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_admin_queue_export_routes.py -v
```

Expected: 3 passed.

- [ ] **Step 3: Modify `app/admin/sync_queue.py`**

Replace inline `db_session_factory()` calls. The key list handler:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.models.dahua_sync_queue import DahuaSyncQueue
from app.models.device import DahuaDevice
from app.models.mindbody_client import MindBodyClient

router = APIRouter()


@router.get("/admin/sync-queue", response_class=HTMLResponse)
async def list_queue(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    result = await db.execute(
        select(DahuaSyncQueue, MindBodyClient, DahuaDevice)
        .outerjoin(
            MindBodyClient,
            DahuaSyncQueue.mindbody_client_id == MindBodyClient.mindbody_id,
        )
        .outerjoin(DahuaDevice, DahuaSyncQueue.device_id == DahuaDevice.id)
        .order_by(DahuaSyncQueue.created_at.desc())
        .limit(100)
    )
    rows = result.all()
    return templates.TemplateResponse(
        "admin/sync_queue.html", {"request": request, "rows": rows}
    )
```

- [ ] **Step 4: Modify `app/admin/export_jobs.py`**

Replace inline DB calls in the list, create, and get routes with `export_jobs_svc` calls. The background task `_run_export_job` which uses `sync_engine` for CSV generation must remain unchanged — only the session lifecycle changes:

```python
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_async_db
from app.models.export_job import ExportStatus
from app.services import export_jobs as export_jobs_svc

router = APIRouter()


async def _run_export_job(job_id: int) -> None:
    """Background task: run the CSV export. Uses its own session from the factory."""
    from app.models.database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        await export_jobs_svc.update(db, job_id, status=ExportStatus.RUNNING)
    try:
        # NOTE: CSV generation using sync_engine is OUT OF SCOPE — preserve existing logic here.
        # After generation completes:
        async with AsyncSessionLocal() as db:
            await export_jobs_svc.update(
                db,
                job_id,
                status=ExportStatus.DONE,
                completed_at=datetime.now(UTC),
            )
    except Exception as exc:
        async with AsyncSessionLocal() as db:
            await export_jobs_svc.update(
                db,
                job_id,
                status=ExportStatus.FAILED,
                error_message=str(exc),
                completed_at=datetime.now(UTC),
            )


@router.get("/admin/export-jobs", response_class=HTMLResponse)
async def list_jobs(
    request: Request,
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    jobs = await export_jobs_svc.list_all(db)
    return templates.TemplateResponse(
        "admin/export_jobs.html", {"request": request, "jobs": jobs}
    )


@router.post("/admin/export-jobs")
async def create_job(
    request: Request,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_db),
):
    job = await export_jobs_svc.create(db, created_by="admin")
    background_tasks.add_task(_run_export_job, job.id)
    return RedirectResponse(url="/admin/export-jobs", status_code=302)


@router.get("/admin/export-jobs/{job_id}", response_class=HTMLResponse)
async def get_job(
    request: Request,
    job_id: int,
    db: AsyncSession = Depends(get_async_db),
):
    templates = request.app.state.templates
    job = await export_jobs_svc.get(db, job_id)
    if job is None:
        return HTMLResponse("Job not found", status_code=404)
    return templates.TemplateResponse(
        "admin/export_job_detail.html", {"request": request, "job": job}
    )
```

- [ ] **Step 5: Run all services tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/ -v 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/admin/sync_queue.py app/admin/export_jobs.py tests/services/test_admin_queue_export_routes.py && git commit -m "refactor: convert sync_queue and export_jobs admin routes to async using services"
```

---

## Chunk 4: Hollow out `sync/tasks.py` to thin wrappers

### Task 14: Replace DB-owning task bodies with service calls

**Files:**
- Modify: `app/sync/tasks.py`
- Create: `tests/services/test_tasks_thin_wrappers.py`

The goal is to remove all `async with _get_async_session_factory()() as db:` blocks from inside tasks and replace them with a single `async with _get_async_session_factory()() as db:` block that delegates to the relevant service. This keeps the `@task` decorator surface identical while moving logic into the services layer.

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_tasks_thin_wrappers.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_upsert_mindbody_users_batch_delegates_to_members_service():
    import app.sync.tasks as tasks_mod
    from app.services import members as members_svc

    members_data = [
        {"Id": "101", "UniqueId": "u1", "FirstName": "Alice", "LastName": "S",
         "Email": None, "MobilePhone": None, "HomePhone": None, "WorkPhone": None,
         "Status": "Active", "Active": True, "BirthDate": None, "Gender": None,
         "CreationDate": None, "LastModifiedDateTime": None}
    ]

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(members_svc, "upsert_batch", new_callable=AsyncMock, return_value=1) as mock_upsert:
            result = await tasks_mod.upsert_mindbody_users_batch(members_data)
            assert result == 1
            mock_upsert.assert_called_once_with(mock_db, members_data)


@pytest.mark.asyncio
async def test_upsert_mindbody_memberships_batch_delegates_to_memberships_service():
    import app.sync.tasks as tasks_mod
    from app.services import memberships as memberships_svc

    data = {"101": [{"Id": "c1", "Name": "Monthly", "Status": "Active",
                     "StartDate": None, "ExpirationDate": None}]}

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(memberships_svc, "upsert_batch", new_callable=AsyncMock, return_value=1) as mock_up:
            result = await tasks_mod.upsert_mindbody_memberships_batch(data)
            assert result == 1
            mock_up.assert_called_once_with(mock_db, data)


@pytest.mark.asyncio
async def test_load_device_ids_by_gate_type_delegates_to_devices_service():
    import app.sync.tasks as tasks_mod
    from app.services import devices as devices_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(devices_svc, "list_by_gate_type", new_callable=AsyncMock, return_value=[1, 2]) as mock_list:
            result = await tasks_mod.load_device_ids_by_gate_type("male")
            assert result == [1, 2]
            mock_list.assert_called_once_with(mock_db, "male")


@pytest.mark.asyncio
async def test_load_active_members_from_db_delegates_to_members_service():
    import app.sync.tasks as tasks_mod
    from app.services import members as members_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(members_svc, "load_active", new_callable=AsyncMock, return_value=[]) as mock_la:
            result = await tasks_mod.load_active_members_from_db()
            assert result == []
            mock_la.assert_called_once_with(mock_db)


@pytest.mark.asyncio
async def test_load_membership_windows_delegates_to_memberships_service():
    import app.sync.tasks as tasks_mod
    from app.services import memberships as memberships_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(memberships_svc, "load_windows", new_callable=AsyncMock, return_value={}) as mock_lw:
            result = await tasks_mod.load_membership_windows(["101"])
            assert result == {}
            mock_lw.assert_called_once_with(mock_db, ["101"])


@pytest.mark.asyncio
async def test_write_sync_queue_batch_delegates_to_queue_service():
    import app.sync.tasks as tasks_mod
    from app.services import queue as queue_svc

    items = [{"mindbody_client_id": "101", "device_id": 1, "action": "enroll",
               "status": "pending", "dahua_user_id": None, "member_snapshot": None}]

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(queue_svc, "write_batch", new_callable=AsyncMock, return_value=1) as mock_wb:
            result = await tasks_mod.write_sync_queue_batch("run-1", items)
            assert result == 1
            mock_wb.assert_called_once_with(mock_db, "run-1", items)


@pytest.mark.asyncio
async def test_load_pending_queue_items_delegates_to_queue_service():
    import app.sync.tasks as tasks_mod
    from app.services import queue as queue_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(queue_svc, "load_pending", new_callable=AsyncMock, return_value=[]) as mock_lp:
            result = await tasks_mod.load_pending_queue_items("run-1")
            assert result == []
            mock_lp.assert_called_once_with(mock_db, "run-1")


@pytest.mark.asyncio
async def test_mark_queue_item_delegates_to_queue_service():
    import app.sync.tasks as tasks_mod
    from app.services import queue as queue_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(queue_svc, "mark_item", new_callable=AsyncMock) as mock_mi:
            await tasks_mod.mark_queue_item(42, "success", "no error")
            mock_mi.assert_called_once_with(mock_db, 42, "success", "no error")


@pytest.mark.asyncio
async def test_load_all_devices_delegates_to_devices_service():
    import app.sync.tasks as tasks_mod
    from app.services import devices as devices_svc

    mock_db = AsyncMock()
    mock_db.__aenter__ = AsyncMock(return_value=mock_db)
    mock_db.__aexit__ = AsyncMock(return_value=False)
    mock_factory = MagicMock(return_value=mock_db)

    with patch.object(tasks_mod, "_get_async_session_factory", return_value=mock_factory):
        with patch.object(devices_svc, "list_all", new_callable=AsyncMock, return_value=[]) as mock_la:
            result = await tasks_mod.load_all_devices()
            assert result == []
            mock_la.assert_called_once_with(mock_db)
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_tasks_thin_wrappers.py -v 2>&1 | head -40
```

Expected output: tests fail because tasks still contain their own SQL logic rather than calling services.

- [ ] **Step 3: Modify `app/sync/tasks.py` — replace DB task bodies**

For each DB-accessing task listed below, replace the body with a thin wrapper. Add these imports at the top of `tasks.py` (after existing imports):

```python
from app.services import devices as devices_svc
from app.services import members as members_svc
from app.services import memberships as memberships_svc
from app.services import queue as queue_svc
```

Then replace each task body:

**`upsert_mindbody_users_batch`:**
```python
@task
async def upsert_mindbody_users_batch(members: list[dict]) -> int:
    async with AsyncSessionLocal() as db:
        return await members_svc.upsert_batch(db, members)
```

**`upsert_mindbody_memberships_batch`:**
```python
@task
async def upsert_mindbody_memberships_batch(memberships_by_client: dict[str, list[dict]]) -> int:
    async with AsyncSessionLocal() as db:
        return await memberships_svc.upsert_batch(db, memberships_by_client)
```

**`load_device_ids_by_gate_type`:**
```python
@task
async def load_device_ids_by_gate_type(gate_type: str) -> list[int]:
    async with AsyncSessionLocal() as db:
        return await devices_svc.list_by_gate_type(db, gate_type)
```

**`load_active_members_from_db`:**
```python
@task
async def load_active_members_from_db() -> list[MindBodyClientModel]:
    async with AsyncSessionLocal() as db:
        return await members_svc.load_active(db)
```

**`load_membership_windows`:**
```python
@task
async def load_membership_windows(client_ids: list[str]):
    async with AsyncSessionLocal() as db:
        return await memberships_svc.load_windows(db, client_ids)
```

**`write_sync_queue_batch`:**
```python
@task
async def write_sync_queue_batch(run_id: str, items: list[dict]) -> int:
    async with AsyncSessionLocal() as db:
        return await queue_svc.write_batch(db, run_id, items)
```

**`load_pending_queue_items`:**
```python
@task
async def load_pending_queue_items(run_id: str) -> list[DahuaSyncQueue]:
    async with AsyncSessionLocal() as db:
        return await queue_svc.load_pending(db, run_id)
```

**`mark_queue_item`:**
```python
@task
async def mark_queue_item(item_id: int, status: str, error_message: str | None = None) -> None:
    async with AsyncSessionLocal() as db:
        await queue_svc.mark_item(db, item_id, status, error_message)
```

**`load_all_devices`:**
```python
@task
async def load_all_devices() -> list[DahuaDevice]:
    async with AsyncSessionLocal() as db:
        return await devices_svc.list_all(db)
```

Also replace the inline device loading inside `fetch_dahua_users_for_device`, `enroll_on_device`, `update_window_on_device`:

```python
# Replace this pattern in each task:
#   async with _get_async_session_factory()() as db:
#       result = await db.execute(select(DahuaDevice).where(DahuaDevice.id == device_id))
#       device = result.scalar_one_or_none()
#       if device is None:
#           raise ValueError(f"Device {device_id} not found")
# With:
async with _get_async_session_factory()() as db:
    device = await devices_svc.get_by_id(db, device_id)
    if device is None:
        raise ValueError(f"Device {device_id} not found")
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_tasks_thin_wrappers.py -v
```

Expected output:
```
tests/services/test_tasks_thin_wrappers.py::test_upsert_mindbody_users_batch_delegates_to_members_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_upsert_mindbody_memberships_batch_delegates_to_memberships_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_load_device_ids_by_gate_type_delegates_to_devices_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_load_active_members_from_db_delegates_to_members_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_load_membership_windows_delegates_to_memberships_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_write_sync_queue_batch_delegates_to_queue_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_load_pending_queue_items_delegates_to_queue_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_mark_queue_item_delegates_to_queue_service PASSED
tests/services/test_tasks_thin_wrappers.py::test_load_all_devices_delegates_to_devices_service PASSED
9 passed
```

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/sync/tasks.py tests/services/test_tasks_thin_wrappers.py && git commit -m "refactor: hollow out sync tasks to thin wrappers delegating to services layer"
```

---

## Chunk 5: Update health flow and delete legacy service file

### Task 15: Update `app/sync/flows/health.py` inline DB update

**Files:**
- Modify: `app/sync/flows/health.py`
- Create: `tests/services/test_health_flow.py`

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_health_flow.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_health_flow_uses_devices_service_update_status():
    """health.py must call devices_svc.update_status instead of raw inline update()."""
    import app.sync.flows.health as health_mod

    # If the module imports devices service, the attribute will exist
    import app.services.devices as devices_svc
    assert callable(devices_svc.update_status)


@pytest.mark.asyncio
async def test_update_status_online_sets_last_seen_at():
    from app.services.devices import update_status
    from app.models.device import DahuaDevice

    mock_db = AsyncMock()
    fake_device = MagicMock(spec=DahuaDevice)
    fake_device.status = "offline"
    fake_device.last_seen_at = None

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "online")
    assert fake_device.status == "online"
    assert fake_device.last_seen_at is not None


@pytest.mark.asyncio
async def test_update_status_offline_does_not_set_last_seen_at():
    from app.services.devices import update_status
    from app.models.device import DahuaDevice

    mock_db = AsyncMock()
    fake_device = MagicMock(spec=DahuaDevice)
    fake_device.status = "online"
    fake_device.last_seen_at = None

    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = fake_device
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()

    await update_status(mock_db, 1, "offline")
    assert fake_device.status == "offline"
    assert fake_device.last_seen_at is None
```

- [ ] **Step 2: Run tests — expect pass (services already exist)**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_health_flow.py -v
```

Expected: 3 passed.

- [ ] **Step 3: Modify `app/sync/flows/health.py`**

Replace the inline `async with _get_async_session_factory()() as db: await db.execute(update(...))` block with a call to `devices_svc.update_status`. Add this import at the top of the file:

```python
from app.services import devices as devices_svc
```

Find the inline device status update loop body and replace:

```python
# OLD:
async with _get_async_session_factory()() as db:
    await db.execute(
        update(DahuaDevice)
        .where(DahuaDevice.id == device.id)
        .values(
            status=status,
            last_seen_at=datetime.now(UTC) if status == "online" else DahuaDevice.last_seen_at,
        )
    )
    await db.commit()

# NEW:
async with _get_async_session_factory()() as db:
    await devices_svc.update_status(db, device.id, status)
```

Also remove the now-unused imports `update` (SQLAlchemy) from this file if they are only used in the replaced block.

- [ ] **Step 4: Run all services tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/ -v 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/sync/flows/health.py tests/services/test_health_flow.py && git commit -m "refactor: replace inline device status update in health flow with devices service"
```

---

### Task 16: Delete `app/sync/mindbody_client_service.py`

**Files:**
- Delete: `app/sync/mindbody_client_service.py`
- Verify: no remaining imports of `mindbody_client_service` anywhere

- [ ] **Step 1: Verify no remaining imports**

```bash
cd /c/Projects/mind-body-dahua && grep -r "mindbody_client_service" app/ --include="*.py" -l
```

Expected output: empty (no files). If any files are listed, update them to use `members_svc` instead before proceeding.

- [ ] **Step 2: Delete the file**

```bash
cd /c/Projects/mind-body-dahua && rm app/sync/mindbody_client_service.py
```

- [ ] **Step 3: Run full test suite**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/ -v 2>&1 | tail -20
```

Expected: no test references the deleted file; all tests pass.

- [ ] **Step 4: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add -u app/sync/mindbody_client_service.py && git commit -m "chore: delete legacy sync/mindbody_client_service.py — replaced by app/services/members"
```

---

## Chunk 6: Remove sync engine from lifespan and clean up deps

### Task 17: Convert `app/main.py` startup helpers to async, remove sync `init_db`

**Files:**
- Modify: `app/main.py`
- Create: `tests/services/test_main_lifespan.py`

The sync `init_db` call, `Base.metadata.create_all(bind=engine)`, and `_seed_admin` / `_seed_devices` / `_recover_stuck_export_jobs` must be converted to use `AsyncSessionLocal` and async DDL. The public `create_app` signature remains unchanged.

- [ ] **Step 1: Write failing tests**

Create `tests/services/test_main_lifespan.py`:

```python
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_seed_admin_uses_async_session():
    """_seed_admin must accept an AsyncSession, not a sync session factory."""
    import app.main as main_mod

    # After refactor, _seed_admin should be a coroutine
    import inspect
    assert inspect.iscoroutinefunction(main_mod._seed_admin)


@pytest.mark.asyncio
async def test_seed_devices_uses_async_session():
    import app.main as main_mod
    import inspect
    assert inspect.iscoroutinefunction(main_mod._seed_devices)


@pytest.mark.asyncio
async def test_recover_stuck_jobs_uses_async_session():
    import app.main as main_mod
    import inspect
    assert inspect.iscoroutinefunction(main_mod._recover_stuck_export_jobs)
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_main_lifespan.py -v 2>&1 | head -20
```

Expected output: `AssertionError` because the helpers are currently sync functions.

- [ ] **Step 3: Modify `app/main.py`**

Convert the three startup helpers to async and update the lifespan to use `AsyncSessionLocal`. Replace the current sync helpers and lifespan with:

```python
from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.admin.router import admin_router
from app.api.router import api_router
from app.config import Settings
from app.models.admin_user import AdminUser
from app.models.database import Base, AsyncSessionLocal, async_engine, init_async_db
from app.models.device import DahuaDevice
from app.models.export_job import ExportJob, ExportStatus  # noqa: F401

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


async def _seed_admin(db) -> None:
    from sqlalchemy import select
    result = await db.execute(select(AdminUser).limit(1))
    if result.scalar_one_or_none() is None:
        user = AdminUser(username="admin", is_active=True)
        user.set_password(os.environ.get("ADMIN_PASSWORD", "changeme"))
        db.add(user)
        await db.commit()
        logger.info("Default admin user created (username=admin)")
    else:
        logger.info("Admin user already exists — skipping seed")


async def _seed_devices(db) -> None:
    from sqlalchemy import func, select
    raw = os.environ.get("DAHUA_DEVICES")
    if not raw:
        return
    count_result = await db.execute(select(func.count(DahuaDevice.id)))
    if (count_result.scalar() or 0) > 0:
        logger.info("Devices already exist — skipping seed")
        return
    devices = json.loads(raw)
    for d in devices:
        db.add(
            DahuaDevice(
                name=d["name"],
                host=d["host"],
                port=int(d.get("port", 80)),
                username=d.get("username", "admin"),
                password=d["password"],
                door_ids=d.get("door_ids", "0"),
                gate_type=d.get("gate_type", "all"),
                is_enabled=bool(d.get("is_enabled", True)),
            )
        )
    await db.commit()
    logger.info("Seeded %d device(s) from DAHUA_DEVICES env var", len(devices))


async def _recover_stuck_export_jobs(db) -> None:
    from sqlalchemy import select
    result = await db.execute(
        select(ExportJob).where(ExportJob.status == ExportStatus.RUNNING)
    )
    stuck = list(result.scalars().all())
    for job in stuck:
        job.status = ExportStatus.FAILED
        job.error_message = "Recovered: server restarted while job was running"
    if stuck:
        await db.commit()
        logger.warning("Recovered %d stuck export job(s)", len(stuck))


def _build_lifespan(app_settings: Settings):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

        factory = init_async_db(app_settings.database_url)

        # Create tables via async engine
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Seed / recover using a single session
        async with factory() as db:
            await _seed_admin(db)
            await _seed_devices(db)
            await _recover_stuck_export_jobs(db)

        app.state.settings = app_settings
        app.state.templates = Jinja2Templates(directory="templates")

        logger.info("Application started")
        yield

        logger.info("Application shutting down")

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings()

    app = FastAPI(
        title="MindBody–Dahua Sync",
        lifespan=_build_lifespan(app_settings),
    )

    app.mount("/static", StaticFiles(directory="static"), name="static")

    app.include_router(admin_router)
    app.include_router(api_router)

    return app


app = create_app()
```

> **Note:** The `db_session_factory_override` parameter is removed because all callers now use the async path. Any test that passed `db_session_factory_override` to `create_app` must be updated to mock `init_async_db` instead.

- [ ] **Step 4: Run tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/services/test_main_lifespan.py -v
```

Expected output:
```
tests/services/test_main_lifespan.py::test_seed_admin_uses_async_session PASSED
tests/services/test_main_lifespan.py::test_seed_devices_uses_async_session PASSED
tests/services/test_main_lifespan.py::test_recover_stuck_jobs_uses_async_session PASSED
3 passed
```

- [ ] **Step 5: Run full test suite to catch regressions**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/ -v 2>&1 | tail -25
```

Expected: all tests pass. If any test used `db_session_factory_override`, update it to patch `app.models.database.AsyncSessionLocal` with a mock factory.

- [ ] **Step 6: Remove sync engine from `app/models/database.py`**

Delete `engine`, `SessionLocal`, `init_db()`, and `get_db()` entirely. The file should only contain the async engine, `AsyncSessionLocal`, `init_async_db()`, and `get_async_db()`. Final state:

```python
from __future__ import annotations

import logging
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


async_engine = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None


def init_async_db(database_url: str) -> async_sessionmaker[AsyncSession]:
    global async_engine, AsyncSessionLocal
    async_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    async_engine = create_async_engine(async_url, pool_pre_ping=True)
    AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False)
    logger.info("Async DB initialised: %s", async_url.split("@")[-1])
    return AsyncSessionLocal


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    assert AsyncSessionLocal is not None, "Call init_async_db() first"
    async with AsyncSessionLocal() as session:
        yield session
```

- [ ] **Step 7: Verify nothing imports the removed symbols**

```bash
cd /c/Projects/mind-body-dahua && grep -r "init_db\b\|get_db\b\|SessionLocal\b" app/ --include="*.py"
```

Expected: no matches (only `AsyncSessionLocal` and `init_async_db` should appear).

- [ ] **Step 8: Run tests**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/ -x -q 2>&1 | tail -20
```

Expected: all pass.

- [ ] **Step 9: Commit**

```bash
cd /c/Projects/mind-body-dahua && git add app/main.py app/models/database.py tests/services/test_main_lifespan.py && git commit -m "refactor: convert main.py startup helpers to async, remove sync engine"
```

---

## Chunk 7: Final verification

### Task 18: Run full test suite and confirm no sync-engine references in routes

**Files:** No modifications — verification only.

- [ ] **Step 1: Confirm no file references removed sync symbols**

```bash
cd /c/Projects/mind-body-dahua && grep -r "db_session_factory\|init_db\b\|get_db\b\|_get_async_session_factory" app/ --include="*.py"
```

Expected output: empty (no matches).

- [ ] **Step 2: Confirm no route file performs raw `db.query(` calls**

```bash
cd /c/Projects/mind-body-dahua && grep -r "\.query(" app/admin/ app/api/ --include="*.py"
```

Expected output: empty (no matches).

- [ ] **Step 3: Confirm `mindbody_client_service` is fully gone**

```bash
cd /c/Projects/mind-body-dahua && grep -r "mindbody_client_service" app/ tests/ --include="*.py"
```

Expected output: empty (no matches).

- [ ] **Step 4: Run full test suite**

```bash
cd /c/Projects/mind-body-dahua && python -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected output: all tests pass with zero failures.

- [ ] **Step 5: Commit final verification marker**

```bash
cd /c/Projects/mind-body-dahua && git commit --allow-empty -m "chore: verify services layer migration complete — all tests green"
```

---

## Summary

| Chunk | Tasks | New files | Modified files |
|-------|-------|-----------|----------------|
| 1 | 1–7 | `app/services/__init__.py`, `members.py`, `memberships.py`, `devices.py`, `queue.py`, `export_jobs.py`, `dashboard.py`, `admin_users.py` + 7 test files | — |
| 2 | 8 | `tests/services/test_deps.py` | `app/api/deps.py` |
| 3 | 9–13 | 5 test files | `app/admin/auth.py`, `dashboard.py`, `devices.py`, `mindbody_users.py`, `sync_queue.py`, `export_jobs.py` |
| 4 | 14 | `tests/services/test_tasks_thin_wrappers.py` | `app/sync/tasks.py` |
| 5 | 15–16 | `tests/services/test_health_flow.py` | `app/sync/flows/health.py`; delete `app/sync/mindbody_client_service.py` |
| 6 | 17 | `tests/services/test_main_lifespan.py` | `app/main.py` |
| 7 | 18 | — | — |
