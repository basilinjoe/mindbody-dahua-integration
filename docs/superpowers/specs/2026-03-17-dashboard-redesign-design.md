# Dashboard Redesign — Design Spec

**Date:** 2026-03-17
**Status:** Approved (mockup sign-off)

---

## Overview

Redesign the admin dashboard (`/admin/dashboard`) to surface operational health at a glance: device user counts, sync queue status, failures, and MindBody member metrics. The current dashboard shows stale stats derived from the removed `SyncedMember` model; this replaces it with live data from `DahuaDevice`, `DahuaSyncQueue`, `MindBodyClient`, and `MindBodyMembership`.

---

## Layout

**KPI Row + Sections** (chosen over Three-Column and Alert-First layouts).

```
[ alert banner — shown only when failures exist ]
[ stat card ][ stat card ][ stat card ][ stat card ][ stat card ]
[ device breakdown table              ][ recent activity     ]
                                       [ mindbody breakdown  ]
```

---

## Components

### 1. Failure Alert Banner

- Shown only when `DahuaSyncQueue` has items with `status='failed'` in the last 24 hours.
- Summarises count and affected devices.
- Links to `/admin/sync-queue?status=failed`.
- Dismissed by navigating away (no server-side dismiss state needed).

### 2. Stat Cards (top row — 5 cards)

| Card | Source | Notes |
|---|---|---|
| Total Members | `MindBodyClient.count()` | All rows |
| Active Subscriptions | `MindBodyClient` with correlated `MindBodyMembership.is_active` EXISTS | Shows count + % of total |
| Pending Queue | `DahuaSyncQueue.count(status='pending')` | |
| Failed (24h) | `DahuaSyncQueue.count(status='failed', created_at >= now-24h)` | Shows success rate % alongside |
| Devices Online | `DahuaDevice` enabled count vs. `status='online'` count | X/Y format |

### 3. Device Breakdown Table

One row per enabled `DahuaDevice`. Columns:

- **Device name** — `DahuaDevice.name`
- **Gate type badge** — derived from `DahuaDevice.gate_type` (`male` / `female` / `all`)
- **Status** — `DahuaDevice.status` (online / offline dot)
- **Users on device** — total count of all users enrolled on the physical device as returned by `DahuaClient.get_all_users()` (not filtered to DB records); fetched asynchronously via HTMX, shows spinner until loaded
- **Pending** — `DahuaSyncQueue.count(device_id=X, status='pending')`
- **Failed** — `DahuaSyncQueue.count(device_id=X, status='failed', created_at >= now-24h)`
- **Last seen** — `DahuaDevice.last_seen_at` formatted as relative time

Live user count endpoint: `GET /admin/devices/{id}/user-count` returns an HTML fragment (integer or `—` if offline).

### 4. Recent Activity Feed

Last 10 `DahuaSyncQueue` items ordered by `created_at DESC`. Shows:

- Action badge (`enroll` / `reactivate` / `deactivate`) — derived from `DahuaSyncQueue.action`; a red `failed` badge is shown when `status='failed'` regardless of action
- Client ID (`DahuaSyncQueue.mindbody_client_id`) + device name (resolved by joining `DahuaDevice` on `device_id` in the route query)
- Relative timestamp
- Status dot (green = success, red = failed, amber = pending)

### 5. MindBody Breakdown Panel

Stacked below the activity feed in the right column:

- Active subscription bar (count + %)
- No-subscription bar (count + %)
- Gender split: male count + bar, female count + bar. Derived by grouping `MindBodyClient.gender` on the known values `'Male'` and `'Female'`; rows with `None` or any other value are excluded. Bar widths are percentages of total members (not just members with known gender).

All derived from `MindBodyClient` and `MindBodyMembership` (same queries as stat cards, broken down further).

---

## Data Loading Strategy

- **All stat cards and table metadata** (device name, status, pending/failed counts, MindBody breakdown) load synchronously on page render — fast DB queries, no external calls.
- **Live user counts per device** load asynchronously via HTMX after the page renders. Each device row has an `hx-get` pointing to `/admin/devices/{id}/user-count` with `hx-trigger="load"`. This prevents one slow/offline device from blocking the whole page.

---

## Backend Changes

### `app/admin/dashboard.py`

- Extend `_get_stats()` to add `failed_24h` and per-device pending/failed counts.
- Extract `_get_recent_queue(db)` helper from the inline query currently in the route handler; extend it to join `DahuaDevice` so device names are available in the template.
- Add `_get_mb_breakdown()` returning gender counts and subscription breakdown.
- Pass all data to the template context.

### `app/admin/devices.py`

- Add `GET /admin/devices/{id}/user-count` route returning an HTMX partial with the live user count (or `—` for offline devices). Reuses `DahuaClient.get_all_users()`.

### Templates

- `app/templates/dashboard.html` — full rewrite matching the mockup layout.
- `app/templates/partials/device_user_count.html` — new HTMX partial fragment.
- `app/templates/partials/stats.html` — updated to match new 5-card layout.

---

## Error Handling

- If a Dahua device is unreachable during the async user-count fetch, the partial returns `—` with an amber offline indicator. The rest of the page is unaffected.
- If `DahuaDevice.status` is `'offline'`, skip the API call and return `—` immediately (no network call made).
- If the `device_id` is not found in the DB, the endpoint returns an HTML fragment containing `—` (not a 404) so HTMX can safely insert it into the DOM without error.
- The `hx-get` attribute in the template must use the full resolved URL path: `/admin/devices/{id}/user-count` — confirming this matches the router prefix (`/admin`) + device router prefix (`/devices`) + route path (`/{id}/user-count`).

---

## Testing

- `tests/unit/admin/test_dashboard_stats.py` — extend to cover `failed_24h` count and MindBody breakdown queries.
- `tests/unit/admin/test_device_user_count.py` — new file; test the async user-count endpoint with mocked `DahuaClient`.
- Existing dashboard route tests updated to assert new template context keys.

---

## Out of Scope

- Charts / time-series data (future iteration).
- Dismiss-state for alert banners.
- Pagination of the activity feed (last 10 is sufficient for an ops view).
