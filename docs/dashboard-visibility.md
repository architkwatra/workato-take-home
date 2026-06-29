# Dashboard Visibility Plan

## Goal

Build the first useful operations dashboard for the local order pipeline.

The dashboard should make the current system testable at a glance:

- how many orders are in each lifecycle state;
- whether workers are alive;
- whether tasks are pending, running, completed, or failed;
- which recent orders moved through the pipeline;
- which recent events explain what happened;
- which failed or delayed tasks need attention.

This is not a marketing page. It should be a dense, work-focused operations
screen for local demos and debugging.

## Current Baseline

The backend now supports the full happy-path lifecycle:

```text
placed -> confirmed -> preparing -> ready -> out_for_delivery -> delivered
```

The database already stores the data needed for visibility:

- `orders` stores current lifecycle state and key refs;
- `order_tasks` stores durable work, attempts, leases, deadlines, and errors;
- `order_events` stores state transitions and retry/operator events;
- `workers` stores worker heartbeat rows.

The frontend is still a scaffold and does not read live data yet.

## Proposed Scope

### API

Add one read-only endpoint first:

```http
GET /dashboard/overview
```

Response shape should be purpose-built for the dashboard and avoid frontend
joining logic:

```json
{
  "generated_at": "...",
  "orders": {
    "total": 123,
    "by_state": {
      "placed": 1,
      "confirmed": 2,
      "preparing": 3,
      "ready": 4,
      "out_for_delivery": 5,
      "delivered": 108,
      "cancelled": 0,
      "failed": 0
    }
  },
  "tasks": {
    "by_status": {
      "pending": 2,
      "running": 1,
      "completed": 300,
      "failed": 0,
      "cancelled": 0
    },
    "due_pending": 1,
    "expired_running": 0,
    "problem_tasks": []
  },
  "workers": {
    "active_count": 3,
    "total_seen": 3,
    "rows": []
  },
  "recent_orders": [],
  "recent_events": []
}
```

Implementation notes:

- Use a new `backend/api/dashboard_store.py` module for SQL queries.
- Keep the route handler thin in `backend/api/app.py`.
- Return all known order states and task statuses even when the count is zero.
- Limit recent rows to a small number, such as 12 orders and 20 events.
- Include failed tasks, expired running tasks, and error-bearing due pending
  tasks in `problem_tasks`.
- Add comments around dashboard query choices, especially why we denormalize the
  response for the frontend and how worker activity is calculated.

### CORS

The browser dashboard runs on:

```text
http://localhost:3000
```

The API runs on:

```text
http://localhost:8080
```

So the API needs constrained CORS for the dashboard origin.

Add:

```text
DASHBOARD_CORS_ORIGINS=http://localhost:3000
```

and configure FastAPI `CORSMiddleware` from that env var. Do not use wildcard
CORS by default.

### Frontend

Replace the scaffold React app with a live operational dashboard.

First screen sections:

1. Header strip
   - API connection state
   - last refresh time
   - active worker count

2. Lifecycle overview
   - state counts for the full order lifecycle;
   - visual bars sized by count;
   - terminal states clearly separated.

3. Task health
   - task status counts;
   - due pending count;
   - expired running count;
   - failed count.

4. Worker heartbeats
   - worker id;
   - hostname;
   - last seen age;
   - active/stale status.

5. Recent orders
   - idempotency key;
   - state;
   - restaurant ref;
   - courier ref presence;
   - updated time.

6. Recent events
   - event type;
   - state transition;
   - order id/idempotency key;
   - task id presence;
   - worker id presence;
   - occurred time.

7. Problem tasks
   - failed tasks;
   - expired running tasks;
   - due pending tasks with `last_error`;
   - attempts and last error.

Frontend behavior:

- Poll `GET /dashboard/overview` every 3 seconds.
- Keep the last successful payload visible if a refresh fails.
- Show a compact error state when the API cannot be reached.
- Avoid mock data.
- Avoid large hero/marketing layout.
- Keep the layout usable on desktop and mobile.

### Styling

Use a restrained operations-dashboard style:

- dense but readable;
- neutral background;
- clear state colors;
- no gradient/orb decoration;
- no nested cards;
- tables should scroll horizontally on small screens rather than overlapping;
- text should not resize with viewport width.

Suggested state colors:

- `delivered`: green;
- `failed`/`cancelled`: red;
- `out_for_delivery`: blue;
- `ready`: teal;
- active/in-progress states: amber or neutral.

## Non-Goals

- No loadgen start/stop controls in this slice.
- No downstream-sim failure controls in this slice.
- No websocket or Postgres `LISTEN/NOTIFY` yet.
- No authentication.
- No editing orders from the dashboard.
- No charting library dependency.

Those can be added after the read-only dashboard proves useful.

## Verification Plan

Static:

```bash
PYTHONPYCACHEPREFIX=/private/tmp/workato-pycache-dashboard python3 -m py_compile \
  backend/api/*.py \
  backend/common/*.py \
  backend/downstream_sim/*.py \
  backend/loadgen/*.py \
  backend/worker/*.py

git diff --check
```

Frontend:

```bash
npm --prefix frontend run build
```

Runtime:

```bash
docker compose up --build -d
curl -fsS http://localhost:8080/dashboard/overview
```

Then run a small loadgen test:

```bash
curl -fsS -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":10,"max_orders":5,"restaurant_ref":"dashboard-demo"}'
```

Verify:

- dashboard loads at `http://localhost:3000`;
- order state counts change while the run progresses;
- final delivered count increases;
- task counts show completed lifecycle tasks;
- worker heartbeat section shows active workers;
- recent events show all transition types;
- problem task section stays empty on happy path.

Failure visibility check:

- stop `downstream-sim`;
- start a small loadgen run;
- verify orders/tasks show stuck or failed work;
- restart `downstream-sim`;
- use the retry-failed endpoint if tasks exhausted retries;
- verify dashboard reflects recovery.

## Later Dashboard Slices

After this read-only visibility slice:

- add loadgen controls;
- add downstream-sim failure toggles;
- add failed-task retry buttons;
- add per-order detail drawer/page;
- add live updates with Postgres notifications or server-sent events.
