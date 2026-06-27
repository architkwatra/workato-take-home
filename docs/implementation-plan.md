# Implementation Plan

This document captures the next implementation slices so the plan survives
between sessions.

## Working Rules

Work should happen on feature branches created from latest `origin/main`. Do not
push implementation work directly to `main`.

## Compose Validation Checkpoint

Local Compose validation has passed after the scaffold and schema PRs:

```bash
docker compose version
docker compose config
docker compose up --build
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8081/healthz
curl http://localhost:8082/healthz
curl http://localhost:3000
```

Database validation has also passed:

```bash
docker compose exec postgres psql -U app -d orders -c "select * from alembic_version;"
docker compose exec postgres psql -U app -d orders -c "\dt"
docker compose exec postgres psql -U app -d orders -c "\d orders"
docker compose exec postgres psql -U app -d orders -c "\d order_tasks"
docker compose exec postgres psql -U app -d orders -c "\d order_events"
docker compose exec postgres psql -U app -d orders -c "\d workers"
```

Expected result:

- Compose config renders without errors.
- Postgres starts healthy.
- API, downstream simulator, loadgen, worker replicas, and dashboard start.
- Health endpoints return `{"status":"ok", ...}`.
- Dashboard renders the scaffold page.
- Alembic version is `20260625_0002`.
- Tables exist: `orders`, `order_tasks`, `order_events`, and `workers`.
- `updated_at` fallback triggers exist on `orders` and `order_tasks`.

## Next Target

Order intake and the persistent load generator are in place. The next target is
worker processing: prove that workers can claim durable tasks exactly once,
advance order state, and drain the queue before adding flaky downstream calls.

## Implementation Slice Breakdown

Each slice should become its own focused PR. Slice numbers are planning order,
not GitHub PR numbers.

### Slice 1: Verify and Fix Compose Scaffold

Goal: make `docker compose up --build` work reliably from a fresh checkout.

Scope:

- Fix any Compose/Dockerfile issues found during validation.
- Keep services as scaffolds; no order logic.
- Add any small README corrections needed for run commands.

Acceptance checks:

```bash
docker compose config
docker compose up --build
curl http://localhost:8080/healthz
curl http://localhost:8081/healthz
curl http://localhost:8082/healthz
curl http://localhost:3000
```

### Slice 2: Migration Bootstrap

Goal: make schema migrations run predictably before the API starts.

Scope:

- Add Alembic config.
- Add a `migrator` Compose service that runs `alembic upgrade head`.
- Make the dependency chain explicit:
  - `postgres` becomes healthy.
  - `migrator` runs and exits successfully.
  - `api` and `worker` start after `migrator` completes successfully.
- Add the first real migration instead of an empty one. Create `orders` because
  it has no foreign-key dependencies and validates actual DDL execution.
- Implement `/readyz` as a real Postgres check using `SELECT 1`.
- Remove `downstream-sim`'s Postgres dependency until simulator persistence is
  implemented; it should not block on the DB while it is still a scaffold.

Acceptance checks:

```bash
docker compose up --build migrator
docker compose up --build api worker postgres
curl http://localhost:8080/readyz
docker compose exec postgres psql -U app -d orders -c "\dt orders"
docker compose exec postgres psql -U app -d orders -c "select * from alembic_version;"
```

Expected:

- `migrator` exits successfully.
- `api` and `worker` start only after `migrator` completes successfully.
- `/readyz` reports Postgres reachable.
- `orders` exists.
- `alembic_version` is populated.

### Slice 3: Minimal Order Intake Schema

Goal: add the tables needed to create an order and enqueue first work.

Scope:

- Add remaining enums needed for order intake.
- Add `order_events`.
- Add `order_tasks`.
- Add `workers`, because worker heartbeat depends on this table in Slice 6.
- Add indexes and constraints needed for idempotency and basic dashboard reads.
- Extend `orders` only if Slice 2 kept it intentionally minimal.

Out of scope:

- Worker claiming.
- Downstream calls.
- Simulator persistence.

Acceptance checks:

```bash
docker compose up --build
docker compose exec postgres psql -U app -d orders -c "\dt"
```

Expected: order intake and worker heartbeat tables exist after migrations.

### Slice 4: `POST /orders` With Idempotency

Goal: prove the first correctness rule in isolation.

Break this into smaller quick PRs:

#### Slice 4A: API Foundation

Scope:

- Add shared DB connection/readiness helpers.
- Add shared order lifecycle constants and transition helpers.
- Keep existing API behavior unchanged except reusing the DB helper in
  `/readyz`.

Acceptance checks:

```bash
python3 -m py_compile backend/common/db.py backend/common/state_machine.py backend/api/app.py
curl http://localhost:8080/readyz
```

Expected:

- Compile succeeds.
- `/readyz` still returns Postgres healthy.

#### Slice 4B: Create Order Only

Scope:

- Implement `POST /orders`.
- Read idempotency key from `Idempotency-Key` header.
- Validate request body with required `restaurant_ref` and optional
  `customer_ref`.
- First request creates one `orders` row in `placed`.
- Duplicate request with the same key returns the existing order.
- Do not create `order_events` or `order_tasks` yet.

Acceptance checks:

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Idempotency-Key: demo-order-1' \
  -H 'content-type: application/json' \
  -d '{"restaurant_ref":"restaurant-1","customer_ref":"customer-1"}'

curl -i -X POST http://localhost:8080/orders \
  -H 'Idempotency-Key: demo-order-1' \
  -H 'content-type: application/json' \
  -d '{"restaurant_ref":"restaurant-1","customer_ref":"customer-1"}'
```

Expected:

- First response: `201 Created`.
- Second response: `200 OK`.
- Both responses contain the same order id.
- Database has one order for that idempotency key.

#### Slice 4C: Add Creation Event

Scope:

- Insert one `order_created` row in the same transaction as the new order.
- Duplicate requests must not create another event.

Acceptance checks:

- First create produces one order and one creation event.
- Duplicate create returns the existing order and event count stays one.

#### Slice 4D: Add Initial Task

Scope:

- Insert the first `order_tasks` row in the same transaction:
  - `task_type = advance_state`
  - `target_state = confirmed`
  - `status = pending`
  - `next_run_at = created_at`
- Duplicate requests must not create another task.

Acceptance checks:

- First create produces one order, one creation event, and one initial task.
- Duplicate create returns the existing order and all counts stay one.

#### Slice 4E: API Error/Response Polish

Scope:

- Missing `Idempotency-Key` returns `400`.
- Missing or invalid `restaurant_ref` returns `422`.
- Response shape is documented and stable before loadgen depends on it.

Acceptance checks:

- Happy path and duplicate path still pass.
- Error cases return predictable JSON responses.

### Slice 5: Minimal Loadgen Control API

Goal: make realistic order volume controllable while the system is running.

Why now:

- `POST /orders` now creates the full intake contract: one order, one
  `order_created` event, and one initial pending task with
  `task_type = advance_state` and `target_state = confirmed`.
- Loadgen can test 1-N order intake before workers and downstream simulators add
  more moving parts.
- The original requirement says evaluators should be able to dial load up and
  down during the demo, which means loadgen should be a persistent service, not
  a one-shot CLI job.

Scope:

- Keep loadgen as the existing `loadgen` Docker Compose service.
- Implement a small in-memory controller inside that service:
  - `GET /healthz`
  - `GET /status`
  - `POST /load/start`
  - `POST /load/stop`
  - `PATCH /load/rate`
- Generate HTTP `POST /orders` calls against `API_BASE_URL`.
- Generate a fresh client-side idempotency key for every simulated order.
- Support low-rate and burst-like traffic with:
  - `rate_per_second`
  - optional `max_orders`
  - optional `restaurant_ref`
  - optional `customer_ref_prefix`
- Track simple runtime counters in memory:
  - running/stopped
  - current configured rate
  - started_at/stopped_at
  - attempted order count
  - successful create/reuse count
  - failed request count
  - last_error

Out of scope:

- Dashboard controls. For this slice, use direct HTTP calls to loadgen.
- Durable loadgen history. If the loadgen container restarts, counters reset.
- Sophisticated traffic distributions. Start with a steady rate; richer burst
  profiles can be added later.
- Retrying failed generated requests. The API idempotency behavior is already
  tested directly; loadgen should surface failures rather than hide them.
- Horizontal loadgen scaling. This take-home should use one loadgen service; to
  create more traffic, increase `rate_per_second` instead of adding replicas.

Design notes:

- Loadgen must be a long-running service so rate can be changed while it is
  running.
- The initial `advance_state -> confirmed` row is a durable task, not an order
  state. Loadgen only verifies the task is created. A later worker slice will
  claim that task and move the order from `placed` to `confirmed`.
- The background producer should use `asyncio` and an async HTTP client so it can
  issue requests without blocking the loadgen API.
- `POST /load/start` starts a new run only when no run is active. If a run is
  already active, it returns `409 Conflict` with the current run status and does
  not modify the rate. Operators must use `PATCH /load/rate` for rate changes;
  this avoids a silent no-op or hidden rate update during the demo.
- `POST /load/stop` should stop the background producer gracefully and leave the
  last counters visible through `GET /status`.
- `PATCH /load/rate` should change the rate for the active run without restarting
  the service.
- Rate changes should wake the producer promptly. Use a shared `asyncio.Event`
  that is set by `PATCH /load/rate` and `POST /load/stop`; the producer waits on
  either the next scheduled send time or that event, then recalculates cadence
  from the latest rate. This avoids multi-second stale sleeps and makes
  dinner-rush ramp-up/ramp-down visible quickly.
- `run_id` is generated fresh on each successful `POST /load/start`, not once per
  container lifetime. Idempotency keys should include that run id and sequence
  number, for example `loadgen-{run_id}-{sequence}`. This makes generated
  traffic easy to query in Postgres and avoids accidental key reuse across
  start/stop/start cycles.
- Keep loadgen single-process/in-memory and single-replica by design for this
  assignment. All demo rate control should go through this one service.

Acceptance checks:

```bash
curl -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":5,"max_orders":10,"restaurant_ref":"restaurant-1"}'

curl http://localhost:8082/status

curl -i -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":20,"max_orders":10,"restaurant_ref":"restaurant-1"}'

curl -X PATCH http://localhost:8082/load/rate \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":2}'

curl -X POST http://localhost:8082/load/stop

curl -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":5,"max_orders":10,"restaurant_ref":"restaurant-1"}'
```

Expected:

- Loadgen starts without restarting Docker Compose.
- `GET /status` shows running state and counters while work is active.
- A second `POST /load/start` during an active run returns `409 Conflict`, does
  not change the active run's rate, and does not create a second producer loop.
- Rate can be changed while the producer is running.
- Stop prevents new order requests and status remains readable.
- A new `POST /load/start` after stop creates a fresh `run_id`.
- For a completed `max_orders = 10` run, Postgres shows:
  - 10 generated orders for the run id
  - 10 `order_created` events
  - 10 initial pending tasks with `task_type = advance_state` and
    `target_state = confirmed`
  - no duplicate orders for generated idempotency keys

### Slice 6: Worker DB Pool and Heartbeat

Goal: make each worker visible as an active process before it starts claiming
work.

Scope:

- Configure the shared Postgres pool in the worker startup path and close it on
  shutdown.
- Generate `worker_id = worker-{uuid4}` once at worker process startup. Do not
  use hostname as the primary id because Compose replica hostnames are
  restart-scoped and can leave stale identities behind.
- Upsert into `workers` every 10 seconds with `worker_id`, `last_seen_at`,
  `started_at`, `hostname`, and metadata.
- Keep the worker from claiming tasks in this slice.

Design notes:

- Add comments explaining that worker heartbeat measures liveness, while task
  leases measure active work.
- `worker_id` only needs to be stable for the process lifetime. The dashboard
  should count active workers with `last_seen_at >= now() - interval '30
  seconds'`, so old rows from previous container starts do not count as active.
- Keep heartbeat failure loud in logs, but do not crash the worker on one failed
  heartbeat. A temporary DB hiccup should be visible and retried.

Acceptance checks:

```bash
docker compose up --build -d
docker compose exec postgres psql -U app -d orders -c \
  "select worker_id, hostname, last_seen_at from workers where last_seen_at >= now() - interval '30 seconds' order by worker_id;"
```

Expected:

- Three active worker rows appear within 30 seconds.
- `last_seen_at` advances while workers are idle.
- Stopping one worker makes its heartbeat go stale while the others continue.

### Slice 7: Claim and Process Initial `placed -> confirmed` Tasks

Goal: prove multiple workers can compete for work, claim each task once, and
move accepted orders one real lifecycle step.

Scope:

- Add a worker task-claim helper that selects one eligible task inside a
  transaction with `SELECT ... FOR UPDATE SKIP LOCKED`.
- Eligible tasks are:
  - `pending` with `next_run_at <= now()`;
  - `running` with `locked_until < now()` for expired-lease recovery.
- Claiming updates the task to `running`, sets `locked_by`, sets
  `locked_until = now() + interval '30 seconds'`, and updates `updated_at`.
- Handle only `task_type = advance_state` with `target_state = confirmed`.
- In one transaction, the worker:
  - loads the task and its order;
  - verifies the order is still in `placed`;
  - updates `orders.state` to `confirmed` with `WHERE id = ? AND version = ?`;
  - increments `orders.version`;
  - inserts an `order_events` row with `event_type = state_transition`,
    `from_state = placed`, `to_state = confirmed`, `task_id = <current task
    id>`, `worker_id = <this worker id>`, and the same `occurred_at` timestamp
    used for the order/task updates;
  - marks the task `completed` with `completed_at`.
- If the order is terminal, mark the task `completed` as a safe no-op.
- If the optimistic update affects zero rows, mark the task `completed` as a
  stale no-op because another actor won the race.

Design notes:

- `SKIP LOCKED` prevents two live workers from seeing the same claimable row.
- The 30-second lease is the crash recovery window, not the expected processing
  duration.
- Worker polling should use a short active interval and idle backoff: poll
  immediately after completing a task, sleep 100ms after a no-work result, and
  exponentially back off to a maximum 1s idle sleep. Reset the delay to 100ms
  whenever work is found.
- Use existing constants from `common.state_machine`, `common.task_types`, and
  `common.event_types`; do not add duplicate raw strings.
- The optimistic-locking comment should explain that `orders.version` protects
  against stale workers, cancellation races, and expired leases.
- Add comments around the claim query; this is the first concurrency-sensitive
  worker code.
- Do not call `downstream-sim` yet. This slice proves the DB worker loop only.

Acceptance checks:

```bash
curl -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":10,"max_orders":10,"restaurant_ref":"restaurant-1"}'

docker compose exec postgres psql -U app -d orders -c \
  "select state, count(*) from orders group by state order by state;"

docker compose exec postgres psql -U app -d orders -c \
  "select task_type, target_state, status, count(*) from order_tasks group by task_type, target_state, status order by task_type, target_state, status;"

docker compose exec postgres psql -U app -d orders -c \
  "select event_type, from_state, to_state, count(*) from order_events group by event_type, from_state, to_state order by event_type, from_state, to_state;"
```

Expected:

- The 10 generated orders move from `placed` to `confirmed`.
- The 10 initial `advance_state -> confirmed` tasks become `completed`.
- The event table has 10 `order_created` events and 10
  `state_transition placed -> confirmed` events for the run.

### Slice 8: Insert the Next Fake Advance Task

Goal: keep the pipeline moving one task at a time without downstream calls.

Scope:

- After completing `placed -> confirmed`, insert the next active task:
  `task_type = advance_state`, `target_state = preparing`.
- Use the existing active `dedupe_key` pattern:
  `{order_id}:{task_type}:{target_state}`.
- Insert the next task in the same transaction as the order transition and task
  completion.

Design notes:

- Inserting the next task only after the guarded state update succeeds prevents
  a stale worker from scheduling future work for an order it did not advance.
- Duplicate next-task insertion should be prevented by the active
  `dedupe_key` unique index.

Acceptance checks:

- Create 10 orders through loadgen.
- Verify orders reach `preparing`.
- Verify each order has exactly one completed `advance_state -> confirmed` task
  and one `advance_state -> preparing` task that is completed or pending,
  depending on how quickly workers drain.

### Slice 9: Complete Fake Lifecycle Without Downstream Calls

Goal: prove the full worker loop and dashboard-ready event trail before adding
flaky downstream systems.

Scope:

- Extend fake `advance_state` handling through the happy path:
  - `confirmed -> preparing`
  - `preparing -> ready`
  - `ready -> out_for_delivery`
  - `out_for_delivery -> delivered`
- Each transition writes one `state_transition` event, completes the current
  task, and inserts the next task until `delivered`.
- Do not create `check_ready`, `check_pickup`, or `check_delivery` tasks yet.
  Those belong to simulator-backed behavior.

Acceptance checks:

- Create a bounded loadgen run.
- Verify all generated orders eventually reach `delivered`.
- Verify queue depth drains to zero pending/running tasks for that run.
- Verify transition events exist in lifecycle order for each generated order.

Simulator handoff:

- Treat this fake lifecycle as disposable demo scaffolding. Before the first
  simulator-backed worker slice, clear local demo data with `docker compose down
  -v` or an explicit truncate script so no pending fake `advance_state -> ready`,
  `advance_state -> out_for_delivery`, or `advance_state -> delivered` tasks
  remain.
- At that handoff, remove direct fake handling for `advance_state -> ready`,
  `advance_state -> out_for_delivery`, and `advance_state -> delivered`.
  Replace it with simulator-backed flow: prep start schedules `check_ready`,
  courier dispatch advances to `out_for_delivery` and schedules `check_pickup`,
  pickup records a `courier_picked_up` milestone, and `check_delivery` advances
  the order to `delivered`.

### Slice 10: `downstream_calls` Crash-Recovery Table

Goal: add the pipeline-side idempotency table before any real downstream
simulator behavior depends on it.

Scope:

- Add `downstream_calls`.
- Add `downstream_action` and `downstream_call_status` enums if not already
  present.
- Add unique constraint on `idempotency_key`.
- Add helper code or tests for `INSERT ... ON CONFLICT (idempotency_key) DO
  UPDATE` semantics.

Out of scope:

- Real downstream HTTP calls.
- Restaurant/courier simulator behavior.

Acceptance checks:

- Insert a downstream call with an idempotency key.
- Upsert the same key again and verify it updates/reuses the existing row.
- Verify a mismatched `request_hash` is treated as an error path.

### Later Slices

- Add durable simulator support tables.
- Add restaurant simulator behavior.
- Add courier simulator behavior.
- Add dashboard controls for loadgen and chaos settings.
- Add SSE/dashboard live updates.
- Add chaos controls and metrics.
