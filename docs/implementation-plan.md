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

Build idempotent order intake. Keep this split into small PRs so each step is
independently reviewable and testable.

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

### Slice 5: `downstream_calls` Crash-Recovery Table

Goal: add the pipeline-side idempotency table before any downstream simulator
behavior depends on it.

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

### Slice 6: Basic Worker Loop Without Downstream Calls

Goal: prove task claiming and lifecycle movement before adding external
complexity.

Scope:

- Worker heartbeat.
- `SELECT ... FOR UPDATE SKIP LOCKED` task claiming.
- Lease creation and renewal.
- Direct fake lifecycle transitions with no simulator calls.
- Multiple worker replicas.

Acceptance checks:

- Create several orders.
- Verify each task is claimed by only one worker.
- Verify orders advance through the fake lifecycle.
- Verify queue depth drains.

### Later Slices

- Add durable simulator support tables.
- Add restaurant simulator behavior.
- Add courier simulator behavior.
- Add loadgen control API.
- Add SSE/dashboard live updates.
- Add chaos controls and metrics.
