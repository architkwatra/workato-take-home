# Implementation Plan

This document captures the next implementation slices so the plan survives
between sessions.

## Current Branch

Work should happen on feature branches created from latest `origin/main`. Do not
push implementation work directly to `main`.

Current planning branch:

```bash
plan/order-intake-slices
```

## Compose Validation Checkpoint

Attempted local validation from this branch:

```bash
docker --version
docker compose version
docker compose config
```

Result in the current execution environment:

```text
docker: command not found
```

This means Docker Compose has not been verified here yet. Before implementing
pipeline logic, run these commands on a machine with Docker installed:

```bash
docker compose config
docker compose up --build
curl http://localhost:8080/healthz
curl http://localhost:8081/healthz
curl http://localhost:8082/healthz
curl http://localhost:3000
```

Expected result:

- Compose config renders without errors.
- Postgres starts healthy.
- API, downstream simulator, loadgen, worker replicas, and dashboard start.
- Health endpoints return `{"status":"ok", ...}`.
- Dashboard renders the scaffold page.

If Compose fails, fix the scaffold before starting database or API work.

## Next Target

Build idempotent order intake:

- Add migration bootstrap.
- Add minimal schema required for order intake.
- Add a real DB readiness check.
- Add `POST /orders` with idempotency.

The first meaningful behavior should be independently testable with only
Postgres and the API.

## PR Breakdown

### PR 1: Verify and Fix Compose Scaffold

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

### PR 2: Migration Bootstrap

Goal: make schema migrations run predictably before the API starts.

Scope:

- Add Alembic config.
- Add a `migrator` Compose service that runs `alembic upgrade head`.
- Make API depend on successful migration completion.
- Add an initial empty migration if useful to validate the path.

Acceptance checks:

```bash
docker compose up --build migrator
docker compose up --build api postgres
curl http://localhost:8080/readyz
```

Expected: API reports Postgres reachable and migrations can run from a clean DB.

### PR 3: Minimal Order Intake Schema

Goal: add the tables needed to create an order and enqueue first work.

Scope:

- Add enums needed for order intake.
- Add `orders`.
- Add `order_events`.
- Add `order_tasks`.
- Add `workers`, because worker heartbeat depends on this table in PR 6.
- Add indexes and constraints needed for idempotency and basic dashboard reads.

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

### PR 4: `POST /orders` With Idempotency

Goal: prove the first correctness rule in isolation.

Scope:

- Implement `POST /orders`.
- Read idempotency key from `Idempotency-Key` header.
- First request creates:
  - one `orders` row in `placed`
  - one `order_created` event
  - one initial `order_tasks` row
- Duplicate request with the same key returns the existing order.

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
- Database has one order, one creation event, and one initial task for that key.

### PR 5: `downstream_calls` Crash-Recovery Table

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

### PR 6: Basic Worker Loop Without Downstream Calls

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

### Later PRs

- Add durable simulator support tables.
- Add restaurant simulator behavior.
- Add courier simulator behavior.
- Add loadgen control API.
- Add SSE/dashboard live updates.
- Add chaos controls and metrics.
