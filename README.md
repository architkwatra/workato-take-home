# Workato Take-Home: Order Pipeline

This project will model a food-delivery order pipeline from `placed` to
`delivered`, including bursty traffic, flaky restaurant/courier integrations,
live operations visibility, and failure recovery.

See [Design Doc](docs/design.md) for the high-level system design and
[Implementation Plan](docs/implementation-plan.md) for the next build slices.

## Current Scaffold

The repository currently contains the first runnable slice: Docker Compose plus
empty service frames. These containers start and expose basic health/status
responses, but the order pipeline logic is not implemented yet.

Run:

```bash
docker compose up --build
```

Services:

- API: `http://localhost:8080/healthz`
- Downstream simulator: `http://localhost:8081/healthz`
- Load generator: `http://localhost:8082/healthz`
- Dashboard: `http://localhost:3000`
- Postgres: `localhost:5432`

## What the System Should Prove

The goal is not to build a perfect production platform. The goal is to show a
working local system with sound engineering judgment:

- Orders can enter the system at high volume.
- Each order moves through a valid lifecycle in order.
- Flaky downstream systems can fail, recover, and slow down without losing
  orders.
- Duplicate work is handled safely through idempotency and state checks.
- Operators can see throughput, failures, stuck orders, retries, and system
  health in real time.
- The demo can trigger normal traffic, a dinner rush, and intentional failures.

## Proposed Local Architecture

Use Docker Compose to run everything on one machine:

- `api`: accepts orders, exposes control endpoints, serves health/readiness, and
  streams live updates to the dashboard from Postgres notifications.
- `worker`: advances orders through the lifecycle by claiming pending work from
  the database. Run multiple replicas locally so killing one worker does not stop
  processing.
- `postgres`: durable source of truth for orders, lifecycle events, attempts,
  and worker leases.
- `downstream-sim`: fake restaurant and courier services with configurable
  latency, random failures, rate limits, and recovery.
- `dashboard`: web UI for the operations/business view.
- `loadgen`: persistent traffic simulator that starts idle, exposes a control
  API, and can change order rate while running.
- `prometheus`/`grafana` or a lightweight metrics endpoint: health and runtime
  observability.

Chosen implementation stack:

- Backend/workers/load generator: Python with FastAPI, asyncio, SQLAlchemy,
  Alembic, and psycopg.
- Database: Postgres.
- Frontend: React/Vite.
- Local orchestration: Docker Compose.

`docker-compose.yml` should set the project name to `workato-take-home`, run
three worker replicas by default with `deploy.replicas: 3`, and avoid a fixed
`container_name` for the worker service so scaling works. Use
`docker compose up --scale worker=1` only when debugging a single worker.

I would keep Postgres as the source of truth instead of relying on an in-memory
queue. For a take-home, a durable `order_tasks` table with worker leases is
simple, inspectable, and defensible. If a worker dies mid-order, another worker
can reclaim the task after the lease expires.

## Order Lifecycle

Primary states:

```text
placed -> confirmed -> preparing -> ready -> out_for_delivery -> delivered
```

Terminal/exception states:

```text
cancelled
failed
```

The state machine should be explicit in code. Workers should only perform valid
transitions, and the database should store every transition as an event so the
dashboard and demo can explain what happened.

## Correctness Model

The system should assume at-least-once processing. Exactly-once delivery is not
realistic across flaky systems, so correctness comes from durable state,
idempotency, and guarded transitions.

Core rules:

- The client/load generator creates a UUID idempotency key for each intended
  order and sends it with the request. The API stores it with a unique
  constraint and returns the existing order if the same key is retried with the
  same request body. Reusing the key with a different body returns `409`.
- Every lifecycle change is written transactionally.
- Workers claim work with a 30-second lease, for example `locked_by` and
  `locked_until`, so tasks from a killed worker become reclaimable quickly during
  the demo.
- Task claiming uses `SELECT FOR UPDATE SKIP LOCKED` inside a transaction, so
  concurrent workers never claim the same task row. Only one worker wins the
  claim; others skip to the next available task.
- Expired leases are recoverable by another worker.
- Tasks retry up to 5 times by default, configurable per stage. After that, the
  task is marked `failed`, while the order remains in its source state until an
  explicit operator retry or a later order-failure policy handles it.
- Expected poll results such as `not_ready`, `not_picked_up`, or `not_delivered`
  are not errors and do not increment attempts; they only move `next_run_at`.
- Rate limit responses (`429`) are handled separately from normal transient
  errors. If the downstream service returns `Retry-After`, the worker schedules
  the next attempt for that time instead of applying standard exponential
  backoff. This prevents the system from hammering a rate-limited service.
- Downstream calls include idempotency keys so repeated calls do not create
  duplicate restaurant confirmations or courier dispatches.
- Before doing work, a worker checks the current order state. If the order has
  already moved on, the duplicate task becomes a no-op.

This means the system can process some work more than once internally, but it
should not produce duplicate business effects.

### Preventing Duplicate Business Effects

The specific failure to avoid is dispatching two couriers, confirming the same
restaurant order twice, or charging twice if payment were included later.

Every downstream call carries a stable idempotency key derived from
`order_id` and the business action, for example
`courier_dispatch:{order_id}` or `restaurant_confirm:{order_id}`. The
downstream simulator stores the first result for that key and returns the same
result for duplicate calls instead of creating a second dispatch or
confirmation.

If a downstream connection drops mid-request, the worker cannot know whether the
restaurant or courier received the call. The worker still retries, but it retries
with the same idempotency key. If the first request reached the downstream
system, the duplicate request returns the already-created result. If it did not,
the retry creates the result once.

### Cancellation and Stale Work

When an order is cancelled, outstanding tasks for that order are no longer
eligible for useful work. A worker checks the latest order state before making a
downstream call; if the order is already terminal (`cancelled`, `failed`, or
`delivered`), the task is treated as a no-op.

If cancellation happens while a downstream request is already in flight, the
response is ignored unless the order is still in the expected state. The order
remains cancelled, and any follow-up task insertion is skipped. Optimistic
locking enforces this: cancellation increments `orders.version`, so a stale
worker's `UPDATE ... WHERE version = ?` affects zero rows.

## Suggested Data Model

High-level tables:

- `orders`: current state, customer/restaurant/courier metadata, timestamps,
  version, terminal reason.
- `order_events`: append-only history of state transitions and milestone events
  with `event_type`, optional `from_state`/`to_state`, `occurred_at`, and
  metadata, so stage duration can be computed from consecutive events.
- `order_tasks`: durable work queue with order id, target stage, attempts,
  status, next run time, lease owner, lease expiry, and `task_type` such as
  `advance_state`, `check_ready`, `check_pickup`, or `check_delivery`.
- `downstream_calls`: idempotency records for restaurant/courier requests and
  responses.
- `workers`: worker heartbeat rows with `worker_id` and `last_seen_at`.

The dashboard can read current status from `orders` and historical flow from
`order_events`.

The `orders.version` column supports optimistic locking. A worker reads the
current version and includes it in `UPDATE orders ... WHERE id = ? AND version = ?`.
If another worker or cancellation already updated the row, the update affects
zero rows and the worker retries or discards the stale task.

`SELECT FOR UPDATE SKIP LOCKED` and optimistic locking protect different cases:
the first prevents two workers from claiming the same task at once; the second
prevents a stale worker from overwriting a newer state after its lease expired.

`downstream_calls` does not need to grow forever. A retention job can prune
successful idempotency records after a safe window, for example several days,
while keeping failed or disputed calls longer for debugging. If a zombie worker
retries after that retention window, idempotency protection is gone; this is an
acceptable demo limitation and would need a production retention policy.

## Pipeline Flow

1. The load generator or API creates an order.
2. The API stores the order as `placed` and inserts the first task.
3. A worker claims the next eligible task.
4. The worker calls the required downstream simulator if the stage needs it.
5. On success, the worker advances the order and inserts the next task. If the
   next step is waiting on slow external progress, it inserts a future task with
   `next_run_at` and releases the lease.
6. On transient failure, the worker records the attempt and schedules a retry.
7. On repeated failure, the task is marked `failed` and can be reopened through
   the operator retry endpoint. Invalid or stale work is completed as a safe
   no-op.

Manual recovery endpoint:

- `POST /orders/{order_id}/tasks/retry-failed` resets failed task rows for that
  order back to `pending` without changing the order state.

Important downstream handoffs:

- Restaurant simulator: confirmation advances `placed -> confirmed`, preparation
  start advances `confirmed -> preparing`, then the worker inserts a future
  `check_ready` task. Later workers claim that task, check readiness once, and
  either advance `preparing -> ready` or reschedule another check without
  incrementing attempts.
- Courier simulator: assignment advances `ready -> out_for_delivery` and inserts
  a future `check_pickup` task. When pickup happens, the worker appends a
  `courier_picked_up` milestone event while the order state remains
  `out_for_delivery`, then schedules `check_delivery`; delivery advances
  `out_for_delivery -> delivered`.

## Dashboard

The dashboard should be useful during a dinner rush, not just pretty.

Show:

- Total orders by state.
- Orders per minute and delivered per minute.
- Average and p95 time in pipeline.
- Retry counts and failure counts.
- Oldest stuck orders.
- Downstream health: restaurant/courier latency, error rate, and rate-limit
  responses.
- Worker health: active workers, queue depth, lease expirations, processing
  rate.

Workers heartbeat every 10 seconds into the `workers` table. The dashboard
counts active workers as those seen within the last 30 seconds, which includes
idle workers that are not currently holding a task lease.

The UI should update through Server-Sent Events. Workers write `order_events`
and issue `pg_notify` with only `order_id` and event type. The API listens with
Postgres `LISTEN/NOTIFY`, fetches current state from Postgres, then pushes the
SSE event to connected dashboard clients.

If the SSE stream disconnects, the dashboard should reconnect automatically. On
reconnect it should fetch a fresh REST snapshot before resuming the stream, so a
dropped browser connection does not leave stale data on screen.

After a dinner-rush burst ends, the dashboard should make the recovery arc
visible: queue depth peaks and then drains, orders-per-minute returns to
baseline, downstream error rate normalizes, and p95 time-in-stage falls as
workers catch up.

## Load Generation

The load generator should be a long-running service started by
`docker compose up`, not a one-shot job. It starts idle, creates a fresh
client-side idempotency key for each intended order, and posts orders to the API
at the configured rate.

The dashboard is the primary control surface. It calls API load-control
endpoints, and the API forwards those commands to the internal `loadgen`
service. CLI commands are just curl wrappers around the same controls.

The load generator should support:

- Steady traffic: small number of orders per second.
- Dinner rush: sharp burst of orders that can be dialed up and back down during
  the demo.
- Promotion mode: sustained high load.
- Start, stop, and live rate changes without restarting the service.

Example future commands:

```bash
docker compose up
curl -X POST http://localhost:8080/admin/load/start \
  -H 'content-type: application/json' \
  -d '{"ratePerSecond":5,"profile":"steady"}'
curl -X PATCH http://localhost:8080/admin/load/rate \
  -H 'content-type: application/json' \
  -d '{"ratePerSecond":100}'
curl -X PATCH http://localhost:8080/admin/load/rate \
  -H 'content-type: application/json' \
  -d '{"ratePerSecond":5}'
curl -X POST http://localhost:8080/admin/load/stop
```

## Failure Demo

The demo should make failures easy to trigger:

- Dashboard: increase restaurant latency, random restaurant failures, courier
  rate limits, and downstream recovery.
- CLI: kill one worker replica while orders are in flight. Other replicas keep
  working and reclaim the killed worker's tasks after the 30-second lease
  expires. This is the only process-level failure action; dashboard chaos
  controls are for downstream behavior. With the fixed Compose project name, the
  demo command can be `docker stop workato-take-home-worker-3`.

Expected behavior:

- Orders already stored in Postgres remain visible.
- In-flight leased tasks become retryable after the lease expires.
- Failed tasks stay terminal until an operator explicitly resets them.
- Duplicate processing attempts do not create duplicate business effects.
- The dashboard shows backlog growth, retries, failures, and recovery.

## Nice Touches

To make the demo feel like an operations tool instead of a raw engineering
console, build a few focused product touches:

- Chaos controls in the dashboard for restaurant failures, courier rate limits,
  downstream latency, and recovery.
- Per-stage p95 latency from `order_events`; pickup is a milestone event while
  the order state remains `out_for_delivery`, so its timing is computed from
  event timestamps rather than state-entry/state-exit pairs.
- Stuck-order highlighting for orders that have not moved in more than a
  configurable threshold.
- A live delivery ETA estimate that updates from current queue depth and recent
  stage latencies.

## Observability

Expose health and metrics:

- `/healthz`: process is alive.
- `/readyz`: dependencies are reachable.
- `/metrics`: counters, gauges, and histograms.

Useful metrics:

- Orders created/delivered/failed.
- Queue depth by stage.
- Task attempts and retries.
- Worker claim rate and completion rate.
- Downstream latency and error rate.
- Lease expirations and recovered tasks.

Logs should include `order_id`, `task_id`, `stage`, and `attempt` so a single
order can be traced during the walkthrough.

## Trade-Offs to Defend

- Postgres-backed queue is simpler and durable, but less horizontally scalable
  than Kafka/RabbitMQ. That is acceptable for a single-machine take-home.
- At-least-once processing is realistic. Idempotency and state checks prevent
  duplicate business effects.
- SSE is simpler than WebSockets for a live dashboard if the UI only needs
  server-to-client updates.
- Simulated downstream systems should be controllably unreliable so the demo can
  show recovery, not just random chaos.

## Final README Shape

This file is currently an implementation blueprint. Before submission, rewrite
the README around the evaluator workflow:

1. Quick Start
2. How to Drive Load
3. How to Trigger Failures
4. Architecture Summary
5. Trade-offs

Most of this planning content should move into the architecture and trade-off
sections once the actual commands and endpoints exist.

## Implementation Order

1. Define order states, database schema, migrations, and `downstream_calls`
   idempotency records.
2. Build order intake API and basic order creation.
3. Build worker task claiming, leases, retries, heartbeats, and lifecycle
   transitions.
4. Add restaurant/courier simulators with failure controls.
5. Add persistent load generator service and dashboard/API controls for live
   rate changes.
6. Wire the SSE stream and REST snapshot path used by the dashboard.
7. Build dashboard with live metrics and order state views.
8. Add health/metrics endpoints and demo failure controls.
9. Write final README instructions for running, load testing, and failure demos.
