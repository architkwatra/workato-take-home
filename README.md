# Worksto Take-Home: Order Pipeline

This project will model a food-delivery order pipeline from `placed` to
`delivered`, including bursty traffic, flaky restaurant/courier integrations,
live operations visibility, and failure recovery.

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
  streams live updates to the dashboard.
- `worker`: advances orders through the lifecycle by claiming pending work from
  the database.
- `postgres`: durable source of truth for orders, lifecycle events, attempts,
  and worker leases.
- `downstream-sim`: fake restaurant and courier services with configurable
  latency, random failures, rate limits, and recovery.
- `dashboard`: web UI for the operations/business view.
- `loadgen`: controllable traffic simulator for normal traffic and dinner rush.
- `prometheus`/`grafana` or a lightweight metrics endpoint: health and runtime
  observability.

Chosen implementation stack:

- Backend/workers/load generator: TypeScript with Node.js and Fastify.
- Database: Postgres.
- Frontend: React/Vite.
- Local orchestration: Docker Compose.

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

- Every submitted order has a stable `order_id` or idempotency key with a unique
  constraint.
- Every lifecycle change is written transactionally.
- Workers claim work with a lease, for example `locked_by` and `locked_until`.
- Task claiming uses `SELECT FOR UPDATE SKIP LOCKED` inside a transaction, so
  concurrent workers never claim the same task row. Only one worker wins the
  claim; others skip to the next available task.
- Expired leases are recoverable by another worker.
- Retries use exponential backoff and max-attempt limits.
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
remains cancelled, and any follow-up task insertion is skipped.

## Suggested Data Model

High-level tables:

- `orders`: current state, customer/restaurant/courier metadata, timestamps,
  version, terminal reason.
- `order_events`: append-only history of state changes and notable failures.
- `order_tasks`: durable work queue with order id, target stage, attempts,
  status, next run time, lease owner, and lease expiry.
- `downstream_calls`: idempotency records for restaurant/courier requests and
  responses.

The dashboard can read current status from `orders` and historical flow from
`order_events`.

The `orders.version` column supports optimistic locking. A worker reads the
current version and includes it in `UPDATE orders ... WHERE id = ? AND version = ?`.
If another worker or cancellation already updated the row, the update affects
zero rows and the worker retries or discards the stale task.

`downstream_calls` does not need to grow forever. A retention job can prune
successful idempotency records after a safe window, for example several days,
while keeping failed or disputed calls longer for debugging.

## Pipeline Flow

1. The load generator or API creates an order.
2. The API stores the order as `placed` and inserts the first task.
3. A worker claims the next eligible task.
4. The worker calls the required downstream simulator if the stage needs it.
5. On success, the worker advances the order and inserts the next task.
6. On transient failure, the worker records the attempt and schedules a retry.
7. On repeated failure or invalid state, the order is marked `failed` or the task
   is discarded as a safe no-op.

Important downstream handoffs:

- Restaurant simulator: confirmation, preparation start, ready status.
- Courier simulator: courier assignment, pickup, delivery.

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

The UI should update automatically via Server-Sent Events, WebSockets, or short
polling. SSE is likely enough and simpler to explain.

If the SSE stream disconnects, the dashboard should reconnect automatically. On
reconnect it should fetch a fresh REST snapshot before resuming the stream, so a
dropped browser connection does not leave stale data on screen.

After a dinner-rush burst ends, the dashboard should make the recovery arc
visible: queue depth peaks and then drains, orders-per-minute returns to
baseline, downstream error rate normalizes, and p95 time-in-stage falls as
workers catch up.

## Load Generation

The load generator should support:

- Steady traffic: small number of orders per second.
- Dinner rush: sharp burst of orders for a configurable duration.
- Promotion mode: sustained high load.
- Controls from either CLI or dashboard.

Example future commands:

```bash
docker compose up
docker compose run loadgen --rate 5 --duration 2m
docker compose run loadgen --rate 100 --duration 1m --burst
```

## Failure Demo

The demo should make failures easy to trigger:

- Increase restaurant latency.
- Make restaurant calls fail randomly.
- Return courier rate limits.
- Kill one worker container while orders are in flight.
- Pause downstream recovery, then restore it.

Expected behavior:

- Orders already stored in Postgres remain visible.
- In-flight leased tasks become retryable after the lease expires.
- Duplicate processing attempts do not create duplicate business effects.
- The dashboard shows backlog growth, retries, failures, and recovery.

## Nice Touches

To make the demo feel like an operations tool instead of a raw engineering
console, build a few focused product touches:

- Chaos controls in the dashboard for restaurant failures, courier rate limits,
  downstream latency, and worker kill/recovery notes.
- Per-stage p95 latency so the team can see whether orders are stuck at
  confirmation, preparation, courier dispatch, pickup, or delivery.
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

1. Define order states, database schema, and migrations.
2. Build order intake API and basic order creation.
3. Build worker task claiming, leases, retries, and lifecycle transitions.
4. Add restaurant/courier simulators with failure controls.
5. Add load generator.
6. Wire the SSE stream and REST snapshot path used by the dashboard.
7. Build dashboard with live metrics and order state views.
8. Add health/metrics endpoints and demo failure controls.
9. Write final README instructions for running, load testing, and failure demos.
