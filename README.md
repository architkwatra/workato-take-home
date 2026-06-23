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

Recommended stack:

- Backend/workers/load generator: TypeScript with Node.js, or Python with
  FastAPI. Pick one language and keep the moving parts easy to explain.
- Database: Postgres.
- Frontend: React/Vite or Next.js.
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
- Expired leases are recoverable by another worker.
- Retries use exponential backoff and max-attempt limits.
- Downstream calls include idempotency keys so repeated calls do not create
  duplicate restaurant confirmations or courier dispatches.
- Before doing work, a worker checks the current order state. If the order has
  already moved on, the duplicate task becomes a no-op.

This means the system can process some work more than once internally, but it
should not produce duplicate business effects.

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

## Implementation Order

1. Define order states, database schema, and migrations.
2. Build order intake API and basic order creation.
3. Build worker task claiming, leases, retries, and lifecycle transitions.
4. Add restaurant/courier simulators with failure controls.
5. Add load generator.
6. Build dashboard with live metrics and order state views.
7. Add health/metrics endpoints and demo failure controls.
8. Write final README instructions for running, load testing, and failure demos.
