# Design Doc: Order Pipeline

This document captures the high-level design for the Workato take-home order
pipeline. It is intentionally brief for now; implementation details can be added
as the system is built.

## Goal

Build a local, full-stack order pipeline that can accept bursty order traffic,
move orders through a valid lifecycle, tolerate flaky downstream systems, and
show live operational health during a demo.

## System Shape

The system runs on one machine through Docker Compose:

- `api`: order intake, admin controls, health checks, dashboard data, and SSE.
- `worker`: durable task processing and order state transitions.
- `postgres`: source of truth for orders, tasks, events, idempotency, and worker
  heartbeats.
- `downstream-sim`: simulated restaurant and courier systems with failure
  controls.
- `loadgen`: persistent controllable traffic generator.
- `dashboard`: live operations UI.

This is not meant to be a production microservice platform. Separate containers
are used to model real boundaries and failure modes while keeping the local
setup understandable.

## Core Flow

1. `loadgen` or a user creates an order through the API with a client-generated
   idempotency key.
2. The API stores the order and inserts the first durable task.
3. Workers claim tasks from Postgres with leases.
4. Workers call downstream simulators, advance order state, and enqueue the next
   task.
5. Long waits, such as food preparation or courier travel, are represented as
   future check tasks rather than blocked workers.
6. Workers append events and notify the API.
7. The API pushes live updates to the dashboard.

## Data Flow Examples

### Example 1: Normal Order

1. `loadgen` sends `POST /orders` with a client-generated idempotency key.
2. `api` creates an `orders` row in `placed`, appends an `order_created` event,
   and inserts the first `order_tasks` row.
3. A `worker` claims the task, calls `downstream-sim` to confirm with the
   restaurant, then updates the order from `placed -> confirmed`.
4. The worker inserts the next task to start preparation. After preparation
   starts, it schedules a future `check_ready` task and releases its lease.
5. Later, a worker claims `check_ready`. If the simulator says `not_ready`, the
   task is rescheduled without incrementing error attempts. If it says `ready`,
   the order moves to `ready`.
6. A worker dispatches a courier through the simulator and moves the order to
   `out_for_delivery`.
7. Future `check_pickup` and `check_delivery` tasks model courier progress. A
   pickup creates a `courier_picked_up` milestone event while the order remains
   `out_for_delivery`; delivery moves the order to `delivered`.

At each meaningful step, the worker appends an `order_events` row and sends a
lightweight Postgres notification. The API receives the notification, fetches
fresh state, and updates the dashboard.

### Example 2: Cancellation During a Downstream Call

1. A worker reads an order in `placed` with `version = 3` and calls the
   restaurant confirmation endpoint.
2. Before the downstream response returns, the order is cancelled through the
   API. Postgres updates the order to `cancelled` and increments `version` to
   `4`.
3. The restaurant simulator later returns a successful confirmation to the
   worker.
4. The worker tries to apply the result with a guarded update:
   `WHERE state = 'placed' AND version = 3`.
5. The update affects zero rows because the order is now `cancelled` with
   `version = 4`.
6. The worker treats the downstream response as stale, does not enqueue the next
   task, and the order remains `cancelled`.

In this case the simulator may have an internal record saying the restaurant
confirmed the order, but the pipeline does not continue. Postgres order state is
authoritative for the platform. In a production system, this is where a
compensating restaurant/courier cancellation would be sent; for the demo, stale
acks are ignored and terminal order state wins.

When the API cancels an order, it updates the order state and marks pending
tasks for that order as `cancelled` in the same transaction. If a worker still
encounters a cancelled order because of a race, it marks its task completed as a
no-op and emits a diagnostic event.

### Example 3: Worker Crash After Downstream Success

1. A worker calls `downstream-sim` to dispatch a courier.
2. The simulator returns success.
3. The worker records `downstream_calls.status = 'succeeded'` but crashes before
   it advances the order or completes the task.
4. The task lease expires and another worker reclaims the task.
5. The new worker upserts the same `downstream_calls.idempotency_key`.
6. Because the existing row is already `succeeded`, the worker skips the HTTP
   call and uses the stored result to attempt the guarded order update.

This avoids both failure modes: no unique-key crash in the pipeline and no
second downstream dispatch.

## Downstream Simulator

`downstream-sim` is one local service that logically behaves like two external
systems: restaurant and courier. It exists to force the pipeline to handle real
network boundaries, latency, rate limits, failures, and uncertain responses.

It exposes three groups of endpoints:

- Restaurant endpoints: confirm order, start preparation, check readiness.
- Courier endpoints: dispatch courier, check pickup, check delivery.
- Admin endpoints: change latency, error rate, rate-limit behavior, and reset
  behavior.

The simulator stores runtime state for the fake external systems:

- Restaurant order state, such as confirmed/preparing/ready time.
- Courier delivery state, such as dispatched/picked-up/delivered time.
- Idempotency results keyed by the idempotency key passed by the worker.
- Current failure settings for restaurant and courier behavior.

The simulator's idempotency and progress state should be durable from the
start, backed by Postgres tables in a simulator namespace/schema. This prevents a
`downstream-sim` restart from forgetting that a courier was already dispatched
for a given idempotency key. Failure settings can remain in memory because they
are demo controls, not business facts.

The simulator must honor idempotency keys. If a worker retries
`courier_dispatch:{order_id}` after a timeout or dropped connection, the
simulator returns the existing dispatch result instead of creating another
courier dispatch.

## Correctness Approach

The pipeline assumes at-least-once processing. Correctness comes from:

- Client-generated inbound idempotency keys.
- Downstream idempotency keys per business action.
- Durable tasks in Postgres.
- Worker leases and reclaiming expired work.
- Guarded state transitions and optimistic locking.
- Downstream call upserts, so a reclaimed task can reuse a prior successful
  downstream result instead of getting stuck on a unique-key conflict.
- Explicit retry behavior for errors and separate scheduling for expected
  `not_ready`-style poll results.
- Poll deadlines for long-running checks, so an order does not stay in
  `preparing` or `out_for_delivery` forever if the simulator never progresses.

The system may retry work internally, but it should not create duplicate
business effects such as dispatching two couriers for one order.

## Observability

The dashboard should show:

- Order counts by state.
- Throughput and queue depth.
- Downstream latency, errors, and rate limits.
- Worker heartbeats and task recovery.
- Stuck orders and per-stage latency.

Workers write order events and issue lightweight Postgres notifications. The API
uses those notifications to fetch current state and update connected dashboard
clients.

If the API's Postgres `LISTEN` connection drops, it reconnects, re-subscribes,
logs the gap, and broadcasts a fresh snapshot to connected SSE clients. This
mirrors the browser reconnect behavior and prevents a silent stale dashboard.

## Database Design

Postgres is the durable source of truth. Use UUID primary keys, `timestamptz`
for timestamps, and `jsonb` only for flexible metadata that is not part of core
query logic.

### Enums

| Enum | Values | Purpose |
| --- | --- | --- |
| `order_state` | `placed`, `confirmed`, `preparing`, `ready`, `out_for_delivery`, `delivered`, `cancelled`, `failed` | Valid lifecycle states for an order. |
| `task_type` | `advance_state`, `check_ready`, `check_pickup`, `check_delivery` | What kind of work a worker should perform. |
| `task_status` | `pending`, `running`, `completed`, `failed`, `cancelled` | Current state of a durable task. |
| `event_type` | `order_created`, `state_transition`, `courier_picked_up`, `retry_scheduled`, `task_cancelled`, `order_cancelled`, `order_failed` | Timeline events shown in the dashboard. |
| `downstream_action` | `restaurant_confirm`, `restaurant_start_prep`, `restaurant_check_ready`, `courier_dispatch`, `courier_check_pickup`, `courier_check_delivery` | Business action sent to a downstream simulator. |
| `downstream_call_status` | `started`, `succeeded`, `failed`, `unknown` | Result of a downstream call from the pipeline's perspective. |

#### Task Types

`task_type` describes what a worker should do next. It is separate from
`order_state` because not every unit of work is a direct state transition.

| Task type | Meaning | Why it exists |
| --- | --- | --- |
| `advance_state` | Perform the required work for a transition and move the order to `target_state`. | Handles normal lifecycle movement like `placed -> confirmed`. |
| `check_ready` | Check whether restaurant preparation is complete. | Lets workers release the task between checks instead of blocking during prep time. |
| `check_pickup` | Check whether the courier has picked up the order. | Captures courier progress while the order remains `out_for_delivery`. |
| `check_delivery` | Check whether the courier has delivered the order. | Separates delivery waiting time from worker execution time. |

#### Task Statuses

`task_status` describes the durable queue row, not the customer-visible order
state.

| Status | Meaning | Why it exists |
| --- | --- | --- |
| `pending` | Waiting to be claimed after `next_run_at`. | Gives workers a simple query for runnable work. |
| `running` | Claimed by a worker lease. | Allows crash recovery when `locked_until` expires. |
| `completed` | Finished successfully or safely no-op'd after a race. | Prevents completed work from being reclaimed. |
| `failed` | Exhausted real error retries. | Stops endless retries and makes failed work visible. |
| `cancelled` | Invalidated because the order reached a terminal state. | Removes stale work when an order is cancelled or failed. |

### `orders`

Stores the current state of each order. This is the table the dashboard reads
for the live order view.

| Column | Type | Explanation |
| --- | --- | --- |
| `id` | `uuid primary key` | Internal order id generated by the API. |
| `idempotency_key` | `text unique not null` | Client/loadgen-generated key. Duplicate create requests return the existing order. |
| `state` | `order_state not null` | Current lifecycle state. |
| `version` | `integer not null default 0` | Optimistic locking guard for stale workers and cancellation races. |
| `customer_ref` | `text` | Simulated customer identifier for demo data. |
| `restaurant_ref` | `text not null` | Simulated restaurant identifier. |
| `courier_ref` | `text` | Simulated courier identifier once assigned. |
| `terminal_reason` | `text` | Human-readable reason for `cancelled` or `failed` orders. |
| `created_at` | `timestamptz not null` | When the order entered the system. |
| `updated_at` | `timestamptz not null` | Last state or metadata update. |
| `delivered_at` | `timestamptz` | Set when the order reaches `delivered`. |
| `cancelled_at` | `timestamptz` | Set when the order reaches `cancelled`. |

Key indexes:

- Unique index on `idempotency_key`.
- Index on `(state, updated_at)` for dashboard counts and stuck-order queries.

### `order_events`

Append-only timeline for state transitions and milestone events. This supports
auditability, live dashboard updates, and latency calculations.

| Column | Type | Explanation |
| --- | --- | --- |
| `id` | `uuid primary key` | Event id. |
| `order_id` | `uuid not null references orders(id)` | Order this event belongs to. |
| `event_type` | `event_type not null` | What happened. |
| `from_state` | `order_state` | Previous state for transition events; null for pure milestones. |
| `to_state` | `order_state` | New state for transition events; null for pure milestones. |
| `task_id` | `uuid references order_tasks(id)` | Task that produced the event, if any. |
| `worker_id` | `text` | Worker that produced the event, if any. |
| `occurred_at` | `timestamptz not null` | Event timestamp used for timelines and latency calculations. |
| `metadata` | `jsonb not null default '{}'` | Extra details such as downstream latency, retry delay, or error code. |

Workers insert an event after each meaningful transition or milestone and issue
`pg_notify` with only `order_id` and `event_type`. The API fetches current state
before sending the SSE update.

Key indexes:

- Index on `(order_id, occurred_at)` for per-order timelines.
- Index on `(event_type, occurred_at)` for dashboard metrics.

### `order_tasks`

Durable work queue. Workers claim rows from this table instead of relying on an
in-memory queue.

| Column | Type | Explanation |
| --- | --- | --- |
| `id` | `uuid primary key` | Task id. |
| `order_id` | `uuid not null references orders(id)` | Order this task advances or checks. |
| `task_type` | `task_type not null` | Work to perform, such as `advance_state` or `check_ready`. |
| `target_state` | `order_state` | Expected state after a successful transition, if applicable. |
| `status` | `task_status not null` | Task status. |
| `attempts` | `integer not null default 0` | Counts actual errors only. Expected poll results like `not_ready` do not increment it. |
| `max_attempts` | `integer not null default 5` | Error limit before the order is marked `failed`. |
| `next_run_at` | `timestamptz not null` | Earliest time this task can be claimed. Used for retries and future checks. |
| `deadline_at` | `timestamptz` | Wall-clock deadline for poll tasks such as `check_ready`, after which repeated `not_ready` results become a failure. Initial defaults can be 10 minutes for restaurant readiness and 15 minutes for courier pickup/delivery. |
| `locked_by` | `text` | Worker id currently holding the lease. |
| `locked_until` | `timestamptz` | Lease expiry. Defaults to 30 seconds after claim. |
| `dedupe_key` | `text` | Stable key to prevent duplicate active tasks for the same order/action. |
| `last_error` | `text` | Most recent error for debugging and dashboard display. |
| `created_at` | `timestamptz not null` | Task creation time. |
| `updated_at` | `timestamptz not null` | Last task update. |
| `completed_at` | `timestamptz` | Set when the task completes. |

Workers claim or reclaim tasks using `SELECT ... FOR UPDATE SKIP LOCKED` where
`next_run_at <= now()` and either the task is `pending` or it is `running` with
an expired `locked_until`.

Workers renew `locked_until` while actively processing a task, for example every
10 seconds. The 30-second lease is the recovery window for crashed workers, not
the maximum allowed duration of a downstream call.

Poll tasks treat expected results such as `not_ready` as successful checks:
they update `next_run_at` without incrementing `attempts`. If `now() >=
deadline_at`, the poll is converted into a real failure and the order moves to
`failed`.

When an order is cancelled, pending tasks for that order are marked
`cancelled` in the same transaction as the order state change.

Key indexes:

- Index on `(status, next_run_at)` for worker polling.
- Index on `(locked_until)` for lease recovery metrics.
- Partial unique index on `dedupe_key` for active tasks where status is
  `pending` or `running`.

### `downstream_calls`

Records calls from the pipeline to restaurant and courier simulators. This is
the pipeline-side audit record for idempotency and debugging.

| Column | Type | Explanation |
| --- | --- | --- |
| `id` | `uuid primary key` | Call record id. |
| `order_id` | `uuid not null references orders(id)` | Order associated with the downstream action. |
| `action` | `downstream_action not null` | Business action being attempted. |
| `idempotency_key` | `text unique not null` | Stable key derived from `order_id` and action, used to avoid duplicate business effects. |
| `status` | `downstream_call_status not null` | Whether the call succeeded, failed, or ended in an unknown state such as a dropped connection. |
| `request_hash` | `text` | Hash of the outbound request body to detect accidental key reuse with different payloads. |
| `downstream_ref` | `text` | Simulated downstream id, such as courier dispatch id. |
| `response_body` | `jsonb` | Last successful or diagnostic response. |
| `last_error` | `text` | Last observed error, timeout, or rate-limit message. |
| `attempts` | `integer not null default 0` | Number of downstream attempts for this action. |
| `created_at` | `timestamptz not null` | First attempt time. |
| `updated_at` | `timestamptz not null` | Last attempt or result update. |

The downstream simulator must also dedupe by the same idempotency key. If a
connection drops after the simulator processed a request, the retry uses the same
key and receives the original result instead of creating a second dispatch or
confirmation.

Workers write this table with `INSERT ... ON CONFLICT (idempotency_key) DO
UPDATE`. If the existing row is `succeeded`, the worker skips the downstream HTTP
call and uses the stored result to advance the order. If the row is `started`,
`unknown`, or `failed`, the worker retries the downstream call with the same
idempotency key. A `request_hash` mismatch is treated as a programming error and
fails the task.

Key indexes:

- Unique index on `idempotency_key`.
- Index on `(order_id, action)` for per-order debugging.

### `workers`

Tracks worker liveness separately from task leases. A worker can be active even
when it is idle and holding no task.

| Column | Type | Explanation |
| --- | --- | --- |
| `worker_id` | `text primary key` | Stable id generated when a worker process starts. |
| `hostname` | `text` | Container or host name for demo/debugging. |
| `started_at` | `timestamptz not null` | Worker process start time. |
| `last_seen_at` | `timestamptz not null` | Heartbeat timestamp, updated every 10 seconds. |
| `metadata` | `jsonb not null default '{}'` | Optional process details such as version or replica name. |

The dashboard counts active workers as rows with `last_seen_at` within the last
30 seconds.

### Simulator Support Tables

These tables belong to `downstream-sim`, not the core platform. They make the
simulator restart-safe enough that idempotency still holds if the simulator
container is restarted during a demo.

| Table | Purpose |
| --- | --- |
| `sim_idempotency_records` | Stores the first response for each downstream idempotency key and returns it on duplicate calls. |
| `sim_restaurant_orders` | Stores simulated restaurant state such as confirmed, preparing, and `ready_at`. |
| `sim_courier_deliveries` | Stores simulated courier state such as dispatched, `pickup_at`, and `delivered_at`. |

The platform still treats `orders` as authoritative. Simulator tables only model
external systems and their idempotent responses.

### Initial Tables Not Needed

The first implementation does not need separate tables for dashboard state,
downstream failure settings, or loadgen state. Those can live in the relevant
services and be exposed through APIs. If persistence becomes useful later, they
can be added without changing the core order-processing model.

## Demo Scenarios

The design should support these live scenarios:

- Normal order flow from placement to delivery.
- Dinner rush by dialing `loadgen` up and down at runtime.
- Downstream degradation and recovery through dashboard controls.
- Worker failure by killing one worker replica and watching other replicas
  recover expired leases.

## Open Details

These can be filled in as implementation starts:

- API routes and request/response shapes.
- Dashboard layout.
- Downstream simulator response contracts.
- Retry and timing defaults.
- Final Docker Compose service definitions.
