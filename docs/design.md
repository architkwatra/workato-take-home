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

## Correctness Approach

The pipeline assumes at-least-once processing. Correctness comes from:

- Client-generated inbound idempotency keys.
- Downstream idempotency keys per business action.
- Durable tasks in Postgres.
- Worker leases and reclaiming expired work.
- Guarded state transitions and optimistic locking.
- Explicit retry behavior for errors and separate scheduling for expected
  `not_ready`-style poll results.

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

## Demo Scenarios

The design should support these live scenarios:

- Normal order flow from placement to delivery.
- Dinner rush by dialing `loadgen` up and down at runtime.
- Downstream degradation and recovery through dashboard controls.
- Worker failure by killing one worker replica and watching other replicas
  recover expired leases.

## Open Details

These can be filled in as implementation starts:

- Exact database schema.
- API routes and request/response shapes.
- Dashboard layout.
- Downstream simulator response contracts.
- Retry and timing defaults.
- Final Docker Compose service definitions.
