# Restaurant Ready Slice Plan

## Goal

Add the next restaurant lifecycle step:

```text
placed -> confirmed -> preparing -> ready
```

This slice should prove the system can handle asynchronous downstream progress
without blocking a worker. `preparing -> ready` is not another immediate command.
It is a polling task: the restaurant may still be cooking, and that is normal
business state rather than a downstream failure.

## Current Baseline

The merged `main` branch already does this:

- API creates orders in `placed`.
- Worker advances `placed -> confirmed` by calling
  `POST /restaurant/confirm`.
- Worker advances `confirmed -> preparing` by calling
  `POST /restaurant/start-prep`.
- Both command calls happen outside DB transactions and row locks.
- Retryable downstream failures increment `attempts`, set `last_error`, and
  leave the order in its source state.
- Once `attempts + 1 >= max_attempts`, the task becomes `failed`.
- Successful task completion clears `last_error` but leaves `attempts` visible
  as retry history.
- Transition finalization locks only the task row, so `orders.version` remains
  the optimistic concurrency guard.

## Proposed Scope

### Downstream Simulator

Add:

```http
POST /restaurant/check-ready
```

Request:

```json
{
  "order_id": "...",
  "restaurant_ref": "...",
  "prep_started_at": "2026-06-28T22:54:34.000000+00:00"
}
```

Response while food is still being prepared:

```json
{
  "status": "not_ready",
  "retry_after_seconds": 2
}
```

Response once food is ready:

```json
{
  "status": "ready"
}
```

The simulator should stay deterministic, side-effect-free, and persistence-free
for this slice. Use time-based readiness instead of storing simulator state:

- read `RESTAURANT_READY_AFTER_SECONDS`, defaulting to a small demo value such
  as `6`;
- return `ready` when `now >= prep_started_at + RESTAURANT_READY_AFTER_SECONDS`;
- otherwise return `not_ready` with a small `retry_after_seconds`.

This models asynchronous progress without adding callbacks, random failures, or
simulator persistence yet.

### Worker Task Insertion

After a successful `confirmed -> preparing` transition, insert the next durable
task in the same DB transaction:

```text
task_type = check_ready
target_state = ready
status = pending
next_run_at = now() + READY_CHECK_INITIAL_DELAY_SECONDS
deadline_at = now() + READY_CHECK_DEADLINE_SECONDS
dedupe_key = <order_id>:check_ready:ready
```

Suggested defaults:

```text
READY_CHECK_INITIAL_DELAY_SECONDS=2
READY_CHECK_INTERVAL_SECONDS=2
READY_CHECK_DEADLINE_SECONDS=60
```

The same-transaction rule matters: if the order reaches `preparing`, the
follow-up check exists; if the transition rolls back, no check can be claimed.

### Worker `check_ready` Processing

Add a handler for:

```text
task_type = check_ready
target_state = ready
```

Processing shape:

1. Claim the task using the existing `claim_one_task` helper.
2. Load task ownership, order state, order version, restaurant ref, and the
   `confirmed -> preparing` transition time.
3. If the order is terminal, complete the task as a no-op.
4. If the order is no longer `preparing`, complete the task as a stale no-op.
5. Call `downstream-sim` at `/restaurant/check-ready` outside any DB
   transaction or row lock.
6. If downstream returns `{"status": "not_ready"}`:
   - leave the order in `preparing`;
   - release the same task back to `pending`;
   - set `next_run_at` using `retry_after_seconds` if valid, otherwise
     `READY_CHECK_INTERVAL_SECONDS`;
   - do not increment `attempts`;
   - clear `last_error`.
7. If downstream returns `{"status": "ready"}`:
   - reopen a transaction;
   - revalidate task ownership and order state;
   - update `orders.state` from `preparing` to `ready` using optimistic locking;
   - insert `state_transition preparing -> ready` with non-null `task_id` and
     `worker_id`;
   - complete the `check_ready` task.
8. If downstream gives no response, times out, returns 5xx, invalid JSON, or an
   unexpected body:
   - leave the order in `preparing`;
   - increment `attempts`;
   - set `last_error`;
   - release the task back to `pending` for a normal retry;
   - mark the task `failed` once `attempts + 1 >= max_attempts`.

The handler should also respect `deadline_at`. If the restaurant keeps returning
`not_ready` after the deadline, fail the task with a clear `last_error` such as
`restaurant ready deadline exceeded`. This deadline failure should not be
counted as a transient downstream attempt; it is a business timeout, not a
connection failure.

## Prep Start Time Source

Use the existing `order_events` row for `confirmed -> preparing` as the
`prep_started_at` value sent to the simulator. That event is inserted in the
same transaction as the order state change, so it is the best durable marker for
when prep began.

If the event is missing for a `check_ready` task, treat that as a retryable local
processing error for this slice. It means the worker cannot safely ask the
simulator whether enough prep time has elapsed.

## Expected Refactor

Keep restaurant command transitions and readiness polling related but distinct:

- `advance_state -> confirmed` and `advance_state -> preparing` are command
  tasks that either succeed or consume the error retry budget.
- `check_ready -> ready` is a polling task with a third normal outcome:
  `not_ready`.

Do not force `check_ready` into `RestaurantTransitionSpec` if that makes the
flow harder to read. A small shared helper for ownership/state loading,
optimistic finalization, and retry/fail updates is useful; hiding the
`not_ready` branch inside an over-general abstraction is not.

Add or reuse helper comments around:

- why `not_ready` does not increment `attempts`;
- why the HTTP call still happens outside DB transactions;
- why `deadline_at` exists for poll tasks;
- why no response is different from `not_ready`.

## Non-Goals

- No callbacks/webhooks yet.
- No courier flow yet.
- No `ready -> out_for_delivery` transition yet.
- No `downstream_calls` table yet.
- No random failure injection.
- No dashboard changes unless the current display breaks.
- No automatic order-level `failed` transition yet when a task exhausts retries.
  Current behavior is task failure while the order remains in its source state.

## Verification Plan

Static:

```bash
PYTHONPYCACHEPREFIX=/private/tmp/workato-pycache python3 -m py_compile \
  backend/api/*.py \
  backend/common/*.py \
  backend/downstream_sim/*.py \
  backend/loadgen/*.py \
  backend/worker/*.py

git diff --check
```

Runtime happy path:

```bash
docker compose up --build -d

curl -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second":10,"max_orders":10,"restaurant_ref":"restaurant-1"}'
```

Use the returned `run_id` in all DB queries.

Expected:

- 10 generated orders reach `ready`.
- Each generated order has three completed tasks:
  - `advance_state -> confirmed`;
  - `advance_state -> preparing`;
  - `check_ready -> ready`.
- Each generated order has three transition events:
  - `placed -> confirmed`;
  - `confirmed -> preparing`;
  - `preparing -> ready`.
- Transition events have non-null `task_id` and `worker_id`.
- No duplicate transition events exist.
- No generated order advances beyond `ready`.

Runtime `not_ready` behavior:

- Set `RESTAURANT_READY_AFTER_SECONDS` high enough to observe polling.
- Start a small bounded loadgen run.
- Before readiness time has elapsed, verify:
  - orders are in `preparing`;
  - `check_ready` tasks are `pending`;
  - `attempts = 0`;
  - `last_error is null`;
  - `next_run_at` moves forward on each `not_ready` response.

Runtime no-response behavior:

- Stop `downstream-sim` while one or more orders are in `preparing` with
  `check_ready` tasks.
- Verify:
  - orders remain `preparing`;
  - `check_ready` tasks retry with `attempts` incrementing;
  - `last_error` is populated;
  - workers remain alive and heartbeating.
- Restart `downstream-sim` before `max_attempts` is exhausted.
- Verify those same tasks recover and orders reach `ready`.

Runtime deadline behavior:

- Configure `RESTAURANT_READY_AFTER_SECONDS` greater than
  `READY_CHECK_DEADLINE_SECONDS`.
- Verify `not_ready` polling does not increment `attempts`.
- Verify the `check_ready` task eventually becomes `failed` with a deadline
  error once `deadline_at` passes.

## Risks And Decisions To Review

- This slice intentionally uses polling. In a real integration, a webhook from
  the restaurant/POS system would usually be preferred if available.
- `deadline_at` prevents infinite business polling, while `max_attempts`
  prevents infinite transient-error retries. They are separate controls and
  should not be collapsed.
- The simulator is time-based but still side-effect-free. Durable downstream
  idempotency remains a later slice.
- The order remains in `preparing` when `check_ready` fails. Moving the order to
  `failed` should be designed as a separate failure-policy slice.
