# Restaurant Prep Slice Plan

## Goal

Add one more deterministic restaurant lifecycle step after confirmation:

```text
placed -> confirmed -> preparing
```

This slice should prove the worker can chain one durable task into the next
while still routing each business transition through `downstream-sim`.

## Current Baseline

The merged `main` branch already does this:

- API creates orders in `placed`.
- API creates one durable `advance_state -> confirmed` task.
- Worker claims the task with `FOR UPDATE SKIP LOCKED`.
- Worker calls `downstream-sim` at `POST /restaurant/confirm`.
- On success, worker updates `orders.state` from `placed` to `confirmed`.
- Worker inserts a `state_transition placed -> confirmed` event.
- Worker completes the task.
- On downstream failure, worker leaves the order in `placed` and retries the
  task with `last_error`, incrementing `attempts` on each retryable failure.
  Once `attempts + 1 >= max_attempts`, the worker marks the task `failed`.
- The task/order load locks only the task row with `FOR UPDATE OF task`. It does
  not lock the order row, so `orders.version` remains a real optimistic
  concurrency guard during finalization.

## Proposed Scope

### Downstream Simulator

Add:

```http
POST /restaurant/start-prep
```

Request:

```json
{
  "order_id": "...",
  "restaurant_ref": "..."
}
```

Response:

```json
{
  "status": "preparing"
}
```

Same properties as the confirm endpoint:

- deterministic;
- side-effect-free for now;
- no persistence;
- no random failures;
- safe to retry for the same `order_id` in this slice.

### Worker Behavior

After a successful `placed -> confirmed` transition, insert the next durable
task in the same DB transaction:

```text
task_type = advance_state
target_state = preparing
status = pending
next_run_at = now()
dedupe_key = <order_id>:advance_state:preparing
```

Then extend worker processing to support:

```text
advance_state -> preparing
```

Processing should mirror the confirm flow:

1. Claim task using the existing durable claim helper.
2. Load task/order details and verify ownership.
3. If order is terminal, complete task as a no-op.
4. If order is no longer `confirmed`, complete task as a stale no-op.
5. Call `downstream-sim` at `/restaurant/start-prep` outside any DB
   transaction or row lock.
6. On `{"status": "preparing"}`, reopen a transaction.
7. Revalidate task ownership and order state.
8. Update `orders.state` from `confirmed` to `preparing` using optimistic
   locking. The task row may be locked for ownership, but the order row must not
   be selected `FOR UPDATE`; the version predicate below is the concurrency
   guard:

```sql
where id = <order_id>
  and version = <version_read_earlier>
  and state = 'confirmed'
```

9. Insert `state_transition confirmed -> preparing` with non-null `task_id` and
   `worker_id`.
10. Mark the prep task `completed`.

Do not insert the next `preparing -> ready` task yet.

This slice remains command-style rather than callback- or polling-driven. The
worker sends a command to `downstream-sim` and advances local state only when the
simulator returns the expected success body. If the simulator gives no response
because of a timeout, connection failure, container stop, 5xx, invalid JSON, or
unexpected status body, the worker treats that as a retryable downstream error:

- the order remains in the source state (`placed` for confirmation, `confirmed`
  for start-prep);
- the claimed task is released back to `pending` with `last_error`;
- `attempts` increments on each retryable failure;
- once `attempts + 1 >= max_attempts`, the task becomes `failed`.

## Expected Refactor

The current worker code is confirm-specific. Rather than duplicate the entire
three-phase flow, introduce a small transition spec, for example:

```python
RestaurantTransitionSpec(
    source_state="placed",
    target_state="confirmed",
    endpoint_path="/restaurant/confirm",
    expected_status="confirmed",
    next_target_state="preparing",
)
```

and:

```python
RestaurantTransitionSpec(
    source_state="confirmed",
    target_state="preparing",
    endpoint_path="/restaurant/start-prep",
    expected_status="preparing",
    next_target_state=None,
)
```

The shared processing shape should stay the same:

```text
prepare/revalidate -> HTTP call outside transaction -> finalize or retry
```

The refactor must preserve two existing correctness properties from `main`:

- retryable downstream failures increment `attempts` and transition the task to
  `failed` once `max_attempts` is exhausted;
- transition finalization locks only the task row before updating `orders`, so
  `WHERE version = <read_version>` can detect stale workers and races.

## Non-Goals

- No `preparing -> ready` task yet.
- No `check_ready` polling yet.
- No downstream callback/webhook handling yet.
- No courier flow.
- No `downstream_calls` table yet.
- No simulator persistence or idempotency records yet.
- No random failure injection.
- No dashboard changes unless an existing display breaks.

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

curl http://localhost:8082/status
```

Use the returned `run_id` in all DB queries.

Expected:

- 10 generated orders reach `preparing`.
- Each generated order has two completed tasks:
  - `advance_state -> confirmed`;
  - `advance_state -> preparing`.
- Each generated order has two transition events:
  - `placed -> confirmed`;
  - `confirmed -> preparing`.
- Both transition event types have non-null `task_id` and `worker_id`.
- No duplicate transition events exist.
- No generated task remains `pending` or `running`.
- No generated order advances beyond `preparing`.

Runtime downstream outage:

- Stop `downstream-sim`.
- Create a small bounded loadgen run.
- Verify orders remain in their pre-transition state:
  - confirm failures leave orders in `placed`;
  - prep failures leave orders in `confirmed`.
- Verify retryable failures increment `attempts` and leave tasks `pending` with
  `last_error` while `attempts + 1 < max_attempts`.
- Verify tasks become `failed` with `last_error` after `max_attempts` is
  exhausted.
- For recovery testing, restart `downstream-sim` before `max_attempts` is
  exhausted.
- Verify those same tasks recover and orders continue to the intended state.

## Risks And Decisions To Review

- The retry budget is currently small (`max_attempts = 5`, retry delay 5s).
  During manual outage demos, tasks can legitimately fail if the simulator is
  left down for roughly 20-25 seconds. We can keep this because it proves the
  circuit breaker, or increase the default retry budget in a later schema slice.
- If this slice is implemented from an older branch instead of current `main`,
  first port the bounded-retry behavior and task-only row locking from PR #21.
- The simulator remains side-effect-free, so repeated prep calls are acceptable
  for now. Durable downstream idempotency should still be added before courier
  dispatch or any non-idempotent simulator behavior.
- Generic transition handling reduces duplication now, but it should stay small:
  only restaurant confirm and restaurant start-prep should be supported in this
  slice.
