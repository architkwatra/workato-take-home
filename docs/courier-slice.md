# Courier Slice Plan

## Goal

Complete the restaurant side of the lifecycle and add the first courier step:

```text
placed -> confirmed -> preparing -> ready -> out_for_delivery -> delivered
```

`ready -> out_for_delivery` is another command task: the worker tells the
courier simulator to assign a courier and gets back a `courier_ref`. That ref is
stored on the order row so later slices (and the dashboard) can display which
courier holds the delivery.

`out_for_delivery -> delivered` is a polling task, following the same shape as
`check_ready -> ready`. The simulator uses elapsed time from the
`ready -> out_for_delivery` event timestamp to decide when delivery is complete.

## Current Baseline

The merged `main` branch already does this:

- Worker advances `placed -> confirmed` and `confirmed -> preparing` as command
  tasks via `RestaurantTransitionSpec` and `RESTAURANT_TRANSITION_SPECS`.
- Worker advances `preparing -> ready` as a `check_ready` poll task with
  `deadline_at` to cap infinite polling.
- `_insert_followup_task` is a shared helper that inserts the next task in the
  same transaction as any state transition. `_followup_schedule_for_spec` maps
  spec follow-up types to their scheduling parameters.
- `RestaurantTransitionSpec` already carries `next_task_type` and
  `next_target_state` to drive follow-up task insertion generically.
- `_finalize_transition_task` uses these fields to call `_insert_followup_task`
  and `_followup_schedule_for_spec` without any per-transition branching.
- `orders.courier_ref` exists in the schema (nullable) and is not yet written.

## New Design Constraint: Courier Ref

The courier assignment response includes a `courier_ref` that must be written to
`orders.courier_ref` in the same finalization transaction as the state change.
This is the first transition where the downstream HTTP response carries data that
must be stored on the order row — previous transitions only needed a status check.

The current three-phase shape for command tasks is:

1. `_prepare_transition_call` — validate, get `restaurant_ref` for the request.
2. HTTP call outside any lock.
3. `_finalize_transition_task` — re-validate, update orders, insert event, complete task.

Phase 3 currently receives only the `spec`; it has no channel to receive the
`courier_ref` that came back in phase 2. The fix is to add an optional
`extra_order_fields: dict | None` parameter to the finalize phase (or to the
`RestaurantTransitionSpec` itself). The courier assignment handler populates it
with `{"courier_ref": "<value from response>"}` before calling finalize. The
finalize SQL adds any `extra_order_fields` to the `UPDATE orders SET ...` clause.

Keep the change minimal: `extra_order_fields` is always `None` for all existing
restaurant specs, so no existing code path changes. The `UPDATE orders` in
`_finalize_transition_task` expands to include the extra columns only when the
dict is non-empty.

## Proposed Scope

### Downstream Simulator

Add two endpoints.

**Courier assignment** (command-style):

```http
POST /courier/assign
```

Request:
```json
{ "order_id": "...", "restaurant_ref": "..." }
```

Response:
```json
{ "status": "assigned", "courier_ref": "courier-<uuid-hex>" }
```

Generate `courier_ref` deterministically from `order_id` (e.g.,
`f"courier-{hashlib.md5(order_id.encode()).hexdigest()[:12]}"`) so repeated
calls for the same order always return the same ref. No persistence needed.

**Delivery status poll**:

```http
POST /courier/check-delivery
```

Request:
```json
{
  "order_id": "...",
  "courier_ref": "...",
  "dispatched_at": "2026-06-29T10:00:00.000000+00:00"
}
```

Response while in transit:
```json
{ "status": "in_transit", "retry_after_seconds": 3 }
```

Response when delivered:
```json
{ "status": "delivered" }
```

Use `COURIER_DELIVERED_AFTER_SECONDS` (default `8`) as the time-based delivery
gate, measured from `dispatched_at`. Same deterministic, persistence-free pattern
as `RESTAURANT_READY_AFTER_SECONDS`.

### Worker: Courier Assignment Command Task

Add to `RESTAURANT_TRANSITION_SPECS`:

```python
ORDER_STATE_OUT_FOR_DELIVERY: RestaurantTransitionSpec(
    source_state=ORDER_STATE_READY,
    target_state=ORDER_STATE_OUT_FOR_DELIVERY,
    endpoint_path="/courier/assign",
    expected_status="assigned",
    action_name="courier assign",
    next_task_type=TASK_TYPE_CHECK_DELIVERY,
    next_target_state=ORDER_STATE_DELIVERED,
),
```

The HTTP response for courier assignment carries `courier_ref`. Extract it from
the response body in `_call_restaurant_transition` (or a new courier-specific
caller) and pass it through to `_finalize_transition_task` via the
`extra_order_fields` mechanism described above.

`_followup_schedule_for_spec` already handles `check_ready`; add an equivalent
branch for `check_delivery`:

```python
if spec.next_task_type == TASK_TYPE_CHECK_DELIVERY:
    return (
        occurred_at + timedelta(seconds=_delivery_check_initial_delay_seconds()),
        occurred_at + timedelta(seconds=_delivery_check_deadline_seconds()),
    )
```

New env vars (with defaults):

```
DELIVERY_CHECK_INITIAL_DELAY_SECONDS=3
DELIVERY_CHECK_INTERVAL_SECONDS=3
DELIVERY_CHECK_DEADLINE_SECONDS=120
```

### Worker: Delivery Poll Task

Add a `process_delivery_check_task` handler that mirrors `process_ready_check_task`
almost exactly. The differences are:

| Dimension | `check_ready` | `check_delivery` |
|---|---|---|
| Expected order state | `preparing` | `out_for_delivery` |
| Transition target | `ready` | `delivered` |
| Sim endpoint | `/restaurant/check-ready` | `/courier/check-delivery` |
| Request field | `prep_started_at` | `dispatched_at` |
| Source event | `confirmed -> preparing` transition | `ready -> out_for_delivery` transition |
| Not-ready status | `not_ready` | `in_transit` |
| Ready status | `ready` | `delivered` |
| Extra order fields on finalize | none | none |

The `dispatched_at` value is read from `order_events` the same way
`prep_started_at` is for `check_ready`: find the `state_transition` event where
`from_state = ready` and `to_state = out_for_delivery`. If missing, treat it as
a retryable local error.

Do not extract courier-specific logic into a new abstraction layer yet. The two
poll handlers are similar enough to template from each other, but a shared
generic poll base would need to handle enough conditional branches to be harder
to read than two explicit functions. Shared helpers for ownership loading,
optimistic finalization, `_retry_or_fail_claimed_task`, and
`_fail_claimed_task_without_attempt_increment` remain in place.

### Operator Retry (`task_store.py`)

`retry_failed_tasks_for_order` in `backend/api/task_store.py` recalculates a
fresh `deadline_at` for `check_ready` tasks after manual recovery. A delivered
order's `check_delivery` task will never reach `failed` while the order is still
`out_for_delivery`, but if it does (e.g., `max_attempts` exhausted while the sim
is down), the operator retry path needs to give it a fresh deadline window too.

Extend the `CASE` expression in the CTE update to include `check_delivery`:

```sql
when failed_task.task_type in ('check_ready', 'check_delivery') then
    %s + greatest(
        coalesce(failed_task.deadline_at - failed_task.created_at,
                 interval '60 seconds'),
        interval '1 second'
    )
```

Import `TASK_TYPE_CHECK_DELIVERY` alongside `TASK_TYPE_CHECK_READY` in
`task_store.py` and pass it in the query parameters.

### Dispatch in `claim_and_process_one_task`

Add a branch:

```python
elif (
    task.task_type == TASK_TYPE_CHECK_DELIVERY
    and task.target_state == ORDER_STATE_DELIVERED
):
    result = process_delivery_check_task(task=task, worker_id=worker_id)
```

Import `TASK_TYPE_CHECK_DELIVERY` and `ORDER_STATE_DELIVERED` alongside the
existing imports.

## Non-Goals

- No `check_pickup` task yet. The schema has the task type; defer it until
  there is a concrete use case (e.g., tracking when the courier arrives at the
  restaurant).
- No `downstream_calls` table yet. Courier assignment is deterministic and the
  simulator returns the same `courier_ref` for the same `order_id`, so repeated
  calls are safe. Durable downstream idempotency remains a later slice.
- No order-level `failed` state propagation yet. If the courier assignment task
  exhausts retries, the task becomes `failed` and the order stays in `ready`.
  The existing operator retry endpoint handles manual recovery.
- No random failure injection in the simulator.
- No dashboard changes unless the current display breaks.

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

Expected after all tasks complete:

- 10 orders reach `delivered`.
- Each order has five completed tasks: `advance_state -> confirmed`,
  `advance_state -> preparing`, `check_ready -> ready`,
  `advance_state -> out_for_delivery`, `check_delivery -> delivered`.

- Each order has five transition events: `placed -> confirmed`,
  `confirmed -> preparing`, `preparing -> ready`, `ready -> out_for_delivery`,
  `out_for_delivery -> delivered`.

- `orders.courier_ref` is non-null for all delivered orders.
- No duplicate transition events.
- No order advances beyond `delivered`.

Runtime delivery outage:

- Stop `downstream-sim` while orders are in `out_for_delivery`.
- Verify `check_delivery` tasks retry with `attempts` incrementing and
  `last_error` set.
- Restart `downstream-sim` before `max_attempts` is exhausted.
- Verify those orders reach `delivered`.

Runtime delivery deadline:

- Set `COURIER_DELIVERED_AFTER_SECONDS` greater than
  `DELIVERY_CHECK_DEADLINE_SECONDS`.
- Verify `in_transit` polling does not increment `attempts`.
- Verify `check_delivery` tasks fail with a deadline error once `deadline_at`
  passes and the order remains in `out_for_delivery`.

## Risks and Decisions to Review

- The `extra_order_fields` extension to `_finalize_transition_task` is the only
  structural deviation from the existing command-task pattern. Keep it narrow: a
  `dict[str, Any] | None`, rendered inline as additional `SET col = %s` pairs.
  Do not generalise it further in this slice.
- `courier_ref` is generated deterministically in the simulator, so the
  `ready -> out_for_delivery` command is safe to retry. If the real integration
  ever returns a different `courier_ref` on retry, `orders.courier_ref` would
  need a conflict policy. Defer that to when the downstream is not deterministic.
- `DELIVERY_CHECK_DEADLINE_SECONDS` defaults to 120, twice the restaurant
  ready deadline. Adjust if demo timing feels too slow or too tight.
