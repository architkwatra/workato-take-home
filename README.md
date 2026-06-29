# Workato Take-Home: Order Pipeline

A local food-delivery order pipeline simulation with order intake, durable worker
processing, downstream restaurant/courier simulators, load generation, and a live
operations dashboard.

Longer background notes from the original README live in
[docs/project-notes.md](docs/project-notes.md).

## Run It

Requirements: Docker Desktop with Compose support.

```bash
docker compose up --build
```

Or run it detached:

```bash
docker compose up --build -d
docker compose ps
```

Open the dashboard:

```text
http://localhost:3000
```

Useful service URLs:

| Service | URL |
| --- | --- |
| Dashboard | http://localhost:3000 |
| API health | http://localhost:8080/healthz |
| Downstream simulator health | http://localhost:8081/healthz |
| Load generator status | http://localhost:8082/status |
| Postgres | localhost:5432, user `app`, password `app`, db `orders` |

To reset local runtime data while keeping the schema:

```bash
docker compose exec postgres psql -U app -d orders \
  -c "truncate table order_events, order_tasks, orders, workers restart identity cascade;"
```

## Drive Load

From the dashboard:

1. Open `http://localhost:3000`.
2. In **Load Generator**, set **Rate / second**.
3. Set **Max orders** for a bounded run.
4. Click **Start**.
5. Watch lifecycle counts, orders/min, deliveries/min, latency, stuck orders,
   workers, problem tasks, and recent events update.

From the CLI:

```bash
curl -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second": 20, "max_orders": 1000}'

curl -X PATCH http://localhost:8082/load/rate \
  -H 'content-type: application/json' \
  -d '{"rate_per_second": 50}'

curl -X POST http://localhost:8082/load/stop

curl http://localhost:8082/status
```

## Trigger Failures

The easiest path is the dashboard header: click **Downstream**, then kill or
restore a simulator endpoint.

Equivalent CLI examples:

```bash
# Kill courier assignment.
curl -X POST http://localhost:8081/control/kill-switches/courier_assign \
  -H 'content-type: application/json' \
  -d '{"killed": true}'

# Restore courier assignment.
curl -X POST http://localhost:8081/control/kill-switches/courier_assign \
  -H 'content-type: application/json' \
  -d '{"killed": false}'
```

Available downstream kill switches:

- `restaurant_confirm`
- `restaurant_start_prep`
- `restaurant_check_ready`
- `courier_assign`
- `courier_check_delivery`

To simulate worker loss:

```bash
docker compose ps worker
docker stop <one-worker-container-name>
```

The worker heartbeat count drops, leased work becomes claimable after the lease
expires, and the remaining workers continue processing. Restore the configured
worker count with:

```bash
docker compose up -d --scale worker=5 worker
```

## Architecture

- `dashboard`: React/Vite operations UI.
- `api`: FastAPI order intake, dashboard read model, order actions.
- `postgres`: durable source of truth for orders, tasks, events, and workers.
- `worker`: five stateless worker replicas claim and process `order_tasks`.
- `downstream-sim`: FastAPI restaurant/courier simulator with kill switches and
  deterministic per-order stage delays.
- `loadgen`: rate-controlled traffic generator for demo and stress runs.
- `migrator`: one-shot Alembic migration container.

Order lifecycle:

```text
placed -> confirmed -> preparing -> ready -> out_for_delivery -> delivered
```

Terminal states:

```text
cancelled, failed
```

## Main Decisions And Trade-offs

- Postgres-backed task queue instead of Kafka/RabbitMQ: simpler local setup and
  easy SQL inspection, with lower ceiling than a dedicated broker.
- At-least-once worker execution: retries are expected, so state changes use
  optimistic locking and task leases to avoid duplicate lifecycle advances.
- Downstream delays are deterministic per order/stage: each order gets a
  staggered delay, but retries for the same order do not re-sample a new delay.
- Retryable downstream errors use exponential backoff; normal "not ready yet"
  responses use the simulator's `retry_after_seconds` and do not consume retry
  attempts.
- Dashboard polling is intentionally simple: the browser polls every 3 seconds,
  but very large datasets can make the effective top-level refresh slower
  because the next poll is scheduled after the previous request finishes.
- Metrics are computed from the operational database instead of a separate
  metrics stack. This keeps the submission self-contained, but heavy load runs
  eventually need query/index tuning or a dedicated metrics store.
