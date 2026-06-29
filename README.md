# Workato Take-Home: Order Pipeline

A food-delivery order pipeline simulation: high-volume order intake, resilient downstream integrations, and live operational visibility — all on a single machine with Docker Compose.

## Quick Start

```bash
git clone <repo>
cd workato-take-home
docker compose up --build
```

Services start in dependency order (Postgres → migrator → API/workers → rest). Once healthy (~30 seconds):

| Service | URL |
|---------|-----|
| Dashboard | http://localhost:3000 |
| API | http://localhost:8080/healthz |
| Downstream sim | http://localhost:8081/healthz |
| Load generator | http://localhost:8082/readyz |
| Postgres | localhost:5432 (user: `app`, pass: `app`, db: `orders`) |

Open the **dashboard** to see everything in one place. It polls every 3 seconds and shows live order counts, throughput, pipeline latency, worker health, and recent events.

## How to Drive Load

### From the dashboard

In the **Load Generator** panel:
1. Set **Rate / Second** (e.g. `5` for steady, `50` for dinner rush)
2. Optionally set **Max Orders** to cap total creation
3. Click **Start**

The pipeline processes orders automatically. Watch the Lifecycle bars fill and drain, the Pipeline Latency panel update, and delivery events stream in the Recent Events table.

### From the CLI

```bash
# Start at 10 orders/second
curl -X POST http://localhost:8082/load/start \
  -H 'content-type: application/json' \
  -d '{"rate_per_second": 10}'

# Change rate live (no restart needed)
curl -X PATCH http://localhost:8082/load/rate \
  -H 'content-type: application/json' \
  -d '{"rate_per_second": 50}'

# Stop
curl -X POST http://localhost:8082/load/stop
```

## How to Trigger Failures

### Kill a downstream service

In the dashboard header, click **Downstream** and toggle any service off. Orders that depend on the killed service stall in that state (e.g. killing restaurant confirmation leaves orders stuck in `placed`). Workers retry with exponential backoff — the killed service is not hammered on recovery.

Re-enable the service to resume processing. Stalled orders pick up within the next retry window (5s → 60s cap).

### Kill a worker replica

```bash
# Find a running worker container
docker ps | grep worker

# Stop one (e.g. replica 3)
docker stop workato-take-home-worker-3
```

Its in-flight tasks become reclaimable after the 30-second lease expires. The remaining 4 workers claim them automatically — no orders are lost. The **Workers** KPI card drops from `5/5` to `4/5` and recovers when you restart the container.

### Trigger task failures

Use the **Downstream** panel to enable random failure mode on restaurant or courier. Tasks retry up to 5 times; on exhaustion the task is marked `failed` and appears in the **Problem Tasks** table. From there, click an order to open its detail view and retry individual failed tasks.

### Cancel orders in-flight

Click any row in the **Recent Orders** table to open the order detail view and click **Cancel Order**. Workers check order state before every downstream call — a cancelled order's in-flight tasks become no-ops via optimistic locking, so no duplicate restaurant/courier actions occur.

## Architecture

```
                        ┌──────────────────┐
Dashboard (React) ──────► API (FastAPI)    │
:3000                   │ :8080            │
                        └──────┬───────────┘
                               │ Postgres task queue
                        ┌──────▼───────────┐
                        │ Workers (×5)      │──► Downstream Sim :8081
                        │ poll / 250ms      │    (restaurant + courier)
                        └──────────────────┘
Load Generator ─────────► API /orders
:8082
```

**Order lifecycle:** `placed → confirmed → preparing → ready → out_for_delivery → delivered`

Terminal states: `cancelled`, `failed`

**Key correctness mechanisms:**

| Mechanism | What it prevents |
|-----------|-----------------|
| `SELECT FOR UPDATE SKIP LOCKED` in claim CTE | Two workers claiming the same task |
| 30-second leases (`locked_by`, `locked_until`) | Work stuck when a worker dies |
| Optimistic locking (`orders.version`) | Stale worker overwriting newer state |
| Client-generated idempotency keys | Duplicate orders from retried creation requests |
| Downstream idempotency keys (`restaurant_confirm:{order_id}`) | Duplicate restaurant confirmations or courier dispatches |
| Exponential backoff (5s base → 60s cap, 5 attempts) | Thundering herd on downstream recovery |

**Task types:**

- `advance_state` — drives restaurant confirmation, prep start, courier assignment
- `check_ready` — polls restaurant until food is ready (doesn't consume retry budget for normal "not ready" responses)
- `check_delivery` — polls courier until delivered (same policy)

## Services

| Service | Port | Tech | Role |
|---------|------|------|------|
| `api` | 8080 | FastAPI + psycopg | Order intake, dashboard data, health/readiness |
| `worker` (×5) | — | asyncio | Task execution, lifecycle advances, heartbeats |
| `postgres` | 5432 | PostgreSQL 16 | Durable source of truth for all state |
| `downstream-sim` | 8081 | FastAPI | Configurable flaky restaurant + courier with kill switches |
| `loadgen` | 8082 | FastAPI | Persistent rate-controlled traffic generator |
| `dashboard` | 3000 | React/Vite | Live operations UI |
| `migrator` | — | Alembic | One-shot schema migration on startup |

## Trade-offs

**Postgres-backed task queue over Kafka/RabbitMQ**

A durable `order_tasks` table with worker leases is simpler to operate locally, fully inspectable via SQL, and requires no extra infrastructure. The trade-off is horizontal write throughput — Postgres works well up to hundreds of concurrent workers but would become a bottleneck before a distributed queue would. Acceptable for a single-machine demo and most early-stage production loads.

**At-least-once processing with idempotency**

Exactly-once delivery across flaky systems requires distributed transactions that add significant complexity. Instead, every downstream call carries a stable idempotency key derived from `order_id` and the business action. The downstream simulator stores the first result for each key and replays it on duplicates — a retry never creates a second restaurant confirmation or courier dispatch. Workers may process some work more than once internally; they never produce duplicate business effects.

**SSE polling over WebSockets**

The dashboard polls `/dashboard/overview` every 3 seconds rather than maintaining a live push connection. This keeps the server stateless, avoids connection lifecycle complexity, and is sufficient for an operational dashboard where sub-second updates are not required. The trade-off is a 0–3 second lag on any state change.

**Fixed 5 worker replicas**

Workers are stateless processes that race for Postgres leases; scaling is `docker compose up --scale worker=N`. Five replicas provide enough parallelism to demonstrate recovery after killing one without overwhelming the downstream simulator. The API reports configured vs. active worker count so operators see degradation immediately.

**No separate metrics service**

Health (`/healthz`) and readiness (`/readyz`) endpoints exist on every service. Runtime counters (throughput, latency, queue depth, worker health) are computed on-demand by the dashboard endpoint rather than pushed to a metrics store. This avoids Prometheus/Grafana setup overhead while still giving operators the numbers they need during a demo.
