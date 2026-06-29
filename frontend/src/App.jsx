import { useEffect, useMemo, useState } from "react";

import "./styles.css";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080").replace(
  /\/$/,
  "",
);
// Polling keeps the dashboard operationally useful without adding websocket
// infrastructure to this small local demo slice.
const POLL_INTERVAL_MS = 3000;

const ORDER_STATES = [
  "placed",
  "confirmed",
  "preparing",
  "ready",
  "out_for_delivery",
  "delivered",
  "cancelled",
  "failed",
];

const TASK_STATUSES = ["pending", "running", "completed", "failed", "cancelled"];

const LABELS = {
  out_for_delivery: "out for delivery",
  check_delivery: "check delivery",
  check_pickup: "check pickup",
  check_ready: "check ready",
  advance_state: "advance state",
};

function humanize(value) {
  if (!value) {
    return "None";
  }
  return LABELS[value] ?? value.replaceAll("_", " ");
}

function numberText(value) {
  return Number(value ?? 0).toLocaleString();
}

function shortId(value) {
  if (!value) {
    return "None";
  }
  return value.length > 12 ? `${value.slice(0, 8)}...` : value;
}

function formatTime(value) {
  if (!value) {
    return "None";
  }

  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function formatAge(seconds) {
  if (seconds === null || seconds === undefined) {
    return "Unknown";
  }

  const safeSeconds = Math.max(0, Number(seconds));
  if (safeSeconds < 60) {
    return `${safeSeconds}s`;
  }
  if (safeSeconds < 3600) {
    return `${Math.floor(safeSeconds / 60)}m ${safeSeconds % 60}s`;
  }
  return `${Math.floor(safeSeconds / 3600)}h ${Math.floor((safeSeconds % 3600) / 60)}m`;
}

function StatusBadge({ active, children }) {
  return <span className={`status-badge ${active ? "active" : "stale"}`}>{children}</span>;
}

function Metric({ label, value, detail, tone = "neutral" }) {
  return (
    <section className={`metric ${tone}`}>
      <p>{label}</p>
      <strong>{value}</strong>
      {detail ? <span>{detail}</span> : null}
    </section>
  );
}

function EmptyRow({ colSpan, children }) {
  return (
    <tr>
      <td className="empty-cell" colSpan={colSpan}>
        {children}
      </td>
    </tr>
  );
}

function App() {
  const [overview, setOverview] = useState(null);
  const [status, setStatus] = useState("loading");
  const [error, setError] = useState("");
  const [lastRefreshAt, setLastRefreshAt] = useState(null);

  useEffect(() => {
    let stopped = false;
    let timerId = null;
    let controller = null;

    async function loadOverview() {
      controller = new AbortController();
      setStatus((current) => (current === "online" ? "refreshing" : "loading"));

      try {
        const response = await fetch(`${API_BASE_URL}/dashboard/overview`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`API returned ${response.status}`);
        }

        const payload = await response.json();
        if (stopped) {
          return;
        }

        setOverview(payload);
        setStatus("online");
        setError("");
        setLastRefreshAt(new Date());
      } catch (caughtError) {
        if (stopped || caughtError.name === "AbortError") {
          return;
        }

        // Keep the last successful payload visible when a refresh fails. The
        // dashboard is more useful during service restarts if stale data stays
        // on screen with a clear connection badge.
        setStatus("offline");
        setError(caughtError.message || "API request failed");
      } finally {
        if (!stopped) {
          timerId = window.setTimeout(loadOverview, POLL_INTERVAL_MS);
        }
      }
    }

    loadOverview();

    return () => {
      stopped = true;
      window.clearTimeout(timerId);
      controller?.abort();
    };
  }, []);

  const totals = useMemo(() => {
    const orderCounts = overview?.orders?.by_state ?? {};
    const taskCounts = overview?.tasks?.by_status ?? {};
    return {
      totalOrders: overview?.orders?.total ?? 0,
      deliveredOrders: orderCounts.delivered ?? 0,
      failedTasks: taskCounts.failed ?? 0,
      duePending: overview?.tasks?.due_pending ?? 0,
      expiredRunning: overview?.tasks?.expired_running ?? 0,
      activeWorkers: overview?.workers?.active_count ?? 0,
      seenWorkers: overview?.workers?.total_seen ?? 0,
    };
  }, [overview]);

  const connectionLabel =
    status === "online" || status === "refreshing"
      ? "Live"
      : status === "offline"
        ? overview
          ? "Stale"
          : "Offline"
        : "Loading";

  return (
    <main className="dashboard-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Workato Take-Home</p>
          <h1>Order Pipeline Dashboard</h1>
        </div>
        <div className="topbar-meta" aria-live="polite">
          <span className={`connection ${status}`}>{connectionLabel}</span>
          <span>Last refresh {lastRefreshAt ? formatTime(lastRefreshAt) : "pending"}</span>
          <span>
            Workers {numberText(totals.activeWorkers)}/{numberText(totals.seenWorkers)}
          </span>
        </div>
      </header>

      {error ? (
        <div className="alert" role="status">
          API connection issue: {error}
        </div>
      ) : null}

      <section className="summary-grid" aria-label="Pipeline summary">
        <Metric label="Orders" value={numberText(totals.totalOrders)} detail="total created" />
        <Metric
          label="Delivered"
          value={numberText(totals.deliveredOrders)}
          detail="terminal success"
          tone="good"
        />
        <Metric
          label="Due Work"
          value={numberText(totals.duePending)}
          detail="pending now"
          tone={totals.duePending > 0 ? "watch" : "neutral"}
        />
        <Metric
          label="Task Issues"
          value={numberText(totals.failedTasks + totals.expiredRunning)}
          detail="failed or expired"
          tone={totals.failedTasks + totals.expiredRunning > 0 ? "bad" : "neutral"}
        />
      </section>

      <div className="content-grid">
        <LifecyclePanel overview={overview} />
        <TaskHealthPanel overview={overview} />
      </div>

      <WorkerPanel overview={overview} />

      <div className="content-grid wide">
        <ProblemTaskPanel overview={overview} />
        <RecentOrdersPanel overview={overview} />
      </div>

      <RecentEventsPanel overview={overview} />
    </main>
  );
}

function LifecyclePanel({ overview }) {
  const counts = overview?.orders?.by_state ?? {};
  const maxCount = Math.max(1, ...ORDER_STATES.map((state) => counts[state] ?? 0));

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Lifecycle</h2>
        <span>{numberText(overview?.orders?.total ?? 0)} total</span>
      </div>
      <div className="state-list">
        {ORDER_STATES.map((state) => {
          const count = counts[state] ?? 0;
          const width = count > 0 ? `${Math.max(8, (count / maxCount) * 100)}%` : "0%";
          return (
            <div className="state-row" key={state}>
              <div className="state-label">
                <span>{humanize(state)}</span>
                <strong>{numberText(count)}</strong>
              </div>
              <div className="bar-track">
                <span className={`bar-fill state-${state}`} style={{ width }} />
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function TaskHealthPanel({ overview }) {
  const counts = overview?.tasks?.by_status ?? {};

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Task Health</h2>
        <span>{numberText(overview?.tasks?.total ?? 0)} total</span>
      </div>
      <div className="status-grid">
        {TASK_STATUSES.map((taskStatus) => (
          <div className="status-count" key={taskStatus}>
            <span>{humanize(taskStatus)}</span>
            <strong>{numberText(counts[taskStatus] ?? 0)}</strong>
          </div>
        ))}
      </div>
      <div className="health-lines">
        <div>
          <span>Due pending</span>
          <strong>{numberText(overview?.tasks?.due_pending ?? 0)}</strong>
        </div>
        <div>
          <span>Expired running</span>
          <strong>{numberText(overview?.tasks?.expired_running ?? 0)}</strong>
        </div>
      </div>
    </section>
  );
}

function WorkerPanel({ overview }) {
  const rows = overview?.workers?.rows ?? [];

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Worker Heartbeats</h2>
        <span>{numberText(overview?.workers?.active_threshold_seconds ?? 30)}s active window</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Worker</th>
              <th>Host</th>
              <th>Last Seen</th>
              <th>Started</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((worker) => (
                <tr key={worker.worker_id}>
                  <td title={worker.worker_id}>{shortId(worker.worker_id)}</td>
                  <td>{worker.hostname ?? "Unknown"}</td>
                  <td>{formatAge(worker.last_seen_seconds_ago)}</td>
                  <td>{formatTime(worker.started_at)}</td>
                  <td>
                    <StatusBadge active={worker.active}>
                      {worker.active ? "Active" : "Stale"}
                    </StatusBadge>
                  </td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={5}>No worker heartbeats yet</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ProblemTaskPanel({ overview }) {
  const rows = overview?.problem_tasks ?? [];

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Problem Tasks</h2>
        <span>{numberText(rows.length)} shown</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Reason</th>
              <th>Task</th>
              <th>Order</th>
              <th>Attempts</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((task) => (
                <tr key={task.task_id}>
                  <td>{humanize(task.problem_reason)}</td>
                  <td>{humanize(task.task_type)}</td>
                  <td title={task.order_id}>{task.idempotency_key}</td>
                  <td>
                    {numberText(task.attempts)}/{numberText(task.max_attempts)}
                  </td>
                  <td className="truncate" title={task.last_error ?? ""}>
                    {task.last_error ?? "None"}
                  </td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={5}>No failed, expired, or error-bearing due tasks</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function RecentOrdersPanel({ overview }) {
  const rows = overview?.recent_orders ?? [];

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Recent Orders</h2>
        <span>{numberText(rows.length)} shown</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Key</th>
              <th>State</th>
              <th>Restaurant</th>
              <th>Courier</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((order) => (
                <tr key={order.order_id}>
                  <td title={order.order_id}>{order.idempotency_key}</td>
                  <td>{humanize(order.state)}</td>
                  <td>{order.restaurant_ref}</td>
                  <td>{order.courier_ref ?? "None"}</td>
                  <td>{formatTime(order.updated_at)}</td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={5}>No orders yet</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function RecentEventsPanel({ overview }) {
  const rows = overview?.recent_events ?? [];

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Recent Events</h2>
        <span>{numberText(rows.length)} shown</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Event</th>
              <th>Transition</th>
              <th>Order</th>
              <th>Worker</th>
              <th>Occurred</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((event) => (
                <tr key={event.event_id}>
                  <td>{humanize(event.event_type)}</td>
                  <td>
                    {event.from_state || event.to_state
                      ? `${humanize(event.from_state)} -> ${humanize(event.to_state)}`
                      : "None"}
                  </td>
                  <td title={event.order_id}>{event.idempotency_key}</td>
                  <td title={event.worker_id ?? ""}>{shortId(event.worker_id)}</td>
                  <td>{formatTime(event.occurred_at)}</td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={5}>No events yet</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default App;
