import { useEffect, useMemo, useState } from "react";

import "./styles.css";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080").replace(
  /\/$/,
  "",
);
// The dashboard reads pipeline state from the API but controls order creation
// through the separate loadgen service, so both browser-visible base URLs are
// configurable for Docker Compose and local development.
const LOADGEN_BASE_URL = (
  import.meta.env.VITE_LOADGEN_BASE_URL ?? "http://localhost:8082"
).replace(/\/$/, "");
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

function rateText(value) {
  const parsed = Number(value ?? 0);
  return parsed.toLocaleString(undefined, {
    maximumFractionDigits: 1,
  });
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

async function requestLoadgen(path, { method = "GET", body, signal } = {}) {
  const response = await fetch(`${LOADGEN_BASE_URL}${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  if (!response.ok) {
    let message = `loadgen returned ${response.status}`;
    try {
      const payload = await response.json();
      message = payload.detail?.message ?? payload.detail ?? message;
    } catch {
      message = `${message}: ${response.statusText}`;
    }
    throw new Error(String(message));
  }
  return response.json();
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
  const [loadgenStatus, setLoadgenStatus] = useState(null);
  const [loadgenConnection, setLoadgenConnection] = useState("loading");
  const [loadgenError, setLoadgenError] = useState("");
  const [loadgenActionError, setLoadgenActionError] = useState("");
  const [loadgenLastRefreshAt, setLoadgenLastRefreshAt] = useState(null);
  const [loadgenActionPending, setLoadgenActionPending] = useState("");
  const [loadgenForm, setLoadgenForm] = useState({
    ratePerSecond: "20",
    maxOrders: "100",
    restaurantRef: "dashboard-manual",
    customerRefPrefix: "dashboard-customer",
  });

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

  useEffect(() => {
    let stopped = false;
    let timerId = null;
    let controller = null;

    // Poll loadgen independently from /dashboard/overview. Load generation can
    // be unavailable while the API is healthy, and the control panel should
    // show that without marking the whole dashboard offline.
    async function loadStatus() {
      controller = new AbortController();
      setLoadgenConnection((current) =>
        current === "online" ? "refreshing" : "loading",
      );

      try {
        const payload = await requestLoadgen("/status", {
          signal: controller.signal,
        });
        if (stopped) {
          return;
        }

        setLoadgenStatus(payload);
        setLoadgenConnection("online");
        setLoadgenError("");
        setLoadgenLastRefreshAt(new Date());
      } catch (caughtError) {
        if (stopped || caughtError.name === "AbortError") {
          return;
        }

        setLoadgenConnection("offline");
        setLoadgenError(caughtError.message || "loadgen request failed");
      } finally {
        if (!stopped) {
          timerId = window.setTimeout(loadStatus, POLL_INTERVAL_MS);
        }
      }
    }

    loadStatus();

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
      tasksCompletedPerSecond:
        overview?.throughput?.tasks_completed_per_second ?? 0,
      tasksCompletedRecent: overview?.throughput?.tasks_completed ?? 0,
      throughputWindowSeconds: overview?.throughput?.window_seconds ?? 30,
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

  function updateLoadgenForm(field, value) {
    setLoadgenForm((current) => ({ ...current, [field]: value }));
  }

  function readPositiveNumber(value, label) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error(`${label} must be greater than zero`);
    }
    return parsed;
  }

  function readMaxOrders(value) {
    if (!value.trim()) {
      // The loadgen API treats null max_orders as an intentionally unbounded
      // run; a blank field is the operator-facing way to request that mode.
      return null;
    }

    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new Error("Max orders must be a positive integer");
    }
    return parsed;
  }

  async function runLoadgenAction(actionName, request) {
    setLoadgenActionPending(actionName);
    setLoadgenActionError("");
    try {
      const payload = await request();
      // Control endpoints return the same status shape as GET /status. Replacing
      // local status immediately keeps the UI responsive between poll ticks.
      setLoadgenStatus(payload);
      setLoadgenConnection("online");
      setLoadgenError("");
      setLoadgenLastRefreshAt(new Date());
    } catch (caughtError) {
      setLoadgenActionError(caughtError.message || "loadgen action failed");
    } finally {
      setLoadgenActionPending("");
    }
  }

  async function startLoadgen() {
    await runLoadgenAction("start", async () => {
      const restaurantRef = loadgenForm.restaurantRef.trim();
      const customerRefPrefix = loadgenForm.customerRefPrefix.trim();
      if (!restaurantRef) {
        throw new Error("Restaurant ref is required");
      }
      if (!customerRefPrefix) {
        throw new Error("Customer prefix is required");
      }

      return requestLoadgen("/load/start", {
        method: "POST",
        body: {
          rate_per_second: readPositiveNumber(
            loadgenForm.ratePerSecond,
            "Rate",
          ),
          max_orders: readMaxOrders(loadgenForm.maxOrders),
          restaurant_ref: restaurantRef,
          customer_ref_prefix: customerRefPrefix,
        },
      });
    });
  }

  async function stopLoadgen() {
    await runLoadgenAction("stop", () =>
      requestLoadgen("/load/stop", { method: "POST" }),
    );
  }

  async function updateLoadgenRate() {
    await runLoadgenAction("rate", () =>
      requestLoadgen("/load/rate", {
        method: "PATCH",
        body: {
          rate_per_second: readPositiveNumber(loadgenForm.ratePerSecond, "Rate"),
        },
      }),
    );
  }

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
          label="Task Rate"
          value={`${rateText(totals.tasksCompletedPerSecond)}/s`}
          detail={`${numberText(totals.tasksCompletedRecent)} in last ${
            totals.throughputWindowSeconds
          }s`}
          tone={totals.tasksCompletedPerSecond > 0 ? "good" : "neutral"}
        />
        <Metric
          label="Task Issues"
          value={numberText(totals.failedTasks + totals.expiredRunning)}
          detail="failed or expired"
          tone={totals.failedTasks + totals.expiredRunning > 0 ? "bad" : "neutral"}
        />
      </section>

      <LoadgenControlPanel
        actionError={loadgenActionError}
        actionPending={loadgenActionPending}
        connection={loadgenConnection}
        error={loadgenError}
        form={loadgenForm}
        lastRefreshAt={loadgenLastRefreshAt}
        onChange={updateLoadgenForm}
        onStart={startLoadgen}
        onStop={stopLoadgen}
        onUpdateRate={updateLoadgenRate}
        status={loadgenStatus}
      />

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

function LoadgenControlPanel({
  actionError,
  actionPending,
  connection,
  error,
  form,
  lastRefreshAt,
  onChange,
  onStart,
  onStop,
  onUpdateRate,
  status,
}) {
  const running = Boolean(status?.running);
  const disabled = Boolean(actionPending);
  const connectionLabel =
    connection === "online" || connection === "refreshing"
      ? "Connected"
      : connection === "offline"
        ? "Offline"
        : "Loading";

  return (
    <section className="section loadgen-panel">
      <div className="section-heading">
        <h2>Load Generator</h2>
        <span>{lastRefreshAt ? `Updated ${formatTime(lastRefreshAt)}` : "Status pending"}</span>
      </div>

      <div className="loadgen-layout">
        <div className="loadgen-controls">
          <label className="field">
            <span>Rate / second</span>
            <input
              min="0.1"
              step="0.1"
              type="number"
              value={form.ratePerSecond}
              onChange={(event) => onChange("ratePerSecond", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Max orders</span>
            <input
              min="1"
              step="1"
              type="number"
              value={form.maxOrders}
              onChange={(event) => onChange("maxOrders", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Restaurant ref</span>
            <input
              type="text"
              value={form.restaurantRef}
              onChange={(event) => onChange("restaurantRef", event.target.value)}
            />
          </label>
          <label className="field">
            <span>Customer prefix</span>
            <input
              type="text"
              value={form.customerRefPrefix}
              onChange={(event) => onChange("customerRefPrefix", event.target.value)}
            />
          </label>
        </div>

        <div className="loadgen-actions">
          <div className="button-row">
            <button
              className="button primary"
              disabled={disabled || running}
              type="button"
              onClick={onStart}
            >
              {actionPending === "start" ? "Starting" : "Start"}
            </button>
            <button
              className="button danger"
              disabled={disabled || !running}
              type="button"
              onClick={onStop}
            >
              {actionPending === "stop" ? "Stopping" : "Stop"}
            </button>
            <button
              className="button"
              disabled={disabled || !running}
              type="button"
              onClick={onUpdateRate}
            >
              {actionPending === "rate" ? "Updating" : "Update Rate"}
            </button>
          </div>

          <dl className="loadgen-stats">
            <div>
              <dt>Status</dt>
              <dd>
                <span className={`run-status ${running ? "running" : "stopped"}`}>
                  {running ? "Running" : "Stopped"}
                </span>
              </dd>
            </div>
            <div>
              <dt>Connection</dt>
              <dd>{connectionLabel}</dd>
            </div>
            <div>
              <dt>Attempted</dt>
              <dd>{numberText(status?.attempted_count ?? 0)}</dd>
            </div>
            <div>
              <dt>Created</dt>
              <dd>{numberText(status?.created_count ?? 0)}</dd>
            </div>
            <div>
              <dt>Failed</dt>
              <dd>{numberText(status?.failed_count ?? 0)}</dd>
            </div>
            <div>
              <dt>Inflight</dt>
              <dd>
                {numberText(status?.inflight_count ?? 0)}/
                {numberText(status?.max_inflight ?? 0)}
              </dd>
            </div>
          </dl>

          {status?.last_error ? <p className="inline-error">{status.last_error}</p> : null}
          {error ? <p className="inline-error">{error}</p> : null}
          {actionError ? <p className="inline-error">{actionError}</p> : null}
        </div>
      </div>
    </section>
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
