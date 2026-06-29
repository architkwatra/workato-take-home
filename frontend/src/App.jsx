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
const DOWNSTREAM_SIM_BASE_URL = (
  import.meta.env.VITE_DOWNSTREAM_SIM_BASE_URL ?? "http://localhost:8081"
).replace(/\/$/, "");
// Polling keeps the dashboard operationally useful without adding websocket
// infrastructure to this small local demo slice.
const POLL_INTERVAL_MS = 3000;
// The simulator controls the minimum/maximum stage delay, but workers only
// observe readiness by polling and then committing an event. Allow a small
// scheduling buffer before marking a reached stage as outside the expected
// window, otherwise normal poll timing can look like a failed delay check.
const PIPELINE_VERIFICATION_TOLERANCE_SECONDS = 3;
const ORDER_DETAIL_ROUTE_PREFIX = "/orders/";

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

const TERMINAL_ORDER_STATES = new Set(["delivered", "cancelled", "failed"]);

const PIPELINE_STATES = [
  "placed",
  "confirmed",
  "preparing",
  "ready",
  "out_for_delivery",
  "delivered",
];

const PIPELINE_DELAY_WINDOWS = {
  confirmed: {
    label: "restaurant confirmation",
    min: numberEnv("VITE_RESTAURANT_CONFIRM_AFTER_SECONDS_MIN", 1),
    max: numberEnv("VITE_RESTAURANT_CONFIRM_AFTER_SECONDS_MAX", 8),
  },
  preparing: {
    label: "restaurant start-prep",
    min: numberEnv("VITE_RESTAURANT_START_PREP_AFTER_SECONDS_MIN", 1),
    max: numberEnv("VITE_RESTAURANT_START_PREP_AFTER_SECONDS_MAX", 8),
  },
  ready: {
    label: "restaurant ready",
    min: numberEnv("VITE_RESTAURANT_READY_AFTER_SECONDS_MIN", 5),
    max: numberEnv("VITE_RESTAURANT_READY_AFTER_SECONDS_MAX", 30),
  },
  out_for_delivery: {
    label: "courier assignment",
    min: numberEnv("VITE_COURIER_ASSIGN_AFTER_SECONDS_MIN", 1),
    max: numberEnv("VITE_COURIER_ASSIGN_AFTER_SECONDS_MAX", 10),
  },
  delivered: {
    label: "courier delivery",
    min: numberEnv("VITE_COURIER_DELIVERED_AFTER_SECONDS_MIN", 10),
    max: numberEnv("VITE_COURIER_DELIVERED_AFTER_SECONDS_MAX", 60),
  },
};

const TASK_STATUSES = ["pending", "running", "completed", "failed", "cancelled"];
const DOWNSTREAM_SERVICES = [
  "restaurant_confirm",
  "restaurant_start_prep",
  "restaurant_check_ready",
  "courier_assign",
  "courier_check_delivery",
];

const LABELS = {
  out_for_delivery: "out for delivery",
  check_delivery: "check delivery",
  check_pickup: "check pickup",
  check_ready: "check ready",
  advance_state: "advance state",
};

const METRIC_HELP = {
  orders: "Total order rows in the database, across every order state.",
  delivered: "Orders whose current state is delivered.",
  taskRate:
    "Completed tasks in the last dashboard throughput window divided by that window size.",
  taskIssues:
    "Failed tasks plus running tasks whose worker lease has expired.",
  workers:
    "Workers with a heartbeat in the active window divided by the configured worker replica count.",
};

function humanize(value) {
  if (!value) {
    return "None";
  }
  return LABELS[value] ?? value.replaceAll("_", " ");
}

function numberEnv(name, fallback) {
  const parsed = Number(import.meta.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
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

function millisecondsBetween(firstValue, secondValue) {
  const firstTime = new Date(firstValue).getTime();
  const secondTime = new Date(secondValue).getTime();
  if (!Number.isFinite(firstTime) || !Number.isFinite(secondTime)) {
    return null;
  }

  return Math.abs(secondTime - firstTime);
}

function formatDelaySeconds(seconds) {
  if (seconds == null) {
    return "Pending";
  }

  return `${seconds}s`;
}

function delayWindowText(delayWindow) {
  if (!delayWindow) {
    return "No downstream delay.";
  }

  // These are the simulator's configured per-stage delay bounds, not the
  // observed timestamps from this order's event history.
  return `Min: ${formatDelaySeconds(delayWindow.min)}. Max: ${formatDelaySeconds(
    delayWindow.max,
  )}.`;
}

function pipelineVerification({ reachedAt, previousReachedAt, deltaMilliseconds, delayWindow }) {
  if (!reachedAt) {
    return {
      label: "Pending",
      status: "pending",
    };
  }

  if (!delayWindow || !previousReachedAt || deltaMilliseconds == null) {
    return {
      label: "Created",
      status: "neutral",
    };
  }

  const elapsedSeconds = deltaMilliseconds / 1000;
  const minSeconds = delayWindow.min;
  const maxSeconds = delayWindow.max + PIPELINE_VERIFICATION_TOLERANCE_SECONDS;
  const verified = elapsedSeconds >= minSeconds && elapsedSeconds <= maxSeconds;

  return verified
    ? {
        label: "Verified",
        status: "verified",
      }
    : {
        label: "Not verified",
        status: "unverified",
      };
}

function pipelineDeltaTooltip({ delayWindow, verification }) {
  return `${delayWindowText(delayWindow)} ${verification.label}.`;
}

function orderIdFromPath(pathname) {
  if (!pathname.startsWith(ORDER_DETAIL_ROUTE_PREFIX)) {
    return null;
  }

  const encodedOrderId = pathname.slice(ORDER_DETAIL_ROUTE_PREFIX.length);
  if (!encodedOrderId || encodedOrderId.includes("/")) {
    return null;
  }

  return decodeURIComponent(encodedOrderId);
}

function selectedOrderIdFromLocation() {
  if (typeof window === "undefined") {
    return null;
  }

  return orderIdFromPath(window.location.pathname);
}

function orderDetailPath(orderId) {
  return `${ORDER_DETAIL_ROUTE_PREFIX}${encodeURIComponent(orderId)}`;
}

function pushRoute(path) {
  // This dashboard has one lightweight detail route, so native history keeps the
  // URL shareable without adding a routing dependency for this small page split.
  if (typeof window === "undefined" || window.location.pathname === path) {
    return;
  }

  window.history.pushState({}, "", path);
}

function connectionLabelFor(status, hasPayload) {
  if (status === "online" || status === "refreshing") {
    return "Live";
  }

  if (status === "offline") {
    return hasPayload ? "Stale" : "Offline";
  }

  return "Loading";
}

function shouldUseNativeLinkNavigation(event) {
  // Real links preserve expected browser behavior: Command-click, Ctrl-click,
  // Shift-click, and middle-click can open the order page in a new tab/window.
  return (
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  );
}

async function requestDashboard(path, { method = "GET", body, signal } = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  if (!response.ok) {
    let message = `API returned ${response.status}`;
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

async function requestDownstreamSim(path, { method = "GET", body, signal } = {}) {
  const response = await fetch(`${DOWNSTREAM_SIM_BASE_URL}${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  if (!response.ok) {
    let message = `downstream-sim returned ${response.status}`;
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

function mergeKillSwitchState(currentServices, updatedService) {
  const servicesByName = new Map(
    currentServices.map((serviceState) => [serviceState.service, serviceState]),
  );
  servicesByName.set(updatedService.service, updatedService);

  return DOWNSTREAM_SERVICES.map(
    (serviceName) =>
      servicesByName.get(serviceName) ?? {
        service: serviceName,
        killed: false,
        status: "unknown",
      },
  );
}

function Metric({ label, value, detail, helpText, tone = "neutral" }) {
  return (
    <section className={`metric ${tone}`}>
      <p className="metric-label">
        <span>{label}</span>
        {helpText ? (
          <span
            aria-label={helpText}
            className="metric-help"
            data-tooltip={helpText}
            tabIndex="0"
          >
            i
          </span>
        ) : null}
      </p>
      <strong>{value}</strong>
      {detail ? <span className="metric-detail">{detail}</span> : null}
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
  const [overviewRefreshToken, setOverviewRefreshToken] = useState(0);
  const [selectedOrderId, setSelectedOrderId] = useState(selectedOrderIdFromLocation);
  const [orderDetail, setOrderDetail] = useState(null);
  const [orderDetailStatus, setOrderDetailStatus] = useState("idle");
  const [orderDetailError, setOrderDetailError] = useState("");
  const [orderDetailLastRefreshAt, setOrderDetailLastRefreshAt] = useState(null);
  const [orderDetailRefreshToken, setOrderDetailRefreshToken] = useState(0);
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
  const [downstreamServices, setDownstreamServices] = useState([]);
  const [downstreamConnection, setDownstreamConnection] = useState("loading");
  const [downstreamError, setDownstreamError] = useState("");
  const [downstreamActionError, setDownstreamActionError] = useState("");
  const [downstreamLastRefreshAt, setDownstreamLastRefreshAt] = useState(null);
  const [downstreamActionPending, setDownstreamActionPending] = useState("");
  const [retryActionPendingOrderId, setRetryActionPendingOrderId] = useState("");
  const [retryActionError, setRetryActionError] = useState("");
  const [cancelActionPendingOrderId, setCancelActionPendingOrderId] = useState("");
  const [cancelActionError, setCancelActionError] = useState("");

  useEffect(() => {
    function handlePopState() {
      setSelectedOrderId(selectedOrderIdFromLocation());
    }

    window.addEventListener("popstate", handlePopState);

    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  useEffect(() => {
    let stopped = false;
    let timerId = null;
    let controller = null;

    async function loadOverview() {
      controller = new AbortController();
      setStatus((current) => (current === "online" ? "refreshing" : "loading"));

      try {
        const payload = await requestDashboard("/dashboard/overview", {
          signal: controller.signal,
        });
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
  }, [overviewRefreshToken]);

  useEffect(() => {
    let stopped = false;
    let timerId = null;
    let controller = null;

    // Kill switches live in downstream-sim memory, so poll that service
    // directly instead of coupling this small operator control to Postgres.
    async function loadKillSwitches() {
      controller = new AbortController();
      setDownstreamConnection((current) =>
        current === "online" ? "refreshing" : "loading",
      );

      try {
        const payload = await requestDownstreamSim("/control/kill-switches", {
          signal: controller.signal,
        });
        if (stopped) {
          return;
        }

        setDownstreamServices(payload.services ?? []);
        setDownstreamConnection("online");
        setDownstreamError("");
        setDownstreamLastRefreshAt(new Date());
      } catch (caughtError) {
        if (stopped || caughtError.name === "AbortError") {
          return;
        }

        setDownstreamConnection("offline");
        setDownstreamError(caughtError.message || "downstream-sim request failed");
      } finally {
        if (!stopped) {
          timerId = window.setTimeout(loadKillSwitches, POLL_INTERVAL_MS);
        }
      }
    }

    loadKillSwitches();

    return () => {
      stopped = true;
      window.clearTimeout(timerId);
      controller?.abort();
    };
  }, []);

  useEffect(() => {
    if (!selectedOrderId) {
      setOrderDetail(null);
      setOrderDetailStatus("idle");
      setOrderDetailError("");
      setOrderDetailLastRefreshAt(null);
      return undefined;
    }

    let stopped = false;
    let timerId = null;
    let controller = null;

    async function loadOrderDetail() {
      controller = new AbortController();
      setOrderDetailStatus((current) =>
        current === "online" ? "refreshing" : "loading",
      );

      try {
        const payload = await requestDashboard(
          `/dashboard/orders/${encodeURIComponent(selectedOrderId)}`,
          {
            signal: controller.signal,
          },
        );
        if (stopped) {
          return;
        }

        setOrderDetail(payload);
        setOrderDetailStatus("online");
        setOrderDetailError("");
        setOrderDetailLastRefreshAt(new Date());
      } catch (caughtError) {
        if (stopped || caughtError.name === "AbortError") {
          return;
        }

        setOrderDetailStatus("offline");
        setOrderDetailError(caughtError.message || "order detail request failed");
      } finally {
        if (!stopped) {
          timerId = window.setTimeout(loadOrderDetail, POLL_INTERVAL_MS);
        }
      }
    }

    setOrderDetail(null);
    loadOrderDetail();

    return () => {
      stopped = true;
      window.clearTimeout(timerId);
      controller?.abort();
    };
  }, [selectedOrderId, orderDetailRefreshToken]);

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
      expiredRunning: overview?.tasks?.expired_running ?? 0,
      tasksCompletedPerSecond:
        overview?.throughput?.tasks_completed_per_second ?? 0,
      tasksCompletedRecent: overview?.throughput?.tasks_completed ?? 0,
      throughputWindowSeconds: overview?.throughput?.window_seconds ?? 30,
      activeWorkers: overview?.workers?.active_count ?? 0,
      configuredWorkers: overview?.workers?.configured_count ?? 0,
      workerActiveThresholdSeconds: overview?.workers?.active_threshold_seconds ?? 30,
    };
  }, [overview]);

  const pageStatus = selectedOrderId ? orderDetailStatus : status;
  const pageLastRefreshAt = selectedOrderId
    ? orderDetailLastRefreshAt
    : lastRefreshAt;
  const connectionLabel = connectionLabelFor(
    pageStatus,
    Boolean(selectedOrderId ? orderDetail : overview),
  );

  function updateLoadgenForm(field, value) {
    setLoadgenForm((current) => ({ ...current, [field]: value }));
  }

  function openOrderDetail(orderId, event) {
    if (event && shouldUseNativeLinkNavigation(event)) {
      return;
    }

    event?.preventDefault();
    pushRoute(orderDetailPath(orderId));
    setSelectedOrderId(orderId);
  }

  function closeOrderDetail(event) {
    if (event && shouldUseNativeLinkNavigation(event)) {
      return;
    }

    event?.preventDefault();
    pushRoute("/");
    setSelectedOrderId(null);
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

  async function runDownstreamAction(actionName, request) {
    setDownstreamActionPending(actionName);
    setDownstreamActionError("");
    try {
      const payload = await request();
      setDownstreamServices((current) => mergeKillSwitchState(current, payload));
      setDownstreamConnection("online");
      setDownstreamError("");
      setDownstreamLastRefreshAt(new Date());
    } catch (caughtError) {
      setDownstreamActionError(
        caughtError.message || "downstream-sim action failed",
      );
    } finally {
      setDownstreamActionPending("");
    }
  }

  async function setDownstreamKilled(serviceName, killed) {
    await runDownstreamAction(`${serviceName}:${killed ? "kill" : "restore"}`, () =>
      requestDownstreamSim(`/control/kill-switches/${serviceName}`, {
        method: "POST",
        body: { killed },
      }),
    );
  }

  async function retryFailedTasks(orderId) {
    setRetryActionPendingOrderId(orderId);
    setRetryActionError("");

    try {
      await requestDashboard(`/orders/${encodeURIComponent(orderId)}/tasks/retry-failed`, {
        method: "POST",
      });
      setOverviewRefreshToken((current) => current + 1);
      if (selectedOrderId) {
        setOrderDetailRefreshToken((current) => current + 1);
      }
    } catch (caughtError) {
      setRetryActionError(caughtError.message || "retry failed tasks request failed");
    } finally {
      setRetryActionPendingOrderId("");
    }
  }

  async function cancelOrder(orderId) {
    const confirmed = window.confirm(
      "Cancel this order? This stops any remaining pipeline work for it.",
    );
    if (!confirmed) {
      return;
    }

    setCancelActionPendingOrderId(orderId);
    setCancelActionError("");

    try {
      await requestDashboard(`/orders/${encodeURIComponent(orderId)}/cancel`, {
        method: "POST",
      });
      setOverviewRefreshToken((current) => current + 1);
      if (selectedOrderId) {
        setOrderDetailRefreshToken((current) => current + 1);
      }
    } catch (caughtError) {
      setCancelActionError(caughtError.message || "cancel order request failed");
    } finally {
      setCancelActionPendingOrderId("");
    }
  }

  return (
    <main className="dashboard-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Workato Take-Home</p>
          <h1>Order Pipeline Dashboard</h1>
        </div>
        <div className="topbar-meta" aria-live="polite">
          <span className={`connection ${pageStatus}`}>{connectionLabel}</span>
          <span>
            Last refresh {pageLastRefreshAt ? formatTime(pageLastRefreshAt) : "pending"}
          </span>
        </div>
      </header>

      {error ? (
        <div className="alert" role="status">
          API connection issue: {error}
        </div>
      ) : null}

      {selectedOrderId ? (
        <OrderDetailPage
          detail={orderDetail}
          error={orderDetailError}
          onBack={closeOrderDetail}
          onCancelOrder={cancelOrder}
          onRetryFailedTasks={retryFailedTasks}
          cancelActionError={cancelActionError}
          cancelActionPendingOrderId={cancelActionPendingOrderId}
          retryActionError={retryActionError}
          retryActionPendingOrderId={retryActionPendingOrderId}
        />
      ) : (
        <>
          <section className="summary-grid" aria-label="Pipeline summary">
            <Metric
              label="Orders"
              value={numberText(totals.totalOrders)}
              detail="total created"
              helpText={METRIC_HELP.orders}
            />
            <Metric
              label="Delivered"
              value={numberText(totals.deliveredOrders)}
              detail="terminal success"
              helpText={METRIC_HELP.delivered}
              tone="good"
            />
            <Metric
              label="Task Rate"
              value={`${rateText(totals.tasksCompletedPerSecond)}/s`}
              detail={`${numberText(totals.tasksCompletedRecent)} in last ${
                totals.throughputWindowSeconds
              }s`}
              helpText={METRIC_HELP.taskRate}
              tone={totals.tasksCompletedPerSecond > 0 ? "good" : "neutral"}
            />
            <Metric
              label="Task Issues"
              value={numberText(totals.failedTasks + totals.expiredRunning)}
              detail="failed or expired"
              helpText={METRIC_HELP.taskIssues}
              tone={totals.failedTasks + totals.expiredRunning > 0 ? "bad" : "neutral"}
            />
            <Metric
              label="Workers"
              value={`${numberText(totals.activeWorkers)}/${numberText(
                totals.configuredWorkers,
              )}`}
              detail={`${numberText(totals.workerActiveThresholdSeconds)}s active window`}
              helpText={METRIC_HELP.workers}
              tone={
                totals.activeWorkers >= totals.configuredWorkers ? "good" : "watch"
              }
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
            status={loadgenStatus}
          />

          <div className="content-grid">
            <LifecyclePanel overview={overview} />
            <RecentOrdersPanel overview={overview} onSelectOrder={openOrderDetail} />
          </div>

          <DownstreamControlPanel
            actionError={downstreamActionError}
            actionPending={downstreamActionPending}
            connection={downstreamConnection}
            error={downstreamError}
            lastRefreshAt={downstreamLastRefreshAt}
            onToggle={setDownstreamKilled}
            services={downstreamServices}
          />

          <div className="content-grid wide">
            <ProblemTaskPanel
              overview={overview}
              onRetryFailedTasks={retryFailedTasks}
              retryActionError={retryActionError}
              retryActionPendingOrderId={retryActionPendingOrderId}
            />
            <RecentEventsPanel overview={overview} />
          </div>

          <TaskHealthPanel overview={overview} />
        </>
      )}
    </main>
  );
}

function OrderDetailPage({
  detail,
  error,
  onBack,
  onCancelOrder,
  onRetryFailedTasks,
  cancelActionError,
  cancelActionPendingOrderId,
  retryActionError,
  retryActionPendingOrderId,
}) {
  const order = detail?.order;
  const tasks = detail?.tasks ?? [];
  const events = detail?.events ?? [];
  const failedTaskCount = tasks.filter((task) => task.status === "failed").length;
  const retryPending = order?.order_id === retryActionPendingOrderId;
  const cancelPending = order?.order_id === cancelActionPendingOrderId;
  const orderActionPending = Boolean(
    retryActionPendingOrderId || cancelActionPendingOrderId,
  );
  const canCancelOrder = order && !TERMINAL_ORDER_STATES.has(order.state);
  const retryButtonText =
    failedTaskCount === 1
      ? "Retry Failed Task"
      : `Retry ${numberText(failedTaskCount)} Failed Tasks`;

  return (
    <section className="detail-page">
      <div className="detail-toolbar">
        <a className="button" href="/" onClick={onBack}>
          Back to Dashboard
        </a>
        <div className="button-row">
          {order && failedTaskCount > 0 ? (
            <button
              className="button primary"
              disabled={orderActionPending}
              type="button"
              onClick={() => onRetryFailedTasks(order.order_id)}
            >
              {retryPending ? "Retrying" : retryButtonText}
            </button>
          ) : null}
          {canCancelOrder ? (
            <button
              className="button danger"
              disabled={orderActionPending}
              type="button"
              onClick={() => onCancelOrder(order.order_id)}
            >
              {cancelPending ? "Cancelling" : "Cancel Order"}
            </button>
          ) : null}
        </div>
      </div>

      {error ? (
        <div className="alert" role="status">
          Order detail issue: {error}
        </div>
      ) : null}
      {retryActionError ? (
        <div className="alert" role="status">
          Retry issue: {retryActionError}
        </div>
      ) : null}
      {cancelActionError ? (
        <div className="alert" role="status">
          Cancel issue: {cancelActionError}
        </div>
      ) : null}

      {order ? (
        <>
          <section className="section order-summary">
            <div>
              <p className="eyebrow">Order</p>
              <h2 title={order.order_id}>{order.idempotency_key}</h2>
            </div>
            <dl className="order-meta">
              <div>
                <dt>State</dt>
                <dd>{humanize(order.state)}</dd>
              </div>
              <div>
                <dt>Restaurant</dt>
                <dd>{order.restaurant_ref}</dd>
              </div>
              <div>
                <dt>Courier</dt>
                <dd>{order.courier_ref ?? "None"}</dd>
              </div>
              <div>
                <dt>Customer</dt>
                <dd>{order.customer_ref ?? "None"}</dd>
              </div>
              <div>
                <dt>Created</dt>
                <dd>{formatTime(order.created_at)}</dd>
              </div>
              <div>
                <dt>Updated</dt>
                <dd>{formatTime(order.updated_at)}</dd>
              </div>
            </dl>
          </section>

          <PipelinePanel order={order} events={events} />

          <div className="content-grid wide">
            <OrderTasksPanel tasks={tasks} />
            <OrderEventsPanel events={events} />
          </div>
        </>
      ) : (
        <section className="section">
          <div className="empty-state">Loading order detail</div>
        </section>
      )}
    </section>
  );
}

function PipelinePanel({ order, events }) {
  const currentIndex = PIPELINE_STATES.indexOf(order.state);
  const reachedAtByState = new Map();
  for (const event of events) {
    if (event.event_type === "order_created") {
      reachedAtByState.set("placed", event.occurred_at);
    }
    if (event.to_state && !reachedAtByState.has(event.to_state)) {
      reachedAtByState.set(event.to_state, event.occurred_at);
    }
  }

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Order Pipeline</h2>
        <span>{humanize(order.state)}</span>
      </div>
      <div className="pipeline">
        {PIPELINE_STATES.map((state, index) => {
          const reachedAt = reachedAtByState.get(state);
          const previousState = PIPELINE_STATES[index - 1];
          const previousReachedAt = previousState
            ? reachedAtByState.get(previousState)
            : null;
          const deltaMilliseconds =
            reachedAt && previousReachedAt
              ? millisecondsBetween(previousReachedAt, reachedAt)
              : null;
          const delayWindow = PIPELINE_DELAY_WINDOWS[state] ?? null;
          const verification = pipelineVerification({
            reachedAt,
            previousReachedAt,
            deltaMilliseconds,
            delayWindow,
          });
          const tooltipText = pipelineDeltaTooltip({ delayWindow, verification });
          const stageStatus =
            currentIndex === -1
              ? reachedAt
                ? "done"
                : "waiting"
              : index < currentIndex
                ? "done"
                : index === currentIndex
                  ? "current"
                  : "waiting";
          return (
            <div
              aria-label={tooltipText}
              className={`pipeline-step ${stageStatus}`}
              data-tooltip={tooltipText}
              key={state}
              tabIndex="0"
              title={tooltipText}
            >
              <span className="pipeline-dot" />
              <div>
                <strong>{humanize(state)}</strong>
                <small>{reachedAt ? formatTime(reachedAt) : "Pending"}</small>
              </div>
            </div>
          );
        })}
      </div>
      {order.state === "cancelled" || order.state === "failed" ? (
        <p className="inline-error">Order ended in {humanize(order.state)}</p>
      ) : null}
    </section>
  );
}

function OrderTasksPanel({ tasks }) {
  return (
    <section className="section">
      <div className="section-heading">
        <h2>Order Tasks</h2>
        <span>{numberText(tasks.length)} shown</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Target</th>
              <th>Status</th>
              <th>Attempts</th>
              <th>Next Run</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {tasks.length ? (
              tasks.map((task) => (
                <tr key={task.task_id}>
                  <td title={task.task_id}>{humanize(task.task_type)}</td>
                  <td>{humanize(task.target_state)}</td>
                  <td>{humanize(task.status)}</td>
                  <td>
                    {numberText(task.attempts)}/{numberText(task.max_attempts)}
                  </td>
                  <td>{formatTime(task.next_run_at)}</td>
                  <td className="truncate" title={task.last_error ?? ""}>
                    {task.last_error ?? "None"}
                  </td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={6}>No task rows for this order</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function OrderEventsPanel({ events }) {
  return (
    <section className="section">
      <div className="section-heading">
        <h2>Order Events</h2>
        <span>{numberText(events.length)} shown</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Event</th>
              <th>Transition</th>
              <th>Worker</th>
            </tr>
          </thead>
          <tbody>
            {events.length ? (
              events.map((event) => (
                <tr key={event.event_id}>
                  <td>{formatTime(event.occurred_at)}</td>
                  <td>{humanize(event.event_type)}</td>
                  <td>
                    {event.from_state || event.to_state
                      ? `${humanize(event.from_state)} -> ${humanize(event.to_state)}`
                      : "None"}
                  </td>
                  <td title={event.worker_id ?? ""}>{shortId(event.worker_id)}</td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={4}>No events for this order yet</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
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

function DownstreamControlPanel({
  actionError,
  actionPending,
  connection,
  error,
  lastRefreshAt,
  onToggle,
  services,
}) {
  const serviceByName = new Map(
    services.map((serviceState) => [serviceState.service, serviceState]),
  );
  const connectionLabel =
    connection === "online" || connection === "refreshing"
      ? "Connected"
      : connection === "offline"
        ? "Offline"
        : "Loading";

  return (
    <section className="section downstream-panel">
      <div className="section-heading">
        <h2>Downstream Kill Switches</h2>
        <span>{lastRefreshAt ? `Updated ${formatTime(lastRefreshAt)}` : connectionLabel}</span>
      </div>

      <div className="service-list">
        {DOWNSTREAM_SERVICES.map((serviceName) => {
          const serviceState = serviceByName.get(serviceName);
          const killed = Boolean(serviceState?.killed);
          const pendingKill = actionPending === `${serviceName}:kill`;
          const pendingRestore = actionPending === `${serviceName}:restore`;
          const pending = pendingKill || pendingRestore;
          const disabled = Boolean(actionPending) || connection === "offline";

          return (
            <div className="service-row" key={serviceName}>
              <div>
                <strong>{humanize(serviceName)}</strong>
                <span
                  className={`service-state ${
                    killed ? "killed" : serviceState ? "online" : "unknown"
                  }`}
                >
                  {killed ? "Killed" : serviceState ? "Online" : "Unknown"}
                </span>
              </div>
              <button
                className={`button ${killed ? "primary" : "danger"}`}
                disabled={disabled}
                type="button"
                onClick={() => onToggle(serviceName, !killed)}
              >
                {pending
                  ? pendingKill
                    ? "Killing"
                    : "Restoring"
                  : killed
                    ? "Restore"
                    : "Kill"}
              </button>
            </div>
          );
        })}
      </div>

      {error ? <p className="inline-error">downstream-sim: {error}</p> : null}
      {actionError ? <p className="inline-error">{actionError}</p> : null}
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

function ProblemTaskPanel({
  overview,
  onRetryFailedTasks,
  retryActionError,
  retryActionPendingOrderId,
}) {
  const rows = overview?.problem_tasks ?? [];

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Problem Tasks</h2>
        <span>{numberText(rows.length)} shown</span>
      </div>
      {retryActionError ? (
        <p className="inline-error" role="status">
          {retryActionError}
        </p>
      ) : null}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Reason</th>
              <th>Task</th>
              <th>Order</th>
              <th>Attempts</th>
              <th>Error</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((task) => {
                const retryPending = task.order_id === retryActionPendingOrderId;
                const canRetry = task.status === "failed";

                return (
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
                    <td>
                      {canRetry ? (
                        <button
                          className="button small"
                          disabled={Boolean(retryActionPendingOrderId)}
                          type="button"
                          onClick={() => onRetryFailedTasks(task.order_id)}
                        >
                          {retryPending ? "Retrying" : "Retry"}
                        </button>
                      ) : (
                        <span className="muted-text">None</span>
                      )}
                    </td>
                  </tr>
                );
              })
            ) : (
              <EmptyRow colSpan={6}>No failed, expired, or error-bearing due tasks</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function RecentOrdersPanel({ overview, onSelectOrder }) {
  const placedRows = overview?.placed_orders ?? [];
  const rows = overview?.recent_orders ?? [];

  return (
    <section className="section">
      <div className="section-heading">
        <h2>Recent Orders</h2>
        <span>{numberText(rows.length)} shown</span>
      </div>
      <div className="placed-watch">
        <div className="placed-watch-heading">
          <span>Placed Orders</span>
          <small>{numberText(placedRows.length)} of 5 shown</small>
        </div>
        {placedRows.length ? (
          <div className="placed-watch-list">
            {placedRows.map((order) => (
              <a
                className="placed-watch-item"
                href={orderDetailPath(order.order_id)}
                key={order.order_id}
                onClick={(event) => onSelectOrder(order.order_id, event)}
              >
                <span title={order.order_id}>{order.idempotency_key}</span>
                <small>{formatTime(order.created_at)}</small>
              </a>
            ))}
          </div>
        ) : (
          <div className="placed-watch-empty">No placed orders waiting</div>
        )}
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
                  <td title={order.order_id}>
                    <a
                      className="text-button"
                      href={orderDetailPath(order.order_id)}
                      onClick={(event) => onSelectOrder(order.order_id, event)}
                    >
                      {order.idempotency_key}
                    </a>
                  </td>
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
