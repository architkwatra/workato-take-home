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
const LOADGEN_RESTAURANT_REF =
  import.meta.env.VITE_LOADGEN_RESTAURANT_REF ?? "dashboard-manual";
const LOADGEN_CUSTOMER_REF_PREFIX =
  import.meta.env.VITE_LOADGEN_CUSTOMER_REF_PREFIX ?? "dashboard-customer";
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
  due_retry: "due retry",
  pending_retry: "pending retry",
  retry_running: "retry running",
};

const METRIC_HELP = {
  orders: "Total order rows in the database, across every order state.",
  ordersCreatedRate:
    "Orders accepted per minute over the rolling dashboard window.",
  ordersDeliveredRate:
    "Orders delivered per minute over the rolling dashboard window.",
  pipelineLatency:
    "End-to-end p95 time from order creation to delivery, across orders delivered in the current latency window.",
  overdueOrders:
    "Active orders that were created more than the configured delivery SLA ago and have not reached a terminal state.",
  stuckOrders:
    "Non-terminal orders whose time in the current lifecycle state is above that stage's configured stuck threshold.",
  taskCompletionRate:
    "Completed task rows per second over the rolling dashboard window. This is worker throughput, not delivered-order rate.",
  taskIssues:
    "Failed tasks, expired running tasks, and tasks retrying after a previous error.",
  workers:
    "Workers with a heartbeat in the active window divided by the configured worker replica count.",
};

const DETAIL_HELP = {
  pipelineSamples:
    "Delivered orders in the current latency window with both an order_created event and a delivered state_transition event.",
  pipelineAvg:
    "Average elapsed time from order_created to delivered across delivered orders in the current latency window.",
  pipelineP95:
    "95th percentile elapsed time from order_created to delivered. 95% of delivered orders in the current latency window completed at or below this value.",
  stageLatency:
    "95th percentile elapsed time between consecutive lifecycle events for transitions reached in the current latency window, calculated from order_events.",
  overdueOrders:
    "Orders are marked overdue when orders.created_at is older than the configured end-to-end delivery SLA and the order is still active. Delivered, cancelled, and failed orders are excluded.",
  stuckOrders:
    "Orders are marked stuck when they are not terminal and orders.updated_at is older than the configured threshold for the current state. Delivered, cancelled, and failed orders are excluded.",
  taskCompletionRate:
    "Completed order_tasks rows per second over the rolling dashboard window. This is worker throughput, not delivered-order rate.",
  duePending:
    "Pending order_tasks rows where next_run_at is now or earlier, so a worker can claim them.",
  retrying:
    "Pending or running tasks with last_error set. Pending retry tasks wait until next_run_at; running retry tasks are currently leased.",
  expiredLeases:
    "Running tasks where locked_until is in the past. These are reclaimable by another worker.",
};

function humanize(value) {
  if (!value) {
    return "—";
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
    return "—";
  }
  return value.length > 12 ? `${value.slice(0, 8)}…` : value;
}

function shortKey(value) {
  if (!value) return "—";
  if (value.length <= 20) return value;
  // For keys like "loadgen-<uuid>-97", keep the meaningful prefix and serial suffix
  const lastDash = value.lastIndexOf("-");
  if (lastDash > 8) {
    return `${value.slice(0, 8)}…${value.slice(lastDash)}`;
  }
  return `${value.slice(0, 12)}…`;
}

function formatDuration(ms) {
  if (ms == null || !Number.isFinite(ms) || ms < 0) return null;
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return rem > 0 ? `${m}m ${rem}s` : `${m}m`;
}

function formatDurationSeconds(seconds) {
  if (seconds == null || !Number.isFinite(Number(seconds))) {
    return "—";
  }
  return formatDuration(Number(seconds) * 1000) ?? "—";
}

function formatLatencySeconds(seconds) {
  if (seconds == null || !Number.isFinite(Number(seconds))) {
    return "—";
  }

  const parsed = Number(seconds);
  if (parsed < 60) {
    return `${parsed.toFixed(1)}s`;
  }

  return formatDurationSeconds(parsed);
}

function formatWindowLabel(seconds) {
  const duration = formatDurationSeconds(seconds);
  return duration === "—" ? "current window" : `last ${duration}`;
}

function stuckThresholdText(thresholds = {}) {
  const entries = PIPELINE_STATES.slice(0, -1).map(
    (state) => `${humanize(state)} ${formatDurationSeconds(thresholds[state])}`,
  );
  return entries.join(", ");
}

function formatTime(value) {
  if (!value) {
    return "—";
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

function maxOrdersText(value) {
  return value == null ? "Unbounded" : numberText(value);
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

function networkErrorFor(serviceLabel, baseUrl, caughtError) {
  if (caughtError?.name === "AbortError") {
    return caughtError;
  }

  // Browsers collapse network failures, refused connections, and CORS blocks
  // into the same TypeError with "Failed to fetch". Add the service and URL so
  // operators know which local container or browser-visible port to check.
  if (caughtError instanceof TypeError || caughtError?.message === "Failed to fetch") {
    return new Error(
      `${serviceLabel} is unreachable at ${baseUrl}. Check that the container is running, the port is exposed, and CORS allows this dashboard origin.`,
    );
  }

  return caughtError;
}

function responseErrorMessage(serviceLabel, response, payload) {
  const fallback = `${serviceLabel} returned ${response.status}: ${response.statusText}`;
  const detail = payload?.detail;
  if (typeof detail === "string") {
    return detail;
  }
  if (detail?.message) {
    return detail.message;
  }
  return fallback;
}

function clearIfReachabilityError(message) {
  return message.includes("unreachable at") || message === "Failed to fetch"
    ? ""
    : message;
}

async function requestJson(
  serviceLabel,
  baseUrl,
  path,
  { method = "GET", body, signal } = {},
) {
  let response;
  try {
    response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
      signal,
    });
  } catch (caughtError) {
    throw networkErrorFor(serviceLabel, baseUrl, caughtError);
  }

  if (!response.ok) {
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      // Keep the fallback with HTTP status and statusText.
    }
    throw new Error(responseErrorMessage(serviceLabel, response, payload));
  }

  return response.json();
}

async function requestDashboard(path, { method = "GET", body, signal } = {}) {
  return requestJson("API", API_BASE_URL, path, {
    method,
    body,
    signal,
  });
}

async function requestLoadgen(path, { method = "GET", body, signal } = {}) {
  return requestJson("loadgen", LOADGEN_BASE_URL, path, {
    method,
    body,
    signal,
  });
}

async function requestDownstreamSim(path, { method = "GET", body, signal } = {}) {
  return requestJson("downstream-sim", DOWNSTREAM_SIM_BASE_URL, path, {
    method,
    body,
    signal,
  });
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
        {helpText ? <HelpIcon text={helpText} /> : null}
      </p>
      <strong>{value}</strong>
      {detail ? <span className="metric-detail">{detail}</span> : null}
    </section>
  );
}

function HelpIcon({ text }) {
  return (
    <span
      aria-label={text}
      className="metric-help"
      data-tooltip={text}
      tabIndex="0"
    >
      i
    </span>
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

function StateBadge({ state }) {
  const cls = `state-badge state-badge--${(state ?? "unknown").replace(/_/g, "-")}`;
  return <span className={cls}>{humanize(state ?? "unknown")}</span>;
}

function TaskStatusBadge({ status }) {
  return (
    <span className={`task-badge task-badge--${status ?? "unknown"}`}>
      {humanize(status ?? "unknown")}
    </span>
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
  });
  const [downstreamServices, setDownstreamServices] = useState([]);
  const [downstreamConnection, setDownstreamConnection] = useState("loading");
  const [downstreamError, setDownstreamError] = useState("");
  const [downstreamActionError, setDownstreamActionError] = useState("");
  const [downstreamLastRefreshAt, setDownstreamLastRefreshAt] = useState(null);
  const [downstreamActionPending, setDownstreamActionPending] = useState("");
  const [downstreamPanelOpen, setDownstreamPanelOpen] = useState(false);
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
        setRetryActionError(clearIfReachabilityError);
        setCancelActionError(clearIfReachabilityError);
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
        setDownstreamActionError(clearIfReachabilityError);
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
        setLoadgenActionError(clearIfReachabilityError);
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
      retryingPending: overview?.tasks?.retrying_pending ?? 0,
      retryingRunning: overview?.tasks?.retrying_running ?? 0,
      duePending: overview?.tasks?.due_pending ?? 0,
      ordersCreatedPerMinute:
        overview?.throughput?.orders_created_per_minute ?? 0,
      ordersDeliveredPerMinute:
        overview?.throughput?.orders_delivered_per_minute ?? 0,
      ordersCreatedRecent: overview?.throughput?.orders_created ?? 0,
      ordersDeliveredRecent: overview?.throughput?.orders_delivered ?? 0,
      tasksCompletedPerSecond:
        overview?.throughput?.tasks_completed_per_second ?? 0,
      tasksCompletedRecent: overview?.throughput?.tasks_completed ?? 0,
      throughputWindowSeconds: overview?.throughput?.window_seconds ?? 30,
      pipelineAvgSeconds: overview?.latency?.pipeline?.avg_seconds ?? null,
      pipelineP95Seconds: overview?.latency?.pipeline?.p95_seconds ?? null,
      pipelineSampleCount: overview?.latency?.pipeline?.sample_count ?? 0,
      latencyWindowSeconds: overview?.latency?.window_seconds ?? 300,
      overdueOrderCount: overview?.overdue_orders?.total ?? 0,
      orderDeliverySlaSeconds: overview?.overdue_orders?.threshold_seconds ?? 120,
      stuckOrderCount: overview?.stuck_orders?.total ?? 0,
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
  const downstreamServiceByName = new Map(
    downstreamServices.map((serviceState) => [serviceState.service, serviceState]),
  );
  const downstreamKilledCount = DOWNSTREAM_SERVICES.filter((serviceName) =>
    Boolean(downstreamServiceByName.get(serviceName)?.killed),
  ).length;
  const downstreamButtonText =
    downstreamKilledCount > 0
      ? `Downstream (${numberText(downstreamKilledCount)} killed)`
      : "Downstream";

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
      return requestLoadgen("/load/start", {
        method: "POST",
        body: {
          rate_per_second: readPositiveNumber(
            loadgenForm.ratePerSecond,
            "Rate",
          ),
          max_orders: readMaxOrders(loadgenForm.maxOrders),
          restaurant_ref: LOADGEN_RESTAURANT_REF,
          customer_ref_prefix: LOADGEN_CUSTOMER_REF_PREFIX,
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
          <div className="topbar-popover-anchor">
            <button
              aria-controls="downstream-kill-switches"
              aria-expanded={downstreamPanelOpen}
              className={`button topbar-action ${
                downstreamKilledCount > 0 ? "danger" : ""
              }`}
              type="button"
              onClick={() => setDownstreamPanelOpen((current) => !current)}
            >
              {downstreamButtonText}
            </button>
            {downstreamPanelOpen ? (
              <div className="topbar-popover" id="downstream-kill-switches">
                <DownstreamControlPanel
                  actionError={downstreamActionError}
                  actionPending={downstreamActionPending}
                  className="topbar-downstream"
                  connection={downstreamConnection}
                  error={downstreamError}
                  lastRefreshAt={downstreamLastRefreshAt}
                  onToggle={setDownstreamKilled}
                  services={downstreamServices}
                />
              </div>
            ) : null}
          </div>
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
              label="Created / Min"
              value={`${rateText(totals.ordersCreatedPerMinute)}/min`}
              detail={`${numberText(totals.ordersCreatedRecent)} in last ${
                totals.throughputWindowSeconds
              }s`}
              helpText={METRIC_HELP.ordersCreatedRate}
              tone={totals.ordersCreatedPerMinute > 0 ? "good" : "neutral"}
            />
            <Metric
              label="Delivered / Min"
              value={`${rateText(totals.ordersDeliveredPerMinute)}/min`}
              detail={`${numberText(totals.deliveredOrders)} total delivered`}
              helpText={METRIC_HELP.ordersDeliveredRate}
              tone={totals.ordersDeliveredPerMinute > 0 ? "good" : "neutral"}
            />
            <Metric
              label="Pipeline P95"
              value={formatLatencySeconds(totals.pipelineP95Seconds)}
              detail={`Avg ${formatLatencySeconds(totals.pipelineAvgSeconds)} · ${
                numberText(totals.pipelineSampleCount)
              } delivered · ${formatWindowLabel(totals.latencyWindowSeconds)}`}
              helpText={METRIC_HELP.pipelineLatency}
              tone={totals.pipelineP95Seconds == null ? "neutral" : "good"}
            />
            <Metric
              label="Overdue Orders"
              value={numberText(totals.overdueOrderCount)}
              detail={`>${formatDurationSeconds(totals.orderDeliverySlaSeconds)} since created`}
              helpText={METRIC_HELP.overdueOrders}
              tone={totals.overdueOrderCount > 0 ? "bad" : "neutral"}
            />
            <Metric
              label="Stuck Orders"
              value={numberText(totals.stuckOrderCount)}
              detail={
                totals.stuckOrderCount > 0
                  ? "over stage threshold"
                  : "none over threshold"
              }
              helpText={METRIC_HELP.stuckOrders}
              tone={totals.stuckOrderCount > 0 ? "bad" : "neutral"}
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

          <div className="content-grid wide">
            <PipelineLatencyPanel overview={overview} />
            <QueueHealthPanel totals={totals} />
          </div>

          <div className="content-grid wide">
            <OverdueOrdersPanel overview={overview} onSelectOrder={openOrderDetail} />
            <StuckOrdersPanel overview={overview} onSelectOrder={openOrderDetail} />
          </div>

          <ProblemTaskPanel
            overview={overview}
            onRetryFailedTasks={retryFailedTasks}
            retryActionError={retryActionError}
            retryActionPendingOrderId={retryActionPendingOrderId}
          />

          <RecentEventsPanel overview={overview} />
        </>
      )}
    </main>
  );
}

function PipelineLatencyPanel({ overview }) {
  const pipeline = overview?.latency?.pipeline ?? {};
  const stages = overview?.latency?.stages ?? [];
  const stagesByTarget = new Map(stages.map((stage) => [stage.to_state, stage]));
  const hasSamples = (pipeline.sample_count ?? 0) > 0;
  const latencyWindowLabel = formatWindowLabel(overview?.latency?.window_seconds);

  // Compute relative bar widths from the slowest stage p95
  const maxP95 = Math.max(
    ...PIPELINE_STATES.slice(1).map((state) => stagesByTarget.get(state)?.p95_seconds ?? 0),
    0.001,
  );

  return (
    <section className="section latency-panel">
      <div className="section-heading">
        <h2>Pipeline Latency</h2>
        <span className="label-with-help">
          {numberText(pipeline.sample_count ?? 0)} delivered · {latencyWindowLabel}
          <HelpIcon text={DETAIL_HELP.pipelineSamples} />
        </span>
      </div>
      <div className="latency-summary">
        <div>
          <span className="label-with-help">
            Avg
            <HelpIcon text={DETAIL_HELP.pipelineAvg} />
          </span>
          <strong>{hasSamples ? formatLatencySeconds(pipeline.avg_seconds) : "—"}</strong>
        </div>
        <div>
          <span className="label-with-help">
            P95
            <HelpIcon text={DETAIL_HELP.pipelineP95} />
          </span>
          <strong>{hasSamples ? formatLatencySeconds(pipeline.p95_seconds) : "—"}</strong>
        </div>
      </div>
      <div className="stage-latency-list-header">
        <span className="label-with-help">
          Stage
          <HelpIcon text={DETAIL_HELP.stageLatency} />
        </span>
        <span>P95</span>
        <span>Avg · n</span>
      </div>
      <div className="stage-latency-list">
        {PIPELINE_STATES.slice(1).map((state, index) => {
          const fromState = PIPELINE_STATES[index];
          const stage = stagesByTarget.get(state);
          const pct = stage?.p95_seconds
            ? Math.min(Math.round((stage.p95_seconds / maxP95) * 88), 88)
            : 0;
          const rowStyle = pct > 0
            ? { background: `linear-gradient(to right, rgba(37,99,111,0.08) ${pct}%, transparent ${pct}%)` }
            : {};
          return (
            <div className="stage-latency-row" key={state} style={rowStyle}>
              <span className="stage-label">
                {humanize(fromState)} → {humanize(state)}
              </span>
              <strong>{formatLatencySeconds(stage?.p95_seconds)}</strong>
              <small>
                {formatLatencySeconds(stage?.avg_seconds)}
                {" · "}n={numberText(stage?.sample_count ?? 0)}
              </small>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function QueueHealthPanel({ totals }) {
  return (
    <section className="section system-metrics-panel">
      <div className="section-heading">
        <h2>Queue Health</h2>
        <span>Worker and task queue status</span>
      </div>
      <dl className="secondary-metrics">
        <div>
          <dt>
            <span className="label-with-help">
              Task completion rate
              <HelpIcon text={DETAIL_HELP.taskCompletionRate} />
            </span>
          </dt>
          <dd>{rateText(totals.tasksCompletedPerSecond)}/s</dd>
          <small>
            {numberText(totals.tasksCompletedRecent)} in last{" "}
            {totals.throughputWindowSeconds}s
          </small>
        </div>
        <div>
          <dt>
            <span className="label-with-help">
              Due pending
              <HelpIcon text={DETAIL_HELP.duePending} />
            </span>
          </dt>
          <dd>{numberText(totals.duePending)}</dd>
          <small>claimable now</small>
        </div>
        <div>
          <dt>
            <span className="label-with-help">
              Retrying
              <HelpIcon text={DETAIL_HELP.retrying} />
            </span>
          </dt>
          <dd>
            {numberText(totals.retryingPending + totals.retryingRunning)}
          </dd>
          <small>pending or running</small>
        </div>
        <div>
          <dt>
            <span className="label-with-help">
              Expired leases
              <HelpIcon text={DETAIL_HELP.expiredLeases} />
            </span>
          </dt>
          <dd>{numberText(totals.expiredRunning)}</dd>
          <small>running past lease</small>
        </div>
      </dl>
    </section>
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
                <dd><StateBadge state={order.state} /></dd>
              </div>
              <div>
                <dt>Restaurant</dt>
                <dd>{order.restaurant_ref}</dd>
              </div>
              <div>
                <dt>Courier</dt>
                <dd>{order.courier_ref ?? "—"}</dd>
              </div>
              <div>
                <dt>Customer</dt>
                <dd>{order.customer_ref ?? "—"}</dd>
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
        <StateBadge state={order.state} />
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
                {deltaMilliseconds != null ? (
                  <small className="pipeline-duration">
                    +{formatDuration(deltaMilliseconds)}
                  </small>
                ) : null}
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
                  <td><TaskStatusBadge status={task.status} /></td>
                  <td>
                    {numberText(task.attempts)}/{numberText(task.max_attempts)}
                  </td>
                  <td>{formatTime(task.next_run_at)}</td>
                  <td className="truncate" title={task.last_error ?? ""}>
                    {task.last_error ?? "—"}
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
                      ? `${humanize(event.from_state)} → ${humanize(event.to_state)}`
                      : "—"}
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
        <div className="loadgen-top">
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
          </div>

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
            <dt>Run Total</dt>
            <dd>{maxOrdersText(status?.max_orders)}</dd>
          </div>
        </dl>

        {status?.last_error ? <p className="inline-error">{status.last_error}</p> : null}
        {error ? <p className="inline-error">{error}</p> : null}
        {actionError ? <p className="inline-error">{actionError}</p> : null}
      </div>
    </section>
  );
}

function DownstreamControlPanel({
  actionError,
  actionPending,
  className = "",
  connection,
  error,
  lastRefreshAt,
  onToggle,
  services,
}) {
  const serviceByName = new Map(
    services.map((serviceState) => [serviceState.service, serviceState]),
  );
  const killedCount = DOWNSTREAM_SERVICES.filter((serviceName) =>
    Boolean(serviceByName.get(serviceName)?.killed),
  ).length;
  const connectionLabel =
    connection === "online" || connection === "refreshing"
      ? "Connected"
      : connection === "offline"
        ? "Offline"
        : "Loading";
  const statusText = lastRefreshAt
    ? `Updated ${formatTime(lastRefreshAt)}`
    : connectionLabel;
  const killSummary =
    services.length === 0
      ? "Pending"
      : killedCount > 0
        ? `${numberText(killedCount)} killed`
        : "All enabled";

  return (
    <section className={`section downstream-panel ${className}`.trim()}>
      <div className="section-heading downstream-heading">
        <div>
          <h2>Downstream Kill Switches</h2>
          <span>{statusText}</span>
        </div>
        <strong>{killSummary}</strong>
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

function LatestTaskSummary({ item }) {
  if (!item.latest_task_type) {
    return "—";
  }

  return (
    <div className="task-stack">
      <span>
        {humanize(item.latest_task_type)}
        {item.latest_task_target_state
          ? ` → ${humanize(item.latest_task_target_state)}`
          : ""}
      </span>
      <TaskStatusBadge status={item.latest_task_status} />
    </div>
  );
}

function OverdueOrdersPanel({ overview, onSelectOrder }) {
  const overdueOverview = overview?.overdue_orders ?? {};
  const rows = overdueOverview.items ?? [];
  const total = overdueOverview.total ?? rows.length;
  const limit = overdueOverview.limit ?? rows.length;
  const thresholdSeconds = overdueOverview.threshold_seconds ?? 120;
  const thresholdLabel = formatDurationSeconds(thresholdSeconds);
  const thresholdHelp = `${DETAIL_HELP.overdueOrders} SLA: ${thresholdLabel}.`;

  return (
    <section className="section overdue-orders-panel">
      <div className="section-heading">
        <h2>Overdue Orders</h2>
        <span className="label-with-help">
          {numberText(total)} over SLA
          <HelpIcon text={thresholdHelp} />
        </span>
      </div>
      <div className="table-wrap">
        <table className="overdue-orders-table">
          <thead>
            <tr>
              <th>Order</th>
              <th>State</th>
              <th>Age</th>
              <th>Overdue By</th>
              <th>Latest Task</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((order) => (
                <tr key={order.order_id}>
                  <td>
                    <a
                      className="text-button"
                      href={orderDetailPath(order.order_id)}
                      title={order.idempotency_key}
                      onClick={(event) => onSelectOrder(order.order_id, event)}
                    >
                      {shortKey(order.idempotency_key)}
                    </a>
                  </td>
                  <td><StateBadge state={order.state} /></td>
                  <td>{formatDurationSeconds(order.age_seconds)}</td>
                  <td className="overdue-duration">
                    {formatDurationSeconds(order.overdue_seconds)}
                  </td>
                  <td><LatestTaskSummary item={order} /></td>
                  <td
                    className="truncate"
                    title={order.latest_task_last_error ?? ""}
                  >
                    {order.latest_task_last_error ?? "—"}
                  </td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={6}>No active orders are over the delivery SLA</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
      {total > rows.length ? (
        <p className="muted-text panel-footnote">
          Showing oldest {numberText(Math.min(limit, rows.length))} of{" "}
          {numberText(total)} overdue orders.
        </p>
      ) : null}
    </section>
  );
}

function StuckOrdersPanel({ overview, onSelectOrder }) {
  const stuckOverview = overview?.stuck_orders ?? {};
  const rows = stuckOverview.items ?? [];
  const total = stuckOverview.total ?? rows.length;
  const limit = stuckOverview.limit ?? rows.length;
  const thresholds = stuckOverview.thresholds_seconds ?? {};
  const thresholdHelp = `${DETAIL_HELP.stuckOrders} Thresholds: ${stuckThresholdText(
    thresholds,
  )}.`;

  return (
    <section className="section stuck-orders-panel">
      <div className="section-heading">
        <h2>Stuck Orders</h2>
        <span className="label-with-help">
          {numberText(total)} over threshold
          <HelpIcon text={thresholdHelp} />
        </span>
      </div>
      <div className="table-wrap">
        <table className="stuck-orders-table">
          <thead>
            <tr>
              <th>Order</th>
              <th>State</th>
              <th>Stuck For</th>
              <th>Threshold</th>
              <th>Latest Task</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((order) => (
                <tr key={order.order_id}>
                  <td>
                    <a
                      className="text-button"
                      href={orderDetailPath(order.order_id)}
                      title={order.idempotency_key}
                      onClick={(event) => onSelectOrder(order.order_id, event)}
                    >
                      {shortKey(order.idempotency_key)}
                    </a>
                  </td>
                  <td><StateBadge state={order.state} /></td>
                  <td className="stuck-duration">
                    {formatDurationSeconds(order.stuck_seconds)}
                  </td>
                  <td>{formatDurationSeconds(order.threshold_seconds)}</td>
                  <td><LatestTaskSummary item={order} /></td>
                  <td
                    className="truncate"
                    title={order.latest_task_last_error ?? ""}
                  >
                    {order.latest_task_last_error ?? "—"}
                  </td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={6}>No orders are over their stage threshold</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
      {total > rows.length ? (
        <p className="muted-text panel-footnote">
          Showing oldest {numberText(Math.min(limit, rows.length))} of{" "}
          {numberText(total)} stuck orders.
        </p>
      ) : null}
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
              <th>Next Run</th>
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
                    <td>
                      <span className={`reason-badge reason-badge--${task.problem_reason ?? "unknown"}`}>
                        {humanize(task.problem_reason)}
                      </span>
                    </td>
                    <td>{humanize(task.task_type)}</td>
                    <td>
                      <a
                        className="text-button"
                        href={orderDetailPath(task.order_id)}
                        title={task.idempotency_key}
                      >
                        {shortKey(task.idempotency_key)}
                      </a>
                    </td>
                    <td>
                      {numberText(task.attempts)}/{numberText(task.max_attempts)}
                    </td>
                    <td>{formatTime(task.next_run_at)}</td>
                    <td className="truncate" title={task.last_error ?? ""}>
                      {task.last_error ?? "—"}
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
                      ) : null}
                    </td>
                  </tr>
                );
              })
            ) : (
              <EmptyRow colSpan={7}>No failed, expired, or retrying tasks</EmptyRow>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function RecentOrdersPanel({ overview, onSelectOrder }) {
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
              <th>Order</th>
              <th>State</th>
              <th>Restaurant</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((order) => (
                <tr key={order.order_id}>
                  <td>
                    <a
                      className="text-button"
                      href={orderDetailPath(order.order_id)}
                      title={order.idempotency_key}
                      onClick={(event) => onSelectOrder(order.order_id, event)}
                    >
                      {shortKey(order.idempotency_key)}
                    </a>
                  </td>
                  <td><StateBadge state={order.state} /></td>
                  <td>{order.restaurant_ref}</td>
                  <td>{formatTime(order.updated_at)}</td>
                </tr>
              ))
            ) : (
              <EmptyRow colSpan={4}>No orders yet</EmptyRow>
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
                      ? `${humanize(event.from_state)} → ${humanize(event.to_state)}`
                      : "—"}
                  </td>
                  <td title={event.idempotency_key}>{shortKey(event.idempotency_key)}</td>
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
