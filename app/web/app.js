const $ = (id) => document.getElementById(id);
const REFRESH_INTERVAL_MS = 4000;
const MAX_HISTORY_POINTS = 24;

let lastSnapshot = null;
let selectedTaskId = null;
let refreshTimer = null;
const throughputHistory = [];

const escapeHtml = (value) => String(value ?? "")
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;").replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");
const shortId = (id) => `${String(id).slice(0, 8)}…`;
const statusLabel = (value) => String(value).replaceAll("_", " ");
const badge = (value) => `<span class="badge badge-${escapeHtml(value)}">${escapeHtml(statusLabel(value))}</span>`;
const priority = (value) => `<span class="priority priority-${escapeHtml(value)}">${escapeHtml(value)}</span>`;
const dateTime = (value) => value ? new Date(value).toLocaleString() : "Unknown";
const clockTime = (value) => value ? new Date(value).toLocaleTimeString() : "Unknown";
const duration = (start, end) => {
  if (!start || !end) return "—";
  const milliseconds = new Date(end) - new Date(start);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return "—";
  return milliseconds < 1000 ? `${milliseconds} ms` : `${(milliseconds / 1000).toFixed(1)} s`;
};

function emptyState(title, detail = "") {
  return `<div class="empty-state"><strong>${escapeHtml(title)}</strong>${detail ? `<span>${escapeHtml(detail)}</span>` : ""}</div>`;
}

function deriveHealth(data) {
  if (!data) return { label: "Offline", className: "offline" };
  if (!data.database.available || !data.queues.available || !data.workers.available) {
    return { label: "Degraded", className: "degraded" };
  }
  return { label: "Healthy", className: "" };
}

function renderMetrics(data) {
  if (!data.tasks || !data.throughput) {
    $("metric-grid").innerHTML = emptyState(
      "Task metrics unavailable",
      data.database.error || "Database is not currently reachable.",
    );
    return;
  }
  const metrics = [
    ["Queued", data.tasks.queued, "Across persisted task records", "#aa6d08"],
    ["Running", data.tasks.running, "Currently executing", "#2878cf"],
    ["Completed", data.tasks.completed, "Terminal successes", "#18855b"],
    ["Failed", data.tasks.failed, "Permanent failures", "#c4483e"],
    ["Dead letter", data.tasks.dead_letter, "Retry policy exhausted", "#8f2435"],
    ["Throughput", `${data.throughput.per_minute.toFixed(1)}/min`, `Completed in last ${data.throughput.window_seconds}s`, "#7157e8"],
  ];
  $("metric-grid").innerHTML = metrics.map(([label, value, note, color]) => `
    <article class="metric" style="--metric-color:${color}">
      <div class="metric-label"><span>${label}</span><span aria-hidden="true">↗</span></div>
      <div class="metric-value">${escapeHtml(value)}</div><div class="metric-note">${escapeHtml(note)}</div>
    </article>`).join("");
}

function renderLifecycle(data) {
  if (!data.tasks) {
    $("task-flow").innerHTML = emptyState(
      "Lifecycle data unavailable",
      "Database is not currently reachable.",
    );
    return;
  }
  $("task-flow").innerHTML = `
    <div class="flow-node" style="--node-color:var(--amber)"><span>QUEUED</span><strong>${data.tasks.queued}</strong></div>
    <i class="flow-connector" aria-hidden="true"></i>
    <div class="flow-node" style="--node-color:var(--blue)"><span>RUNNING</span><strong>${data.tasks.running}</strong></div>
    <i class="flow-connector" aria-hidden="true"></i>
    <div class="flow-branches">
      <div class="flow-branch"><span>COMPLETED</span><strong>${data.tasks.completed}</strong></div>
      <div class="flow-branch"><span>FAILED</span><strong>${data.tasks.failed}</strong></div>
      <div class="flow-branch"><span>DEAD LETTER</span><strong>${data.tasks.dead_letter}</strong></div>
    </div>`;
}

function renderQueues(queues) {
  $("queue-availability").textContent = queues.available ? `${queues.total} jobs` : "Unavailable";
  if (!queues.available) {
    $("queue-lanes").innerHTML = `<div class="degraded-state"><strong>Queue telemetry unavailable</strong><span>${escapeHtml(queues.error || "Redis is not currently reachable.")}</span></div>`;
    return;
  }
  const lanes = [["high", "#c4483e"], ["medium", "#7157e8"], ["low", "#68717c"]];
  $("queue-lanes").innerHTML = lanes.map(([name, color]) => {
    const depth = Number(queues[name]) || 0;
    const percentage = queues.total ? Math.round((depth / queues.total) * 100) : 0;
    return `<div class="queue-lane" style="--lane-color:${color}">
      <div class="queue-label"><strong>${name.toUpperCase()}</strong><span>AVAILABLE</span></div>
      <div class="queue-track" role="meter" aria-label="${name} queue share" aria-valuenow="${percentage}" aria-valuemin="0" aria-valuemax="100"><div class="queue-fill" style="width:${percentage}%"></div></div>
      <div class="queue-depth">${depth} job${depth === 1 ? "" : "s"}</div>
    </div>`;
  }).join("");
}

function renderWorkers(workers) {
  $("worker-count").textContent = workers.available ? `${workers.count} online` : "Unavailable";
  if (!workers.available) {
    $("worker-fleet").innerHTML = `<div class="degraded-state"><strong>Worker telemetry unavailable</strong><span>Redis/Celery infrastructure is not currently reachable. Database metrics remain available.</span></div>`;
    return;
  }
  if (!workers.items.length) {
    $("worker-fleet").innerHTML = emptyState("No workers responded", "Waiting for Celery worker telemetry.");
    return;
  }
  $("worker-fleet").innerHTML = workers.items.map((worker) => `
    <article class="worker-card"><header><span class="worker-name" title="${escapeHtml(worker.identifier)}">${escapeHtml(worker.identifier)}</span><span class="status-label"><i class="status-dot"></i>ONLINE</span></header>
      <div class="worker-stats"><div class="worker-stat"><span>Active jobs</span><strong>${worker.active_tasks}</strong></div><div class="worker-stat"><span>Reserved</span><strong>${worker.reserved_tasks}</strong></div></div>
    </article>`).join("");
}

function renderTasks(tasks) {
  $("recent-tasks").innerHTML = tasks.length ? tasks.map((task) => `<tr>
    <td><span class="task-id mono" title="${escapeHtml(task.id)}">${shortId(task.id)}</span></td><td>${escapeHtml(task.task_type)}</td><td>${priority(task.priority)}</td><td>${badge(task.status)}</td>
    <td class="mono" title="Inspect for execution-attempt history">—</td><td title="${escapeHtml(dateTime(task.created_at))}">${escapeHtml(clockTime(task.created_at))}</td><td class="mono">${duration(task.started_at, task.completed_at)}</td>
    <td><button class="button button-link" data-inspect="${escapeHtml(task.id)}">Inspect</button></td></tr>`).join("") : `<tr><td colspan="8">${emptyState("Waiting for task activity", "No persisted tasks yet.")}</td></tr>`;
}

function renderFailurePanels(data) {
  const deadLetters = data.recent_failures.filter((task) => task.status === "DEAD_LETTER");
  $("dead-count").textContent = data.tasks ? `${data.tasks.dead_letter} total` : "Unknown";
  $("dead-letter-list").innerHTML = deadLetters.length ? deadLetters.map((task) => `
    <article class="compact-item" data-inspect="${escapeHtml(task.id)}" tabindex="0">
      <div><div class="compact-title"><span class="mono" title="${escapeHtml(task.id)}">${shortId(task.id)}</span>${badge(task.status)}</div><div class="error-preview">${escapeHtml(task.last_error || "No error detail")}</div><div class="compact-meta">${escapeHtml(task.task_type)} · ${task.retry_count}/${task.max_retries} retries · ${escapeHtml(clockTime(task.completed_at))}</div></div>
      <div class="compact-actions"><button class="button button-link" data-inspect="${escapeHtml(task.id)}">Inspect</button><button class="button button-secondary button-danger" data-requeue="${escapeHtml(task.id)}">Requeue</button></div>
    </article>`).join("") : emptyState("Dead-letter queue is empty", "No recent dead-letter tasks in this snapshot.");

  $("failure-list").innerHTML = data.recent_failures.length ? data.recent_failures.map((task) => `
    <article class="compact-item" data-inspect="${escapeHtml(task.id)}" tabindex="0"><div><div class="compact-title"><span class="mono" title="${escapeHtml(task.id)}">${shortId(task.id)}</span>${badge(task.status)}</div><div class="error-preview">${escapeHtml(task.last_error || "No error detail")}</div><div class="compact-meta">${escapeHtml(clockTime(task.completed_at))}</div></div><button class="button button-link" data-inspect="${escapeHtml(task.id)}">Inspect</button></article>`).join("") : emptyState("No recent failures", "Terminal errors will appear here.");
}

function renderHealth(data) {
  // "API" is not a fabricated indicator: this function only ever runs after a
  // successful fetch of /monitoring/summary, so "Reachable" is true by
  // construction, not a hardcoded guess. It is worded differently from
  // "Healthy" to avoid implying a deeper API-side health check that does not
  // exist. Database/Celery/Redis are each derived from real response fields.
  const states = [
    ["API", "Reachable", ""],
    ["Database", data.database.available ? "Healthy" : "Unavailable", data.database.available ? "" : "unknown"],
    ["Celery", data.workers.available ? "Online" : "Unavailable", data.workers.available ? "" : "unknown"],
    ["Redis", data.queues.available ? "Online" : "Unavailable", data.queues.available ? "" : "unknown"],
    ["Workers", data.workers.available ? `${data.workers.count} online` : "Unknown", data.workers.available ? "" : "unknown"],
  ];
  $("health-grid").innerHTML = states.map(([name, value, className]) => `<div class="health-item"><span>${name}</span><strong><i class="status-dot ${className}"></i>${escapeHtml(value)}</strong></div>`).join("");
}

function renderChart(value) {
  if (value === null || value === undefined) {
    $("throughput-value").textContent = "—";
    $("throughput-chart").innerHTML = "Database unavailable";
    $("throughput-chart").className = "chart-empty";
    return;
  }
  throughputHistory.push(Number(value) || 0);
  if (throughputHistory.length > MAX_HISTORY_POINTS) throughputHistory.shift();
  $("throughput-value").textContent = `${Number(value).toFixed(1)}/min`;
  if (throughputHistory.length < 2) {
    $("throughput-chart").innerHTML = "Collecting session history";
    $("throughput-chart").className = "chart-empty";
    return;
  }
  const width = 420, height = 105, padding = 6;
  const maximum = Math.max(...throughputHistory, 1);
  const points = throughputHistory.map((item, index) => {
    const x = padding + index * ((width - padding * 2) / (throughputHistory.length - 1));
    const y = height - padding - (item / maximum) * (height - padding * 2);
    return [x, y];
  });
  const line = points.map(([x, y], index) => `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const area = `${line} L${points.at(-1)[0]},${height} L${points[0][0]},${height} Z`;
  $("throughput-chart").className = "";
  $("throughput-chart").innerHTML = `<svg class="chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Session throughput history"><defs><linearGradient id="chart-gradient" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#7157e8" stop-opacity=".2"/><stop offset="1" stop-color="#7157e8" stop-opacity="0"/></linearGradient></defs><path class="area" d="${area}"/><path class="line" d="${line}"/><circle cx="${points.at(-1)[0]}" cy="${points.at(-1)[1]}" r="3"/></svg>`;
}

function renderSnapshot(data) {
  renderMetrics(data); renderLifecycle(data); renderQueues(data.queues);
  renderWorkers(data.workers); renderTasks(data.recent_tasks);
  renderFailurePanels(data); renderHealth(data); renderChart(data.throughput ? data.throughput.per_minute : null);
  const health = deriveHealth(data);
  $("overall-health").innerHTML = `<i class="status-dot ${health.className}"></i>${health.label}`;
  $("live-dot").className = `status-dot ${health.className}`;
  $("live-label").textContent = health.label === "Healthy" ? "Live" : health.label;
  $("sidebar-api").innerHTML = '<i class="status-dot"></i>ONLINE';
  bindDynamicActions();
}

async function showTask(id) {
  selectedTaskId = id;
  openDrawer();
  $("task-detail").innerHTML = '<div class="skeleton-lines"></div>';
  try {
    const response = await fetch(`/tasks/${encodeURIComponent(id)}`);
    if (!response.ok) throw new Error("Task detail unavailable");
    const task = await response.json();
    const attempts = task.execution_attempts || [];
    $("task-detail").innerHTML = `
      <div>${badge(task.status)}<p class="drawer-id">${escapeHtml(task.id)}</p><strong>${escapeHtml(task.task_type)}</strong></div>
      <section class="detail-section"><h3>Overview</h3><div class="detail-grid">
        <div class="detail-item"><span>Priority</span><strong>${priority(task.priority)}</strong></div><div class="detail-item"><span>Status</span><strong>${escapeHtml(statusLabel(task.status))}</strong></div>
        <div class="detail-item"><span>Retry count</span><strong class="mono">${task.retry_count}</strong></div><div class="detail-item"><span>Max retries</span><strong class="mono">${task.max_retries}</strong></div></div></section>
      <section class="detail-section"><h3>Timestamps</h3><div class="detail-grid">
        <div class="detail-item"><span>Created</span><strong>${escapeHtml(dateTime(task.created_at))}</strong></div><div class="detail-item"><span>Started</span><strong>${escapeHtml(dateTime(task.started_at))}</strong></div>
        <div class="detail-item"><span>Completed</span><strong>${escapeHtml(dateTime(task.completed_at))}</strong></div><div class="detail-item"><span>Duration</span><strong class="mono">${duration(task.started_at, task.completed_at)}</strong></div></div></section>
      ${task.last_error ? `<section class="detail-section"><h3>Last error</h3><div class="error-box">${escapeHtml(task.last_error)}</div></section>` : ""}
      <section class="detail-section"><h3>Execution timeline</h3>${attempts.length ? `<div class="timeline">${attempts.map((attempt) => `<article class="timeline-item"><i class="timeline-dot"></i><div class="timeline-title"><strong>Attempt ${attempt.attempt_number}</strong><span>${attempt.error ? badge("FAILED") : attempt.finished_at ? badge("COMPLETED") : badge("RUNNING")}</span></div><div class="timeline-meta">${escapeHtml(attempt.worker_identifier || "Unknown worker")}<br>${escapeHtml(clockTime(attempt.started_at))} → ${escapeHtml(clockTime(attempt.finished_at))}</div>${attempt.error ? `<div class="timeline-error">${escapeHtml(attempt.error)}</div>` : ""}</article>`).join("")}</div>` : emptyState("No execution attempts", "This task has not been claimed by a worker.")}</section>
      ${task.status === "DEAD_LETTER" ? `<section class="detail-section"><button class="button button-primary" data-requeue="${escapeHtml(task.id)}">Requeue Task</button></section>` : ""}`;
    bindDynamicActions();
  } catch (_) {
    $("task-detail").innerHTML = emptyState("Task detail unavailable", "Close the inspector and try again.");
  }
}

function openDrawer() {
  $("drawer-backdrop").hidden = false;
  requestAnimationFrame(() => $("drawer-backdrop").classList.add("visible"));
  $("task-drawer").classList.add("open");
  $("task-drawer").setAttribute("aria-hidden", "false");
  $("drawer-close").focus();
}

function closeDrawer() {
  $("task-drawer").classList.remove("open");
  $("task-drawer").setAttribute("aria-hidden", "true");
  $("drawer-backdrop").classList.remove("visible");
  setTimeout(() => { $("drawer-backdrop").hidden = true; }, 220);
}

function requestRequeue(id) {
  selectedTaskId = id;
  $("requeue-dialog").showModal();
}

async function requeueSelected(event) {
  event.preventDefault();
  $("confirm-requeue").disabled = true;
  try {
    const response = await fetch(`/tasks/${encodeURIComponent(selectedTaskId)}/requeue`, { method: "POST" });
    if (!response.ok) throw new Error("Requeue request failed");
    $("requeue-dialog").close(); closeDrawer(); showToast("Task requeued successfully"); await refresh();
  } catch (_) {
    showToast("Task could not be requeued", true);
  } finally {
    $("confirm-requeue").disabled = false;
  }
}

function showToast(message, isError = false) {
  const toast = document.createElement("div");
  toast.className = `toast${isError ? " error" : ""}`;
  toast.textContent = message;
  $("toast-region").append(toast);
  setTimeout(() => toast.remove(), 3500);
}

function bindDynamicActions() {
  document.querySelectorAll("[data-inspect]").forEach((element) => {
    element.onclick = (event) => { event.stopPropagation(); showTask(element.dataset.inspect); };
    element.onkeydown = (event) => { if (event.key === "Enter" || event.key === " ") showTask(element.dataset.inspect); };
  });
  document.querySelectorAll("[data-requeue]").forEach((button) => {
    button.onclick = (event) => { event.stopPropagation(); requestRequeue(button.dataset.requeue); };
  });
}

async function refresh() {
  $("refresh-button").disabled = true;
  try {
    const response = await fetch("/monitoring/summary");
    if (!response.ok) throw new Error("Monitoring request failed");
    lastSnapshot = await response.json();
    renderSnapshot(lastSnapshot);
    $("telemetry-alert").hidden = true;
    $("updated-at").textContent = new Date().toLocaleTimeString();
  } catch (_) {
    $("telemetry-alert").hidden = false;
    $("live-dot").className = "status-dot offline";
    $("live-label").textContent = "Offline";
    $("sidebar-api").innerHTML = '<i class="status-dot offline"></i>OFFLINE';
    if (!lastSnapshot) $("overall-health").innerHTML = '<i class="status-dot offline"></i>Offline';
  } finally {
    $("refresh-button").disabled = false;
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(refresh, REFRESH_INTERVAL_MS);
  }
}

$("refresh-button").addEventListener("click", refresh);
$("retry-button").addEventListener("click", refresh);
$("drawer-close").addEventListener("click", closeDrawer);
$("drawer-backdrop").addEventListener("click", closeDrawer);
$("confirm-requeue").addEventListener("click", requeueSelected);
$("menu-button").addEventListener("click", () => {
  const open = $("sidebar").classList.toggle("open");
  $("menu-button").setAttribute("aria-expanded", String(open));
});
document.querySelectorAll(".nav-link").forEach((link) => link.addEventListener("click", () => {
  document.querySelectorAll(".nav-link").forEach((item) => item.classList.remove("active"));
  link.classList.add("active");
  $("sidebar").classList.remove("open");
}));
document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeDrawer(); });
refresh();
