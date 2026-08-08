const $ = (id) => document.getElementById(id);
const badge = (value) => `<span class="badge badge-${value}">${value}</span>`;
const shortId = (id) => `${id.slice(0, 8)}…`;
const when = (value) => value ? new Date(value).toLocaleString() : "—";

function renderSummary(data) {
  const cards = [
    ["Queue depth", data.queues.available ? data.queues.total : "Unavailable"],
    ["Workers", data.workers.available ? data.workers.count : "Unavailable"],
    ["Queued", data.tasks.queued], ["Running", data.tasks.running],
    ["Completed", data.tasks.completed], ["Failed", data.tasks.failed],
    ["Dead letter", data.tasks.dead_letter],
    ["Completed / min", data.throughput.per_minute.toFixed(1)],
  ];
  $("summary-cards").innerHTML = cards.map(([label,value]) => `<article class="card"><div class="label">${label}</div><div class="value">${value}</div></article>`).join("");
  $("queues").innerHTML = data.queues.available
    ? ["high","medium","low"].map(q => `<div class="queue-row"><span class="queue-name">${q.toUpperCase()}</span><span class="depth">${data.queues[q]}</span></div>`).join("")
    : `<div class="empty">Unavailable · ${data.queues.error}</div>`;
  $("workers").innerHTML = data.workers.available
    ? (data.workers.items.map(w => `<div class="worker-row"><span><i class="dot"></i>${w.identifier}</span><span>${w.active_tasks} active · ${w.reserved_tasks} reserved</span></div>`).join("") || `<div class="empty">No workers responded</div>`)
    : `<div class="empty"><i class="dot offline"></i>Unavailable · ${data.workers.error}</div>`;
  $("recent-tasks").innerHTML = data.recent_tasks.map(task => `<tr><td class="mono" title="${task.id}">${shortId(task.id)}</td><td>${task.task_type}</td><td>${badge(task.priority)}</td><td>${badge(task.status)}</td><td>${task.retry_count}/${task.max_retries}</td><td>${when(task.created_at)}</td><td><button data-task-id="${task.id}">Details</button></td></tr>`).join("") || `<tr><td colspan="7" class="empty">No tasks yet</td></tr>`;
  $("failures").innerHTML = data.recent_failures.map(task => `<div class="failure"><strong>${badge(task.status)} ${task.task_type}</strong><div class="mono">${shortId(task.id)}</div><div>${task.last_error || "No error detail"}</div></div>`).join("") || `<div class="empty">No recent failures</div>`;
  document.querySelectorAll("[data-task-id]").forEach(button => button.addEventListener("click", () => showTask(button.dataset.taskId)));
}

async function showTask(id) {
  const response = await fetch(`/tasks/${id}`);
  if (!response.ok) return;
  const task = await response.json();
  $("task-detail").innerHTML = `<h2>${task.task_type}</h2><p class="mono">${task.id}</p><p>${badge(task.priority)} ${badge(task.status)} · retries ${task.retry_count}/${task.max_retries}</p><p>Created ${when(task.created_at)} · Started ${when(task.started_at)} · Completed ${when(task.completed_at)}</p>${task.last_error ? `<div class="failure">${task.last_error}</div>` : ""}<h3>Execution attempts</h3>${task.execution_attempts.map(a => `<div class="attempt"><strong>Attempt ${a.attempt_number}</strong> · ${a.worker_identifier || "Unknown worker"}<br>${when(a.started_at)} → ${when(a.finished_at)}${a.error ? `<br>${a.error}` : ""}</div>`).join("") || `<div class="empty">No attempts recorded</div>`}`;
  $("task-dialog").showModal();
}

async function refresh() {
  try {
    const response = await fetch("/monitoring/summary");
    if (!response.ok) throw new Error("Monitoring request failed");
    renderSummary(await response.json());
    $("refresh-status").textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (_) {
    $("refresh-status").textContent = "Monitoring unavailable";
  }
}

$("dialog-close").addEventListener("click", () => $("task-dialog").close());
refresh();
setInterval(refresh, 4000);
