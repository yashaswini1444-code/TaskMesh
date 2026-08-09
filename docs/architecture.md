# Architecture and lifecycle

## System boundaries

FastAPI validates submissions and owns the public API. SQLAlchemy writes tasks
and execution attempts to PostgreSQL (or SQLite in tests). Celery publishes task
identifiers through Redis. Workers load current state from the database, claim a
task atomically, execute a registered handler, and persist the outcome.

The broker message contains an identifier, not authoritative task state. This
keeps queryable history independent of Redis retention and Celery result-backend
behavior — `task_ignore_result=True` and no result backend is configured;
PostgreSQL is the only source of truth for task state.

## Lifecycle

```text
POST /tasks
    |
    v
  QUEUED --atomic claim--> RUNNING --success--> COMPLETED
    ^  ^                      |  |
    |  |                      |  +--permanent error--> FAILED
    |  |                      |
    |  +--lease expired-------+--retryable error, budget left
    |  |  (recovery reclaims,      |
    |  |   redispatches)          +--budget exhausted--> DEAD_LETTER
    |  |                                                    |
    |  +--lease expired, budget exhausted------------------>+
    |                                                        |
    +--explicit requeue-------------------------------------+
    |
    +--manual redispatch (already QUEUED, dispatch had failed)
```

Every actual execution creates a `TaskExecutionAttempt`. Attempts retain worker
identity, start/end times, and errors. Requeue and recovery never delete
history — they close out the abandoned attempt truthfully and add a new one.

## Priority routing

Task priority maps directly to `high`, `medium`, or `low` queues. Both Docker
Compose and the CI integration suite run one dedicated worker process per
queue (plus a `control` queue for administrative/periodic work — see
Recovery, below). This provides real capacity isolation: a HIGH task can
never be stuck waiting behind a backlog of LOW tasks, because no worker
serves both queues. It is not a strict global scheduler — concurrency within
a queue and worker availability still affect completion order within that
priority tier. This is proven in CI (see `tests/integration/test_live_stack.py`)
by asserting that a HIGH-submitted task's execution attempt was actually
processed by the `worker-high` process, not just that the message was
addressed to the right queue.

## Delivery semantics: at-least-once, not exactly-once

Celery workers run with `task_acks_late=True` and
`task_reject_on_worker_lost=True` (`app/workers/celery_app.py`), chosen
deliberately over Celery's defaults:

- **Without late-ack** (Celery's default): a message is acknowledged the
  instant a worker receives it, before execution starts. A worker killed in
  the small window between receiving a message and starting the DB claim
  transaction loses that message permanently — no redelivery, task stuck
  `QUEUED` with zero execution attempts, no error anywhere.
- **With late-ack**: the message is only acknowledged after the task
  returns or raises a non-retried exception. An unacked message is
  eventually redelivered (bounded by `broker_transport_options.visibility_timeout`,
  set to `TASKMESH_TASK_LEASE_SECONDS`).

This makes **duplicate broker delivery possible** — a message can be
redelivered while the original worker is still legitimately executing it, or
after it finished but before the ack landed. This is exactly what the atomic
claim (below) exists to make safe. Late-ack would not be safe to enable
without it.

**What TaskMesh does not provide:** exactly-once execution. If a job's real
side effect (e.g. an external API call) happens and *then* the worker is
killed before the commit that marks it `COMPLETED`, the task is later either
lease-recovered or redelivered — either way, the handler runs again.
TaskMesh guarantees at-least-once delivery and duplicate-*processing*
protection at the persisted-task-id level, not idempotent business side
effects. Handlers with non-idempotent external effects need their own
idempotency keys; none is implemented here because the one built-in handler
(`echo`) has no external side effect to protect.

## Atomic claim (the mechanism everything else relies on)

`app/services/task_lifecycle._claim_task` is a single conditional statement:

```sql
UPDATE tasks SET status='RUNNING', ... WHERE id=:id AND status='QUEUED'
```

Exactly one concurrent caller can win this for a given task — the database's
row-level locking serializes it, not application code. A duplicate delivery,
a concurrent recovery scan, or (in a hypothetical multi-process test) eight
threads racing the same row all resolve to "one winner, seven safe no-ops."
This is proven against real PostgreSQL row-locking (not just SQLite) in
`tests/integration/test_postgres_concurrency.py`.

Every other state transition in the system (`_complete_task`,
`_finalize_failure`, and the equivalent in `app/services/recovery.py`) uses
the identical pattern: `UPDATE ... WHERE status = 'RUNNING'` (or `QUEUED`, or
`DEAD_LETTER`, depending on the transition). This is a single, consistent
concurrency-control idiom applied everywhere a status transition happens —
not a special case invented once and forgotten.

## Task leases and stale-RUNNING recovery

A worker crash (process killed, host lost) after the RUNNING commit but
before completion used to strand the task forever, with no code addressing
it — the previous version of this document said so explicitly. That gap is
now closed with a lease:

- `_claim_task` sets `tasks.lease_expires_at = now + TASKMESH_TASK_LEASE_SECONDS`
  (default 300s) at claim time, and every terminal transition clears it.
- `app/services/recovery.recover_stale_tasks` finds `RUNNING` tasks whose
  lease has expired and reclaims each with the same compare-and-swap pattern
  as everything else: `UPDATE ... WHERE status='RUNNING' AND lease_expires_at < :now`.
  This closes the race against the original worker finishing normally at the
  same moment — whichever transaction's UPDATE actually matches a row wins;
  the other affects zero rows and is a safe no-op.
- Reclaimed tasks respect the same retry budget as an in-worker failure:
  retries left → `QUEUED` **and explicitly redispatched** (no Celery process
  is alive to call `self.retry()` — the recovery scan has to publish a new
  broker message itself); budget exhausted → `DEAD_LETTER`. The abandoned
  attempt is closed out with a truthful error (`"Execution lease expired;
  worker presumed lost"`), never deleted.
- Triggered two ways: a Celery Beat schedule (`app/workers/celery_app.py`,
  every `TASKMESH_TASK_RECOVERY_INTERVAL_SECONDS`, default 30s) on the
  dedicated `control` queue, and an administrative
  `POST /recovery/stale-running` endpoint (also wired into the dashboard) for
  on-demand use without waiting for the schedule.

**What this guarantees:** a task is only reclaimed after its lease has
genuinely expired; reclaiming is atomic (at most one caller wins); attempt
history is preserved and truthful; retry budget is respected.

**What this does not guarantee:** exactly-once execution, for the same
reason described above — if the "presumed dead" worker was actually just
slower than the configured lease, it may complete the job's real side
effects *after* recovery already redispatched a second attempt. This is a
deliberate, explicit trade-off: a lease that is too short relative to real
job duration trades "stuck forever" for "possibly executed twice." Set
`TASKMESH_TASK_LEASE_SECONDS` well above the slowest expected job duration.

To close the other half of this race — the original worker resurfacing
*after* being reclaimed — `_complete_task` and `_finalize_failure` are
themselves guarded the same way (`WHERE status = 'RUNNING'`). If recovery
already moved the task off `RUNNING`, the late completion/failure raises
`TaskReclaimedError` instead of silently overwriting recovery's decision.
This is deliberately a design decision, not just a lucky consequence of using
UPDATE everywhere: every place a task leaves `RUNNING` has to agree on who
"owns" that transition, and the answer is always "whoever's UPDATE actually
matched a row."

## Consistency window: persist-then-dispatch is not atomic

Task persistence (the `POST /tasks` DB commit) and broker publication
(`dispatcher.dispatch`) are two separate operations, not a single
transaction — there is no transactional outbox. If the broker is
unreachable at submission time, `POST /tasks` returns `503` with the
persisted task's id, and the task sits `QUEUED` with no corresponding
message. The same window exists on `POST /tasks/{id}/requeue` and inside
lease recovery's own redispatch step.

Two ways to recover, since the earlier version of this project had none:

- `POST /tasks/{id}/redispatch` republishes a message for an already-QUEUED
  task — same task id, no new row, no state reset. Safe to call more than
  once or concurrently: it does not bypass the atomic claim, so an extra
  message for a task another delivery already claimed is at worst a wasted
  broker publish, never a duplicate execution.
- The dashboard exposes this as a confirm-gated action on `QUEUED` tasks.

This is a manual/administrative recovery path, not a solved problem. A
transactional outbox (write the "needs dispatch" fact in the same DB
transaction as the task, with a separate publisher process draining it)
would remove the window entirely; it is not implemented here, and no claim
to the contrary should be made.

## Consistency and failure behavior — summary

- The claim transition is conditional on `QUEUED`, preventing duplicate
  broker deliveries — or a lease-recovery scan — from starting the same task
  concurrently.
- Retry state is committed before scheduling the next delivery, so counters
  and attempt history remain durable even if the process crashes immediately
  after.
- Submission/requeue database commits and broker publication are not atomic
  (above) — mitigated with manual redispatch, not eliminated.
- A worker crash after a RUNNING commit is recovered by lease expiry
  (above), on a bounded delay, with a documented possible-double-execution
  trade-off — not eliminated, and intentionally not a heavier
  heartbeat/lease-owner system.
- Worker and Redis inspection have short timeouts and return degraded
  monitoring data instead of failing the summary endpoint. Database
  inspection now degrades the same way: a DB outage during
  `/monitoring/summary` returns `database.available=false` with
  `tasks`/`throughput` left unset (never fabricated as zero), instead of a
  500 — this specific gap was present until this pass and is now fixed.

## Retry backoff — exact numbers

Delay for the *n*th retry (1-based) is `min(2**n, 16)` seconds
(`app/services/task_lifecycle.py`). With the default `max_retries=3`, only
retries 1–3 ever happen, giving delays of **2, 4, 8 seconds** — the 16s cap
is real, tested code (`retry_countdown` is verified up to retry 5), but it is
only reached when `max_retries >= 4`, which is not the default. Do not
describe the default behavior as "2, 4, 8, then 16" — that overstates what a
freshly submitted task with default settings will ever do.

## Data model

`tasks` stores identity, type, JSON payload, priority, status, bounded retry
counters, lifecycle timestamps, the last error, and `lease_expires_at`.
`task_execution_attempts` stores a normalized one-to-many execution audit
trail and enforces unique attempt numbers per task. Database constraints
protect enum-like domain values and non-negative retry data. A composite
index on `(status, lease_expires_at)` backs the recovery scan's actual query
shape; no standalone index on `lease_expires_at` alone, since no real query
filters on it without `status` first.

Two migrations exist: the initial schema, and a second migration adding the
lease column and its index — both have tested upgrade *and* downgrade paths
(`tests/test_migrations.py`), against SQLite in the deterministic suite and
against real PostgreSQL in CI's `integration` job.

## Monitoring

`/monitoring/summary` combines four independently-degrading signals:
database (task counts, throughput, recent tasks/failures, and a
`stale_running` gauge — RUNNING tasks whose lease has already expired, a
cheap read-only signal separate from actually running recovery), Celery
worker inspection, Redis queue depth, and — implicitly — the fact that the
endpoint responded at all. Each of the first three degrades to an
`available: false` + sanitized `error` field on failure rather than
fabricating data or crashing the endpoint. Verified against real
infrastructure in CI (`test_monitoring_reflects_real_infrastructure`), not
only against fakes.

## Observability

Structured-enough logging (`app/core/logging.py`) covers every lifecycle
transition, dispatch attempt, and recovery action, with `task_id` and
relevant context (task_type, priority, attempt_number, retry_number,
worker_identifier) as key=value pairs — not JSON, not shipped anywhere, not
correlated across services beyond the shared `task_id`. Payloads are never
logged. There is no metrics export and no distributed tracing; both are
listed as explicit future work, not implemented and not claimed.

## Security posture

The repository contains safe defaults and placeholders only. `.env`, virtual
environments, caches, and local database files are ignored. Container
credentials must be supplied at runtime. Payloads should never contain
secrets — there is no size/depth limit enforced on submitted JSON payloads
today. The demo has no authentication and should not be exposed directly to
an untrusted network.
