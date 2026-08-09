# TaskMesh

**Asynchronous Job Processing & Distributed Task Operations**

[![tests](https://github.com/yashaswini1444-code/TaskMesh/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/yashaswini1444-code/TaskMesh/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-2ea44f.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](requirements.txt)
[![Ruff](https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black)](pyproject.toml)
[![mypy](https://img.shields.io/badge/types-mypy-2A6DB2)](pyproject.toml)
[![Tests](https://img.shields.io/badge/tests-109%20passing-2ea44f)](tests/)

[![FastAPI](https://img.shields.io/badge/FastAPI-005571?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![Celery](https://img.shields.io/badge/Celery-37814A?logo=celery&logoColor=white)](https://docs.celeryq.dev/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)](docker-compose.yml)

TaskMesh is a portfolio-grade job-processing system built with FastAPI,
SQLAlchemy, PostgreSQL, Redis, and Celery. It exists to demonstrate — not
just describe — a set of distributed-systems problems and their trade-offs:
durable task state independent of broker retention, priority-isolated queue
routing, at-least-once delivery made safe by an atomic claim, bounded retry
with dead-letter handling, and recovery from a worker that crashes mid-task.

Every claim in this document is backed by a test that runs in CI — the
deterministic suite (97 tests, SQLite, no infrastructure, ~5s) and a
separate integration job that provisions real PostgreSQL and Redis
containers and runs the actual multi-worker Celery topology
(`tests/integration/`, 12 more tests). See
[docs/resume-claims.md](docs/resume-claims.md) for exactly which claims are
safe to make and which aren't.

## Key capabilities

- Durable task and per-attempt execution history (SQLAlchemy 2.x + Alembic,
  two migrations, both with tested upgrade *and* downgrade paths)
- HIGH / MEDIUM / LOW priority queues, each with a dedicated Celery worker
  process — real capacity isolation, not a shared-pool approximation
- Atomic, compare-and-swap task claiming — proven safe under real
  concurrent load against PostgreSQL row-level locking
- Bounded exponential retry backoff with dead-letter handling on exhaustion
- Lease-based recovery for tasks abandoned by a crashed worker, triggered by
  a periodic Celery Beat schedule and an on-demand admin endpoint
- Manual redispatch for tasks that were persisted but never reached the
  broker (the submission API is not atomic with broker publish — this is a
  deliberate, documented trade-off, not a hidden bug)
- Four-signal monitoring endpoint (database, Celery workers, Redis queues,
  and a stale-task gauge) that degrades each signal independently instead of
  crashing or fabricating data
- A dashboard with live polling, task inspection, and real operational
  actions — dead-letter requeue, stuck-task redispatch, stale-task recovery
- `ruff` + `mypy` quality gates, a fast deterministic test suite, and a CI
  job that runs the real infrastructure stack end to end

## Architecture

```text
        client / dashboard
               |
               v
        FastAPI API  <----------------------------+
               |                                   |
               v                                   |
          PostgreSQL  <---------------+             |
    tasks + execution attempts        |             |
                                       |             |
               |                  (recovery reads/writes,
               v                   redispatch on demand)
             Redis
     high | medium | low | control
       |      |      |       |
       v      v      v       v
   worker- worker- worker- worker-    scheduler
    high   medium   low   control  (celery beat;
  (dedicated workers,              publishes to
   one per queue)                  control queue)
```

PostgreSQL is the persistent source of truth; Celery has no result backend
configured (`task_ignore_result=True`) and is never treated as a source of
task state. See [docs/architecture.md](docs/architecture.md) for the full
delivery-semantics, consistency-window, and recovery discussion.

## Task lifecycle

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

Every execution creates a `TaskExecutionAttempt` (worker identity, start/end
timestamps, error text). Requeue and recovery never delete history — they
close out the abandoned attempt truthfully and add a new one.

## Priority queues

Priority maps directly to a `high`, `medium`, or `low` Redis queue, each
consumed by exactly one dedicated worker process (plus a `control` queue for
periodic recovery, consumed by its own worker). This is real isolation: a
HIGH task can never be stuck behind a LOW backlog, because no worker serves
more than one queue. It is not a strict global scheduler — tasks within a
single queue still complete in whatever order that queue's worker processes
them. The CI integration suite proves routing by checking which specific
worker process executed a given task, not just which queue the message was
addressed to.

## Retry and dead-letter semantics

Only handlers that raise `RetryableTaskExecutionError` retry; anything else
(unsupported task type, malformed payload) goes straight to `FAILED`, no
retry. Delay for the *n*th retry is `min(2**n, 16)` seconds. **With the
default `max_retries=3`, that's 2, 4, then 8 seconds** — the 16-second cap is
real, tested code, but only reached when `max_retries >= 4`, which is not
the default. State and the attempt record are committed *before* Celery is
told to retry, so a crash between those two steps still leaves correct,
durable state. Exhausted retryable tasks enter `DEAD_LETTER`; a requeue
resets retry state while preserving attempt history and dispatches through
the task's original priority queue.

## Task recovery (crashed workers)

Every claimed task carries `lease_expires_at` (default 300s from claim
time). If a worker crashes mid-task, the task would otherwise stay `RUNNING`
forever; instead, once the lease expires, `app/services/recovery.py`
reclaims it with the same compare-and-swap pattern used everywhere else in
this codebase, respecting the normal retry budget (requeued + redispatched,
or dead-lettered) and preserving attempt history. Triggered by a Celery Beat
schedule (every 30s by default) and by `POST /recovery/stale-running` for
on-demand use, also available as a button in the dashboard.

**Explicitly not guaranteed:** exactly-once execution. If the "presumed
dead" worker was actually just slower than the lease, its real side effects
can land *after* recovery already redispatched a second attempt. This is a
stated trade-off of timeout-based leases, not an oversight — set
`TASKMESH_TASK_LEASE_SECONDS` well above realistic job duration, and prefer
idempotent handlers for real work. Full reasoning in
[docs/architecture.md](docs/architecture.md#task-leases-and-stale-running-recovery).

## Distributed-system consistency, in one place

| Window | What can happen | Mitigation | Eliminated? |
|---|---|---|---|
| Submit/requeue commit vs. broker publish | Task persisted, no message sent | Manual `POST /tasks/{id}/redispatch` | No — no transactional outbox |
| Broker redelivery | Duplicate message for one task | Atomic claim (`UPDATE ... WHERE status='QUEUED'`) makes it a no-op | Yes, at the processing level — not at the delivery level |
| Worker crash mid-task | Task stuck `RUNNING` | Lease-based recovery, reclaimed after timeout | Delayed, not instant; possible double-execution if lease is too short |
| Recovered task's original worker resurfaces | Late completion could overwrite recovery's decision | `_complete_task`/`_finalize_failure` are also compare-and-swap guarded; late arrival raises `TaskReclaimedError` | Yes |
| Database briefly unreachable | `/monitoring/summary` used to 500 | Degrades to `database.available=false`, no fabricated counts | Yes |

## Stack

Python 3.12 · FastAPI · SQLAlchemy 2.x · Alembic · Celery 5 · Redis · PostgreSQL 17 (production) / SQLite (tests) · pytest · ruff · mypy · Docker Compose · GitHub Actions

## Local setup (SQLite)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m alembic upgrade head
python -m uvicorn app.main:app --reload
```

The default database is `sqlite:///./taskmesh.db`. The API is at
`http://localhost:8000`, Swagger UI at `/docs`, dashboard at `/dashboard`.
Submitting work also needs a Redis broker and a running worker; without one,
the API returns `503` after preserving the `QUEUED` task, which can later be
redispatched once a broker is available.

The only implemented handler, `echo`, expects a non-empty string in
`payload.message`:

```json
{"task_type": "echo", "payload": {"message": "hello"}, "priority": "MEDIUM"}
```

## Full stack (PostgreSQL + Redis + Celery, via Docker Compose)

```powershell
Copy-Item .env.example .env
# edit .env: choose a local-only PostgreSQL password
docker compose config
docker compose build
docker compose up -d
docker compose ps
```

This starts: `postgres`, `redis`, `api` (applies migrations on boot, then
serves the app), `worker-high` / `worker-medium` / `worker-low` (one
dedicated worker per priority queue), `worker-control` (consumes the
recovery queue), and `scheduler` (`celery beat`, publishes the periodic
recovery scan). Shut down with `docker compose down`; add `-v` only when
intentionally deleting local database/Redis volumes.

This developer machine does not have Docker Desktop installed, so this
compose stack is validated by structural review and by the fact that CI's
`integration` job runs the equivalent topology directly on a Linux runner —
not by a local `docker compose up` on this machine. Say so if asked; don't
imply local verification that didn't happen.

## Dashboard

`GET /dashboard` — live-polling operational view: task counts and lifecycle
flow, queue depth by priority, worker fleet status, recent executions with
real per-task attempt counts, dead-letter queue with requeue, recent
failures, and a system health panel. Every action button is a real API call
behind a confirmation dialog — dead-letter requeue, stuck-`QUEUED`
redispatch (from the task inspector), and stale-task recovery (from System
Health). Nothing on the dashboard is decorative or randomly generated;
health indicators are derived from the same response fields a script would
check.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Service identity |
| GET | `/health` | API liveness |
| POST | `/tasks` | Persist and dispatch a task |
| GET | `/tasks` | Paginated/filterable task list |
| GET | `/tasks/{task_id}` | Task and execution-attempt history |
| POST | `/tasks/{task_id}/requeue` | Requeue a dead-letter task |
| POST | `/tasks/{task_id}/redispatch` | Republish a broker message for an already-QUEUED task |
| GET | `/monitoring/summary` | Database/queue/worker status, throughput, recent tasks/failures |
| POST | `/recovery/stale-running` | Reclaim RUNNING tasks whose lease has expired, on demand |
| GET | `/dashboard` | Operational dashboard |

Task-list filters: `priority`, `status`, `task_type`. Pagination defaults to
`offset=0&limit=20`, capped at `limit=100`.

**503 behavior:** `POST /tasks` and `POST /tasks/{id}/requeue` return `503`
with `{"message": ..., "task_id": ...}` if the broker publish fails — the
task is still persisted (check the returned `task_id`), and can be recovered
with `POST /tasks/{id}/redispatch` once the broker is reachable.

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type": "echo", "payload": {"message": "hello"}, "priority": "HIGH"}'

curl http://localhost:8000/monitoring/summary

curl -X POST http://localhost:8000/tasks/<task_id>/redispatch
curl -X POST http://localhost:8000/recovery/stale-running
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TASKMESH_DATABASE_URL` | `sqlite:///./taskmesh.db` | SQLAlchemy URL; use `postgresql+psycopg://...` in deployment |
| `TASKMESH_CELERY_BROKER_URL` | `redis://localhost:6379/0` | Redis broker and queue-inspection endpoint |
| `TASKMESH_TASK_LEASE_SECONDS` | `300` | How long a claimed task may run before it's eligible for stale-task recovery |
| `TASKMESH_TASK_RECOVERY_INTERVAL_SECONDS` | `30` | How often Celery Beat triggers the recovery scan |
| `TASKMESH_APP_NAME` | `TaskMesh` | API display name |
| `TASKMESH_APP_VERSION` | `0.1.0` | API version |
| `TASKMESH_DEBUG` | `false` | FastAPI debug mode |

## Tests

```powershell
# Deterministic suite: SQLite, no infrastructure, ~5s
.\.venv\Scripts\python.exe -m pytest -v --basetemp=.pytest_tmp -p no:cacheprovider -m "not integration"

# Static analysis
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m mypy app
```

The `tests/integration/` suite requires a real PostgreSQL + Redis + running
Celery workers + a running API server; it's marked `integration` and
excluded from the command above. It isn't meant to run on a bare Windows
dev machine — see the CI section below for where it actually runs.

## Continuous integration

Three independent GitHub Actions jobs run on every push:

1. **`pytest`** — byte-compiles the source, then runs the deterministic
   suite.
2. **`quality`** — `ruff check .` and `mypy app`.
3. **`integration`** — provisions real `postgres:17-alpine` and
   `redis:7-alpine` service containers, applies migrations, starts four real
   Celery worker processes (`worker-high`/`-medium`/`-low`/`-control`) and
   the actual FastAPI app, then runs `tests/integration/` against all of it:
   migration correctness on real PostgreSQL, priority routing verified by
   which worker process executed the task, a deterministic failure path,
   dead-letter requeue through a real redispatch, monitoring against real
   infrastructure, and the PostgreSQL-specific concurrent-claim proof. All
   waits are bounded (timeout + poll, never an arbitrary sleep); a stack
   that never becomes ready fails the job instead of hanging.

## Failure behavior reference

| Scenario | What happens |
|---|---|
| Redis/Celery unavailable at submission | `POST /tasks` returns `503`; task is persisted `QUEUED`; redispatch once broker is back |
| Broker unavailable during dashboard poll | Dashboard shows Celery/Redis "Unavailable"; database-backed data still renders |
| Database briefly unreachable | `/monitoring/summary` returns `200` with `database.available=false`, no fabricated counts |
| Worker crashes mid-task | Task stays `RUNNING` until its lease expires, then is reclaimed automatically (or on demand) |
| Duplicate broker delivery | Second delivery's claim attempt affects zero rows; handler runs once |
| Malformed submission | `422` with field-level validation detail |
| Requeue/redispatch on wrong state | `409 Conflict` |

## Observability

Structured application logging (`app/core/logging.py`, plain Python
`logging`, not JSON) covers every lifecycle transition, dispatch attempt,
and recovery action, keyed by `task_id` with relevant fields
(task_type/priority/attempt_number/retry_number/worker_identifier).
Payloads are never logged. Failure logs carry exception *type*, not raw
exception text — nothing from a broker/database connection string can leak
through a log line. There is no metrics export and no distributed tracing;
both are real future work, not implemented, and not claimed anywhere in this
repository.

## Known limitations

- No transactional outbox — persist-then-dispatch has a real (if now
  recoverable) consistency window.
- Lease-based recovery does not guarantee exactly-once execution if a
  worker is merely slow, not dead.
- No idempotency-key mechanism for handler side effects — moot today since
  the only handler (`echo`) has no external side effect, but real handlers
  would need to bring their own.
- No authentication, authorization, rate limiting, or submitted-payload size
  limit. Out of scope for this portfolio project; do not expose this to an
  untrusted network.
- No metrics export or distributed tracing — logging only.
- Monitoring is a point-in-time snapshot, not a retained time series.
- Offset-based pagination; will not scale to very large task tables as-is.

## Design decisions worth asking about

See [docs/interview-guide.md](docs/interview-guide.md) for the full
treatment. Short version: every state transition in this codebase — claim,
complete, fail/retry/dead-letter, and lease-recovery's own reclaim — uses
the identical `UPDATE ... WHERE status = '<expected>'` compare-and-swap
idiom, not a special case invented once per feature. That consistency is
what lets late-ack delivery, duplicate broker messages, and a worker
resurfacing after being reclaimed all resolve safely without a distributed
lock.

## Further reading

- [Architecture and lifecycle](docs/architecture.md)
- [Load-testing guide](docs/load-testing.md)
- [Evidence-based resume claims](docs/resume-claims.md)
- [Interview guide](docs/interview-guide.md)

## License

[MIT](LICENSE) © 2026 Yashaswini L
