# TaskMesh

TaskMesh is a portfolio-grade asynchronous job-processing system built with
FastAPI, SQLAlchemy, PostgreSQL, Redis, and Celery. It persists task lifecycle
and execution history in the database, routes work to priority-specific queues,
applies bounded retries, and exposes operational state through an API and a
lightweight dashboard.

## Highlights

- Durable task and per-attempt history with SQLAlchemy 2.x and Alembic
- HIGH, MEDIUM, and LOW queues with dedicated Celery workers
- Atomic task claims and explicit lifecycle transitions
- Exponential retry backoff and dead-letter handling
- Safe, conditional requeue of dead-letter tasks
- Database, queue, and worker monitoring with graceful degradation
- Responsive dashboard with live polling and execution-attempt detail
- Deterministic SQLite test suite; no infrastructure required for tests
- Docker Compose development stack and reproducible HTTP load tool

## Architecture

```text
client / dashboard
       |
       v
   FastAPI API --------> PostgreSQL
       |                 tasks + execution attempts
       v
     Redis
  high | medium | low
       v
 dedicated Celery workers
```

PostgreSQL is the persistent source of truth. Redis/Celery is transport and
execution infrastructure; TaskMesh does not treat Celery result state as task
history. See [the architecture guide](docs/architecture.md) for lifecycle and
failure-boundary details.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Service identity |
| GET | `/health` | API liveness |
| POST | `/tasks` | Persist and dispatch a task |
| GET | `/tasks` | Paginated/filterable task list |
| GET | `/tasks/{task_id}` | Task and execution-attempt history |
| POST | `/tasks/{task_id}/requeue` | Requeue a dead-letter task |
| GET | `/monitoring/summary` | Status, throughput, queue, and worker summary |
| GET | `/dashboard` | Operational dashboard |

Task-list filters are `priority`, `status`, and `task_type`. Pagination defaults
to `offset=0&limit=20` and caps `limit` at 100.

## Local setup (SQLite)

Python 3.12 is the supported development version.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m alembic upgrade head
python -m uvicorn app.main:app --reload
```

The safe default database is `sqlite:///./taskmesh.db`. The API is available at
`http://localhost:8000`, Swagger UI at `/docs`, and the dashboard at
`/dashboard`. Submitting work also needs a Redis broker and worker; without one,
the API returns 503 after preserving the QUEUED task for diagnosis.

The implemented `echo` task expects a non-empty string in `payload.message`:

```json
{"task_type": "echo", "payload": {"message": "hello"}, "priority": "MEDIUM"}
```

## Docker Compose

Copy the safe template, choose a local-only PostgreSQL password, and start the
stack. Do not commit the resulting `.env` file.

```powershell
Copy-Item .env.example .env
docker compose config
docker compose build
docker compose up -d
docker compose ps
```

The API container applies migrations after PostgreSQL becomes healthy. Separate
workers consume `high`, `medium`, and `low`. Shut down with:

```powershell
docker compose down
```

Use `docker compose down -v` only when intentionally deleting local database and
Redis volumes.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TASKMESH_DATABASE_URL` | `sqlite:///./taskmesh.db` | SQLAlchemy URL; use `postgresql+psycopg://...` in deployment |
| `TASKMESH_CELERY_BROKER_URL` | `redis://localhost:6379/0` | Redis broker and queue inspection endpoint |
| `TASKMESH_APP_NAME` | `TaskMesh` | API display name |
| `TASKMESH_APP_VERSION` | `0.1.0` | API version |
| `TASKMESH_DEBUG` | `false` | FastAPI debug mode |

## Tests and load validation

The standard suite is fully offline and uses temporary SQLite databases and
dependency overrides:

```powershell
.\.venv\Scripts\python.exe -m pytest -v --basetemp=.pytest_tmp -p no:cacheprovider
```

With a running local stack, exercise the submission API safely:

```powershell
.\.venv\Scripts\python.exe -m scripts.load_test --tasks 100 --concurrency 10
```

The tool refuses non-loopback targets unless explicitly overridden. See
[load testing](docs/load-testing.md) for interpretation and limitations.

## Retry and dead-letter semantics

Handlers explicitly mark transient errors as retryable. Delays grow 2, 4, 8,
then 16 seconds and stay capped at 16 seconds. `max_retries` counts deliveries
after the first attempt. Exhausted retryable tasks enter `DEAD_LETTER`; permanent
failures enter `FAILED`. A requeue resets retry state while preserving attempt
history and routes the task through its original priority queue.

## Known limitations

- Dispatch and database commit are separate operations; a durable transactional
  outbox is not implemented.
- A worker crash after committing RUNNING can leave an abandoned task; lease and
  recovery processing is not implemented.
- Authentication, authorization, rate limiting, distributed tracing, and
  production secret management are outside this portfolio scope.
- Queue priority isolates workloads but cannot guarantee global completion order.
- Monitoring is a bounded snapshot, not a long-term metrics store.

## Further reading

- [Architecture and lifecycle](docs/architecture.md)
- [Load-testing guide](docs/load-testing.md)
- [Evidence-based resume claims](docs/resume-claims.md)
- [Interview guide](docs/interview-guide.md)

## License

No license has been granted. Add an explicit license before redistribution.
