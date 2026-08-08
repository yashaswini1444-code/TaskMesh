# TaskMesh

TaskMesh is an asynchronous job-processing API. SQLAlchemy is the persistent
source of truth, while Celery and Redis provide asynchronous transport.

## Priority queue workers

Task submissions are routed by their persisted priority:

- `HIGH` to `high`
- `MEDIUM` to `medium`
- `LOW` to `low`

Run a worker for one queue from the project root:

```powershell
celery -A app.workers.celery_app.celery_app worker -Q high --loglevel=info
celery -A app.workers.celery_app.celery_app worker -Q medium --loglevel=info
celery -A app.workers.celery_app.celery_app worker -Q low --loglevel=info
```

A worker can consume all three queues when workload separation is not needed:

```powershell
celery -A app.workers.celery_app.celery_app worker -Q high,medium,low --loglevel=info
```

Queue routing provides workload separation; it does not guarantee that every
HIGH task globally finishes before MEDIUM or LOW tasks. Execution order depends
on worker allocation, concurrency, prefetching, and Celery scheduling behavior.

## Supported task types

Milestone 6 supports an offline `echo` task. Its payload must contain a
non-empty string under `message`, for example:

```json
{"task_type": "echo", "payload": {"message": "hello"}, "priority": "MEDIUM"}
```

Workers atomically claim only `QUEUED` tasks, commit the `RUNNING` state and
execution-attempt record, and then run the handler. A process crash after that
commit can leave the task `RUNNING` with an unfinished attempt. Recovery of
abandoned work is intentionally deferred to a later milestone.
