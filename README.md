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
