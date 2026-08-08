# Architecture and lifecycle

## System boundaries

FastAPI validates submissions and owns the public API. SQLAlchemy writes tasks
and execution attempts to PostgreSQL (or SQLite in tests). Celery publishes task
identifiers through Redis. Workers load current state from the database, claim a
task atomically, execute a registered handler, and persist the outcome.

The broker message contains an identifier, not authoritative task state. This
keeps queryable history independent of Redis retention and Celery result-backend
behavior.

## Lifecycle

```text
POST /tasks
    |
    v
  QUEUED --atomic claim--> RUNNING --success--> COMPLETED
    ^                         |
    |                         +--permanent error--> FAILED
    |                         |
    +--scheduled retry--------+--retryable error
                                  |
                                  +--budget exhausted--> DEAD_LETTER
                                                             |
                                                    explicit requeue
                                                             |
                                                             v
                                                           QUEUED
```

Every actual execution creates a `TaskExecutionAttempt`. Attempts retain worker
identity, start/end times, and errors. Requeueing does not delete history.

## Priority routing

Task priority maps directly to `high`, `medium`, or `low`. Docker Compose runs a
dedicated worker for each queue. This provides capacity isolation and enables
independent scaling. It is not a strict global scheduler: concurrency, prefetch,
and worker availability affect completion order.

## Consistency and failure behavior

- The claim transition is conditional on `QUEUED`, preventing duplicate broker
  deliveries from starting the same task concurrently.
- Retry state is committed before scheduling the next delivery, so counters and
  attempt history remain durable.
- Submission/requeue database commits and broker publication are not atomic. A
  broker failure returns 503 while leaving a visible QUEUED record. A production
  extension should use a transactional outbox and recovery publisher.
- A process crash after a RUNNING commit can strand work. A production extension
  should add leases/heartbeats and a recovery process.
- Worker and Redis inspection have short timeouts and return degraded monitoring
  data instead of failing the summary endpoint.

## Data model

`tasks` stores identity, type, JSON payload, priority, status, bounded retry
counters, lifecycle timestamps, and the last error. `task_execution_attempts`
stores a normalized one-to-many execution audit trail and enforces unique
attempt numbers per task. Database constraints protect enum-like domain values
and non-negative retry data.

## Security posture

The repository contains safe defaults and placeholders only. `.env`, virtual
environments, caches, and local database files are ignored. Container credentials
must be supplied at runtime. Payloads should never contain secrets. The demo has
no authentication and should not be exposed directly to an untrusted network.
