# Interview guide

Questions a serious backend/distributed-systems interviewer could actually
ask, with answers that hold up under a follow-up "how do you know?"

## Why is PostgreSQL the source of truth?

Broker messages are transient delivery signals. Persisting status and
attempts in the relational database gives clients durable, queryable history
and avoids coupling correctness to Celery result retention. No result
backend is configured (`task_ignore_result=True`) — Celery is transport
only.

## How are duplicate deliveries handled?

A worker uses a conditional database update
(`UPDATE tasks SET status='RUNNING' ... WHERE status='QUEUED'`) to move only
a `QUEUED` task to `RUNNING`. If another worker — or a redelivered copy of
the same message — already claimed it, the duplicate delivery finds zero
rows affected and exits without executing the handler. This is proven
against real PostgreSQL row-locking with eight threads racing the same task
row (`tests/integration/test_postgres_concurrency.py`), not just asserted.

## Why is duplicate delivery even possible? Didn't you choose that?

Yes, deliberately. `task_acks_late=True` means a message is only acked after
the task finishes, which is what makes it survivable when a worker is killed
between receiving a message and starting the DB claim — without late-ack,
that message is silently lost forever (task stuck `QUEUED`, zero attempts,
no error). The cost of late-ack is that a message can be redelivered while
the original worker is still executing it. That's an acceptable trade
specifically *because* the atomic claim above makes the redelivery a safe
no-op. Enabling one without the other would be wrong in both directions.

## What do priorities actually guarantee?

Real queue and worker capacity isolation — a HIGH task cannot be stuck
behind a LOW backlog, because Docker Compose (and CI) run one dedicated
worker process per queue, not one worker consuming all three. They do not
guarantee every HIGH task finishes before every lower-priority task
globally, because each queue's own tasks still execute concurrently within
that worker, and completion order isn't otherwise coordinated across
queues. The isolation claim is proven in CI by checking which specific
worker process (`worker-high`, not `worker-medium`) actually executed a
HIGH-priority task.

## How do retries work, exactly?

Only handlers that raise `RetryableTaskExecutionError` retry — an
unsupported task type or a malformed payload raises a plain
`TaskExecutionError` and goes straight to `FAILED`, no retry. When a retry
is warranted, state and the attempt record are committed *before* Celery is
told to retry, so a crash between those two steps still leaves durable,
correct state. Backoff is `min(2**retry_number, 16)` seconds; with the
default `max_retries=3`, that's 2, 4, 8 seconds — the 16s cap exists and is
tested, but is only reachable with `max_retries >= 4`. Exhaustion moves the
task to `DEAD_LETTER`, preserving every prior attempt.

## What happens if a worker dies mid-task?

Until recently: stuck forever, no recovery, documented as a known gap. Now:
every claimed task carries a `lease_expires_at`; a periodic scan (Celery
Beat, every 30s by default) and an on-demand admin endpoint both reclaim
`RUNNING` tasks whose lease has expired, using the same compare-and-swap
pattern as the claim itself, so a reclaim scan can never race incorrectly
against the original worker actually finishing at the same moment. Reclaimed
tasks go back through the normal retry-budget decision (`QUEUED`+redispatch
or `DEAD_LETTER`). I'd explain the trade-off unprompted: if the worker
wasn't actually dead, just slow, its real side effects can still land after
a second attempt already ran — that's a deliberate consequence of a
timeout-based lease, not an oversight, and it's why the lease default (300s)
should be set well above realistic job duration.

## What are the important failure windows, and which are actually mitigated now?

- **Persist-then-dispatch is not atomic.** A `503` from `POST /tasks`
  leaves a real, visible `QUEUED` row with no broker message. Mitigated
  with `POST /tasks/{id}/redispatch` (manual/administrative, not automatic)
  — not eliminated; a transactional outbox would eliminate it and isn't
  implemented.
- **Worker crash after claiming.** Mitigated by lease recovery, on a
  bounded delay, with the documented possible-double-execution trade-off —
  not eliminated.
- **Duplicate delivery.** Made safe (not prevented) by the atomic claim.
- **Database outage during monitoring.** Was an unhandled 500 until this
  pass; now degrades to `database.available=false` with real fields left
  unset, matching how worker/queue inspection already degraded.

## Why use SQLite in tests, and how do you know it also works on PostgreSQL?

SQLite makes the deterministic suite fast and infrastructure-independent —
94 tests run in about 10 seconds with zero setup. But that suite alone would
only prove the code works against SQLite. A separate CI job
(`integration`, marked `pytest.mark.integration`, excluded from the default
run) starts real `postgres:17-alpine` and `redis:7-alpine` service
containers, applies migrations, starts four real Celery worker processes
matching the docker-compose topology, starts the actual FastAPI app, and
drives all of it through the real HTTP API — priority routing (checked via
which worker process actually processed the task), a deterministic failure
path, dead-letter requeue through a real redispatch, and monitoring against
real infrastructure. That job also carries the PostgreSQL-specific
concurrency proof, since SQLite's coarse whole-database locking wouldn't
actually exercise the row-level locking the claim mechanism depends on in
production.

## How is observability degraded safely?

Database summaries still return when Celery or Redis inspection fails, and
now the reverse holds too — worker/queue summaries still return truthfully
if the database is briefly unreachable. External inspection has bounded
timeouts and reports sanitized availability/error fields; failure logs carry
exception *type*, not raw exception text, so credentials or connection
details in an exception message never leak into logs or API responses.

## What logging/observability actually exists?

Every lifecycle transition, dispatch attempt, and recovery action logs
through a dedicated `taskmesh.*` logger namespace with `task_id` and
relevant structured fields (never the payload). It's plain Python logging
with a custom formatter, not JSON, not shipped anywhere, not correlated
across services beyond the shared `task_id` appearing in each log line. No
metrics export, no distributed tracing — I'd say so directly if asked,
rather than imply otherwise.

## What would you build next, and in what order?

1. A transactional outbox, to actually close the persist-then-dispatch
   window instead of offering manual redispatch.
2. Authentication/authorization and rate limiting — currently out of scope
   entirely, appropriate for a portfolio demo, not for anything real.
3. A configurable per-task-type retry policy and idempotency-key support for
   handlers with real external side effects (the one built-in handler,
   `echo`, has none, so this hasn't been forced yet).
4. Metrics export (Prometheus) and basic tracing, now that structured
   logging exists as a foundation to correlate against.
5. Load/throughput characterization under the real multi-worker topology —
   `scripts/load_test.py` proves submission-path throughput, not end-to-end
   completion throughput under sustained load.
