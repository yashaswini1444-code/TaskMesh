# Evidence-based resume claims

Every claim below is scoped to behavior actually implemented and verified in
this repository — either by the deterministic test suite, by the CI
integration job against real PostgreSQL/Redis/Celery, or both. Where a claim
is conditional on something (a specific job, a specific test), that's stated
rather than implied away. Add measured throughput/latency numbers only after
running `scripts/load_test.py` on a named environment and recording the
command, sample size, and result — never state a number "from memory."

## Safe claims

These are backed by passing tests against real infrastructure, not just
mocks, and can be defended in detail if asked "how do you know?"

- Built an asynchronous job-processing API (FastAPI, SQLAlchemy 2.x,
  PostgreSQL, Redis, Celery) using the database as the sole durable source
  of task state — no Celery result backend is configured.
- Designed priority-isolated HIGH/MEDIUM/LOW task queues with one dedicated
  worker process per queue, verified in CI by confirming which specific
  worker process executed a given priority's task, not just that a message
  was addressed to the right queue.
- Implemented atomic, compare-and-swap task claiming that makes duplicate
  Celery message delivery safe against double execution — proven under real
  concurrent load against PostgreSQL row-level locking (eight concurrent
  claimants, not a single-threaded assertion).
- Implemented bounded exponential retry backoff (2s/4s/8s at default
  settings, capped at 16s) with durable per-attempt history and dead-letter
  handling on retry exhaustion.
- Designed and implemented lease-based recovery for tasks abandoned by a
  crashed worker — a stale `RUNNING` task is automatically reclaimed
  (requeued and redispatched, or dead-lettered per its retry budget) via a
  periodic scan and an on-demand administrative endpoint, with the recovery
  transition itself guarded against racing a worker that turns out not to
  be dead.
- Implemented graceful multi-signal degradation for an operational
  monitoring endpoint: database, Celery worker, and Redis queue health are
  reported and degrade independently, with no fabricated data and no
  leaked exception detail on any one of them failing.
- Built a CI pipeline with three independent jobs: a fast deterministic
  suite (SQLite, no infrastructure, ~10s), static analysis (ruff + mypy),
  and a full integration job that provisions real PostgreSQL and Redis
  service containers, runs real Celery workers matching the production
  topology, and drives the whole stack through the live HTTP API.
- Implemented normalized execution-attempt auditing (worker identity,
  timestamps, error text) with lifecycle visibility through a monitoring API
  and a dashboard, including administrative recovery actions (dead-letter
  requeue, stuck-task redispatch, stale-task reclaim) exposed as real,
  confirm-gated operations — not decorative UI.
- Explicitly chose and documented Celery's delivery/acknowledgement
  semantics (late-ack, worker-loss rejection) rather than leaving them at
  framework defaults, with the resulting duplicate-delivery risk
  deliberately made safe by the atomic claim mechanism above.

## Conditional claims

True, but state the condition — don't let the headline outrun the scope.

- "Tested against a production-shaped topology" — true of the CI
  `integration` job and Docker Compose, both of which run one worker per
  queue plus a scheduler; the *deterministic* suite (94 of 106 tests) runs
  against SQLite only and proves business logic, not infrastructure
  behavior.
- "At-least-once task delivery" — true given the configured Celery ack
  settings; do not shorten this to "reliable delivery" without the
  at-least-once qualifier, since duplicate delivery is an accepted
  consequence, not something eliminated.
- "Automatic crash recovery" — true for a worker process crash (lease
  recovery) and for message loss on submission (manual redispatch exists);
  not true for every possible failure mode (e.g. no transactional outbox,
  so a broker outage during submission still requires the redispatch action
  to be taken).
- "94 passing tests" (or whatever the current count is) — state that this
  is the deterministic count; mention the additional integration tests
  separately if citing a total, since they require infrastructure the
  reader can't assume is running.
- Any throughput/latency figure from `scripts/load_test.py` — valid only
  with the exact command, environment, and sample size attached; the tool
  itself documents that results vary with CPU, concurrency, and warm-up.

## Claims to avoid

Not because they sound bad, but because they are not true of this system —
each would fail a direct follow-up question.

- **Exactly-once execution.** Explicitly not provided, in two distinct
  ways: broker redelivery (mitigated, not eliminated, by the atomic claim)
  and lease-recovery redispatch racing a worker that wasn't actually dead
  (a documented, deliberate trade-off, not a bug, but still not
  exactly-once).
- **Strict global priority ordering.** Priorities guarantee queue/worker
  isolation, not a total completion order across priorities.
- **A transactional outbox** or any claim that the persist-then-dispatch
  window is "solved" rather than mitigated with a manual redispatch action.
- **Production security posture.** No authentication, authorization, rate
  limiting, or payload size limits exist. This is explicitly out of scope
  for a portfolio project, not an oversight to gloss over.
- **Full observability / distributed tracing / metrics export.** What
  exists is structured application logging and a point-in-time monitoring
  snapshot endpoint. There is no metrics pipeline and no tracing — say so
  if asked, rather than let "observability" imply more than that.
- **Any specific measured throughput number** without the command,
  environment, and date attached.
