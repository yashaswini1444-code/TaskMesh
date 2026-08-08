# Interview guide

## Why is PostgreSQL the source of truth?

Broker messages are transient delivery signals. Persisting status and attempts in
the relational database gives clients durable, queryable history and avoids
coupling correctness to Celery result retention.

## How are duplicate deliveries handled?

A worker uses a conditional database update to move only a QUEUED task to
RUNNING. If another worker already claimed it, the duplicate delivery exits
without executing the handler.

## What do priorities guarantee?

They provide queue and worker isolation. They do not guarantee every HIGH task
finishes before every lower-priority task because workers execute concurrently
and brokers prefetch messages.

## How do retries work?

Only explicit transient exceptions retry. State and an attempt record are
committed, then Celery schedules a countdown on the original queue. Backoff is
bounded; exhaustion moves the task to DEAD_LETTER. Permanent errors go directly
to FAILED.

## What are the important failure windows?

The database commit and broker publish are not atomic, so a QUEUED task can exist
without a message. A worker can also crash after committing RUNNING. The natural
next improvements are a transactional outbox plus lease/heartbeat recovery.

## Why use SQLite in tests?

It makes the standard suite deterministic, fast, and infrastructure-independent.
The models and migrations remain PostgreSQL-ready, while a separate container
smoke test is appropriate for dialect- and infrastructure-specific validation.

## How is observability degraded safely?

Database summaries still return when Celery or Redis inspection fails. External
inspection has bounded timeouts and reports sanitized availability/error fields;
the API does not leak broker credentials.

## What would you build next?

Authentication/authorization, rate limiting, a transactional outbox, abandoned
task recovery, structured metrics/tracing, production orchestration, and
PostgreSQL integration tests would be prioritized according to deployment risk.
