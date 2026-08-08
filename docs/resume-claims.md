# Evidence-based resume claims

These claims are intentionally scoped to behavior implemented and tested in this
repository. Add measured numbers only after running the documented workload on a
named environment.

- Built an asynchronous job-processing API with FastAPI, SQLAlchemy, PostgreSQL,
  Redis, and Celery, using the database as the durable task-history source.
- Designed priority-isolated HIGH/MEDIUM/LOW queues with dedicated workers,
  atomic task claims, bounded exponential retries, and dead-letter requeue.
- Implemented normalized execution-attempt auditing and lifecycle visibility
  through monitoring APIs and a polling dashboard.
- Created an offline test strategy using temporary SQLite databases, dependency
  injection, fake dispatchers, and fake infrastructure monitors.
- Packaged a multi-service development environment with Docker Compose and added
  automated Python 3.12 tests in GitHub Actions.

Avoid claiming exactly-once execution, strict global priority ordering, automatic
crash recovery, production security, or a measured throughput figure without
additional evidence; those properties are not provided by the current design.
