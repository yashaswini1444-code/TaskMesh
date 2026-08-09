"""Genuine multi-connection concurrency proof for the atomic task claim,
against real PostgreSQL row-level locking and MVCC — not SQLite's coarse
whole-database write lock.

tests/test_recovery.py already proves the equivalent guard for lease
recovery under SQLite (two real connections to a file-backed database, since
SQLite still needs that much to mean anything). This file is the PostgreSQL
counterpart for the primary claim path (`process_task` / `_claim_task`),
which is the specific mechanism that makes duplicate Celery delivery safe
(see app/workers/celery_app.py's task_acks_late documentation).
"""

from __future__ import annotations

import threading
from uuid import UUID

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.models import Task, TaskExecutionAttempt, TaskStatus
from app.services.task_lifecycle import LifecycleOutcome, process_task

pytestmark = pytest.mark.integration

CONCURRENT_CLAIMANTS = 8


@pytest.fixture
def pg_session_factory() -> sessionmaker[Session]:
    engine = create_engine(
        get_settings().database_url,
        pool_size=CONCURRENT_CLAIMANTS + 2,
        max_overflow=5,
    )
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def _create_queued_task(session_factory: sessionmaker[Session]) -> UUID:
    with session_factory() as session:
        task = Task(
            task_type="echo",
            payload={"message": "pg-claim-race"},
            status=TaskStatus.QUEUED,
        )
        session.add(task)
        session.commit()
        return task.id


def test_concurrent_claims_against_real_postgres_execute_exactly_once(
    pg_session_factory: sessionmaker[Session],
) -> None:
    """CONCURRENT_CLAIMANTS threads, each with its own PostgreSQL
    connection, race to claim and execute the same QUEUED task via the
    exact same code path a real duplicate Celery delivery would use
    (process_task). Exactly one must complete it; every other claimant must
    observe it as already claimed (SKIPPED) — never execute the handler
    twice, and never error.
    """

    task_id = _create_queued_task(pg_session_factory)

    outcomes: list[LifecycleOutcome] = []
    errors: list[BaseException] = []
    outcomes_lock = threading.Lock()
    barrier = threading.Barrier(CONCURRENT_CLAIMANTS)

    def attempt_claim(worker_number: int) -> None:
        barrier.wait()
        try:
            outcome = process_task(
                task_id,
                worker_identifier=f"pg-race-worker-{worker_number}",
                session_factory=pg_session_factory,
            )
        except BaseException as exc:  # surfaced via the assertion below
            with outcomes_lock:
                errors.append(exc)
            return
        with outcomes_lock:
            outcomes.append(outcome)

    threads = [
        threading.Thread(target=attempt_claim, args=(number,))
        for number in range(CONCURRENT_CLAIMANTS)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == [], f"unexpected errors from concurrent claimants: {errors!r}"
    assert outcomes.count(LifecycleOutcome.COMPLETED) == 1
    assert outcomes.count(LifecycleOutcome.SKIPPED) == CONCURRENT_CLAIMANTS - 1

    with pg_session_factory() as session:
        persisted = session.get(Task, task_id)
        assert persisted is not None
        assert persisted.status is TaskStatus.COMPLETED
        attempts = (
            session.query(TaskExecutionAttempt)
            .filter(TaskExecutionAttempt.task_id == task_id)
            .all()
        )
    # If the claim guard failed under real concurrency, this would be > 1 —
    # the whole point of the test.
    assert len(attempts) == 1
