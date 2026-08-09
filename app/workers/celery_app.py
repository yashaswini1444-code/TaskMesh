from celery import Celery
from kombu import Queue

from app.core.config import get_settings
from app.core.logging import configure_logging

configure_logging()
settings = get_settings()

celery_app = Celery(
    "taskmesh",
    broker=settings.celery_broker_url,
    include=["app.workers.tasks"],
)
celery_app.conf.update(
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    enable_utc=True,
    timezone="UTC",
    task_always_eager=False,
    task_ignore_result=True,
    # --- Delivery / acknowledgement semantics (deliberately chosen, not
    # left at Celery's defaults) -------------------------------------------
    #
    # task_acks_late=True: a message is acknowledged only *after* the task
    # returns (success, or an exception that isn't retried), not the moment
    # a worker receives it. Without this (Celery's default is early-ack),
    # a worker killed between "received the message" and "started the DB
    # claim transaction" loses that message permanently — it is never
    # redelivered, and the task is stuck QUEUED forever with zero execution
    # attempts, silently. Late-ack closes that window: an unacked message
    # is eventually redelivered (subject to the broker's visibility
    # timeout, below).
    #
    # task_reject_on_worker_lost=True: if the worker process itself is
    # killed (not just the task raising), the in-flight message is
    # rejected/requeued rather than left in limbo, reinforcing the same
    # guarantee for the "worker dies mid-execution" case specifically.
    #
    # Consequence: duplicate broker delivery becomes possible. A message
    # can be redelivered while the *original* worker is still legitimately
    # executing it (broker visibility timeout elapses before the job
    # finishes), or after it already finished but before the ack landed.
    # This is why app.services.task_lifecycle._claim_task exists: the
    # redelivered message's claim attempt is a conditional
    # `UPDATE ... WHERE status = 'QUEUED'`, so a duplicate delivery for a
    # task that is already RUNNING/COMPLETED/etc. is a safe no-op — it does
    # not re-execute the job. That guard is what makes late-ack safe to
    # enable here at all.
    #
    # What this does NOT provide: exactly-once execution. If a job's
    # side effect (e.g. an external API call) happens, and *then* the
    # worker is killed before the DB commit that would mark it COMPLETED,
    # the task is later either lease-recovered (app.services.recovery) or
    # redelivered by the broker — either way, the job's application logic
    # runs again. TaskMesh provides at-least-once *delivery* plus
    # duplicate-*processing* protection at the persisted-task-id level, not
    # idempotent business side effects. Handlers that perform non-idempotent
    # external effects are responsible for their own idempotency keys.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Redis transport's own redelivery window for unacked messages. Set
    # close to the application-level task lease so broker-level and
    # DB-level recovery windows stay in the same order of magnitude instead
    # of drifting independently (Redis defaults to 3600s, far longer than
    # this system's lease).
    broker_transport_options={"visibility_timeout": settings.task_lease_seconds},
    task_queues=(
        Queue("high"),
        Queue("medium"),
        Queue("low"),
        # Administrative/periodic work (lease recovery) is isolated on its
        # own queue so it never competes with priority task capacity.
        Queue("control"),
    ),
    task_default_queue="medium",
    task_create_missing_queues=False,
    # Periodically reclaim RUNNING tasks whose execution lease has expired
    # (crashed/killed worker). See app.services.recovery.recover_stale_tasks
    # and app.workers.tasks.recover_stale_tasks_task.
    beat_schedule={
        "recover-stale-running-tasks": {
            "task": "taskmesh.recover_stale_tasks",
            "schedule": settings.task_recovery_interval_seconds,
            "options": {"queue": "control"},
        },
    },
)
