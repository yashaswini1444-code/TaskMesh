from app.workers.celery_app import celery_app


@celery_app.task(name="taskmesh.execute_task", ignore_result=True)
def execute_task(task_id: str) -> None:
    """Accept a persisted task ID; execution behavior starts in Milestone 6."""

    # The UUID-only message proves the broker boundary without changing task
    # lifecycle state or creating execution-attempt records in this milestone.
    return None
