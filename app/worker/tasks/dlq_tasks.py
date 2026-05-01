import logging

from app.utils.time import utc_now
from app.worker.celery_app import celery_app
from app.worker.db import get_task_db
from app.worker.task_base import async_task

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.worker.tasks.dlq_tasks.handle_failed_task",
    queue="dlq_v1",
)
@async_task
async def handle_failed_task(
    failed_task_id: str,
    error: str,
    task_name: str,
    task_args: list | None = None,
    task_kwargs: dict | None = None,
):
    logger.error(
        "DLQ: task=%s id=%s error=%s",
        task_name,
        failed_task_id,
        error,
    )
    try:
        db = await get_task_db()
        await db.failed_tasks.insert_one({
            "task_id": failed_task_id,
            "task_name": task_name,
            "task_args": task_args or [],
            "task_kwargs": task_kwargs or {},
            "error": error,
            "failed_at": utc_now(),
            "replayed": False,
        })
    except Exception:
        logger.exception(
            "DLQ: failed to persist to MongoDB task=%s id=%s",
            task_name,
            failed_task_id,
        )
