import logging

import sentry_sdk
from celery import Task, current_app

logger = logging.getLogger(__name__)


class BaseTaskWithDLQ(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(
            "Task failed permanently: task=%s id=%s error=%s",
            self.name,
            task_id,
            exc,
        )
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("celery.task", self.name)
            scope.set_tag("celery.task_id", task_id)
            scope.set_context("celery", {"task": self.name, "task_id": task_id})
            sentry_sdk.capture_exception(exc)
        try:
            # Serialise args/kwargs so the DLQ record can be used to replay
            # the task manually. Limit depth to avoid storing huge payloads.
            safe_args = list(args) if args else []
            safe_kwargs = dict(kwargs) if kwargs else {}
            current_app.send_task(
                "app.worker.tasks.dlq_tasks.handle_failed_task",
                args=[task_id, str(exc), self.name, safe_args, safe_kwargs],
                queue="dlq_v1",
                exchange="dlq",
                routing_key="dlq",
            )
        except Exception:
            logger.exception("Failed to route task to DLQ: task_id=%s", task_id)
