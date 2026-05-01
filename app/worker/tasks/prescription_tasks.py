import logging
from datetime import timedelta

from app.utils.time import utc_now
from app.worker.base_task import BaseTaskWithDLQ
from app.worker.celery_app import celery_app
from app.worker.db import get_task_db
from app.worker.task_base import async_task

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.worker.tasks.prescription_tasks.cleanup_prescription_drafts",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def cleanup_prescription_drafts():
    """Delete prescription drafts that were never finalized within 7 days.

    A draft with no PDF is not a medical record — it is an abandoned work-in-progress.
    Auto-finalizing it (setting is_draft=False without a PDF) would create a record
    that appears in the patient's prescription history but cannot be opened, and that
    the doctor can no longer edit or regenerate. Deletion is the correct action.
    """
    db = await get_task_db()
    cutoff = utc_now() - timedelta(days=7)
    result = await db.prescriptions.delete_many(
        {
            "is_draft": True,
            "created_at": {"$lte": cutoff},
        },
    )
    logger.info("cleanup_prescription_drafts: deleted=%d", result.deleted_count)


@celery_app.task(
    name="app.worker.tasks.prescription_tasks.cleanup_stuck_generation_locks",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True,
)
@async_task
async def cleanup_stuck_generation_locks():
    """Reset generation_in_progress flags that were never cleared.

    If the FastAPI process crashes between setting generation_in_progress=True
    and the final prescriptions.update_one that clears it, the prescription is
    permanently stuck: the doctor cannot regenerate (409) and cannot edit
    (is_draft is still True so editing is allowed, but generation is blocked).
    Any prescription still flagged after 10 minutes is assumed to have failed
    mid-flight and is reset to a clean draft state so the doctor can retry.
    """
    db = await get_task_db()
    cutoff = utc_now() - timedelta(minutes=10)
    result = await db.prescriptions.update_many(
        {
            "is_draft": True,
            "generation_in_progress": True,
            "updated_at": {"$lte": cutoff},
        },
        {
            "$set": {
                "generation_in_progress": False,
                "updated_at": utc_now(),
            }
        },
    )
    logger.info("cleanup_stuck_generation_locks: released=%d", result.modified_count)

