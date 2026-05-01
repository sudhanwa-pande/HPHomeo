import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.services.cache_service import invalidate_doctor_cache
from app.utils.time import utc_now
from app.worker.base_task import BaseTaskWithDLQ
from app.worker.celery_app import celery_app
from app.worker.db import ensure_task_redis, get_task_db
from app.worker.task_base import async_task

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.worker.tasks.cache_tasks.warm_doctor_cache",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def warm_doctor_cache():
    db = await get_task_db()
    await ensure_task_redis()

    now = utc_now()
    ist = ZoneInfo("Asia/Kolkata")
    today_ist = now.astimezone(ist).date()
    tomorrow_ist = today_ist + timedelta(days=1)
    day_after_ist = today_ist + timedelta(days=2)

    tomorrow_start = datetime(
        tomorrow_ist.year,
        tomorrow_ist.month,
        tomorrow_ist.day,
        0,
        0,
        0,
        tzinfo=ist,
    ).astimezone(timezone.utc)
    day_after_start = datetime(
        day_after_ist.year,
        day_after_ist.month,
        day_after_ist.day,
        0,
        0,
        0,
        tzinfo=ist,
    ).astimezone(timezone.utc)

    pipeline = [
        {
            "$match": {
                "status": "confirmed",
                "scheduled_at": {
                    "$gte": tomorrow_start,
                    "$lt": day_after_start,
                },
            }
        },
        {"$group": {"_id": "$doctor_id"}},
    ]
    docs = await db.appointments.aggregate(pipeline).to_list(length=500)

    for doc in docs:
        await invalidate_doctor_cache(
            str(doc["_id"]),
            day=tomorrow_ist.isoformat(),
        )
    logger.info("warm_doctor_cache: invalidated=%d doctors", len(docs))

