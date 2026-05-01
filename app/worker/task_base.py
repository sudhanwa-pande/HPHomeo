import asyncio
from functools import wraps

from app.core.redis import close_redis
from app.worker.db import close_task_db

def async_task(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        async def _runner():
            try:
                return await fn(*args, **kwargs)
            finally:
                # Tasks run in isolated asyncio.run loops; close loop-bound clients each run.
                await close_task_db()
                await close_redis()

        return asyncio.run(_runner())

    return wrapper
