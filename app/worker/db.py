from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import settings
from app.core.redis import connect_redis, get_redis

_client = None
_db = None


async def get_task_db():
    global _client, _db
    if _db is None:
        _client = AsyncIOMotorClient(settings.MONGODB_URI.get_secret_value())
        _db = _client[settings.MONGODB_DB]
    return _db


async def close_task_db():
    global _client, _db
    if _client:
        _client.close()
    _client, _db = None, None


async def ensure_task_redis():
    try:
        return get_redis()
    except RuntimeError:
        await connect_redis()
        return get_redis()

