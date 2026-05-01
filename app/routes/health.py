from time import perf_counter

from fastapi import APIRouter
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.redis import get_redis

router = APIRouter()

@router.get("/health", dependencies=[rl(30, 60)])
async def health():
    db = get_db()
    await db.command("ping")
    redis = get_redis()
    t0 = perf_counter()
    await redis.ping()
    redis_ping_ms = round((perf_counter() - t0) * 1000, 2)
    return {
        "status": "ok",
        "db": "ok",
        "redis": "ok",
        "redis_ping_ms": redis_ping_ms,
    }
