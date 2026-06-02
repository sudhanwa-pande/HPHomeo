from time import perf_counter
import json
from datetime import datetime

from fastapi import APIRouter
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.redis import get_redis
from app.utils.time import ensure_utc, utc_now

router = APIRouter()

@router.get("/health", dependencies=[rl(30, 60)])
async def health():
    db = get_db()
    await db.command("ping")
    redis = get_redis()
    t0 = perf_counter()
    await redis.ping()
    redis_ping_ms = round((perf_counter() - t0) * 1000, 2)
    
    celery_beat_status = "ok"
    try:
        beat_raw = await redis.get("celery_beat_heartbeat")
        if not beat_raw:
            celery_beat_status = "missing"
        else:
            beat_data = json.loads(beat_raw)
            beat_time = ensure_utc(datetime.fromisoformat(beat_data["timestamp"]))
            if (utc_now() - beat_time).total_seconds() > 600:
                celery_beat_status = "stale"
    except Exception:
        celery_beat_status = "error"

    return {
        "status": "ok" if celery_beat_status == "ok" else "degraded",
        "db": "ok",
        "redis": "ok",
        "redis_ping_ms": redis_ping_ms,
        "celery_beat": celery_beat_status,
    }
