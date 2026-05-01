import logging
from datetime import datetime, timezone

from app.core.config import settings
from app.core.redis import get_redis
from app.core.redis_keys import blacklist_key
from app.utils.time import utc_now

logger = logging.getLogger(__name__)


async def is_token_blacklisted(db, jti: str | None) -> bool:
    if not jti:
        return True

    backend = (settings.BLACKLIST_BACKEND or "redis").lower()

    redis_ok = False
    redis_hit = False
    mongo_ok = False
    mongo_hit = False

    if backend in {"redis", "dual"}:
        try:
            redis = get_redis()
            redis_hit = bool(await redis.exists(blacklist_key(jti)))
            redis_ok = True
        except Exception:
            logger.critical(
                "Token blacklist: Redis unavailable — failing closed for jti=%s", jti, exc_info=True
            )

    if backend in {"mongo", "dual"}:
        try:
            mongo_hit = bool(await db.token_blacklist.find_one({"jti": jti}, {"_id": 1}))
            mongo_ok = True
        except Exception:
            logger.critical(
                "Token blacklist: MongoDB unavailable — failing closed for jti=%s", jti, exc_info=True
            )

    # If every configured backend failed we cannot verify the token — deny access.
    if backend == "redis" and not redis_ok:
        return True
    if backend == "mongo" and not mongo_ok:
        return True
    if backend == "dual" and not redis_ok and not mongo_ok:
        return True

    return redis_hit or mongo_hit


async def blacklist_token(db, jti: str, expires_at: datetime, created_at: datetime | None = None) -> None:
    backend = (settings.BLACKLIST_BACKEND or "redis").lower()
    now = created_at or utc_now()

    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    ttl_seconds = max(1, int((expires_at - now).total_seconds()))

    if backend in {"redis", "dual"}:
        redis = get_redis()
        await redis.set(blacklist_key(jti), "1", ex=ttl_seconds)

    if backend in {"mongo", "dual"}:
        await db.token_blacklist.update_one(
            {"jti": jti},
            {"$set": {"jti": jti, "expires_at": expires_at, "created_at": now}},
            upsert=True,
        )

