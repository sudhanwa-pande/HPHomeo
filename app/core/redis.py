import logging

from redis.asyncio import Redis

from app.core.config import settings

logger = logging.getLogger(__name__)

_redis_client: Redis | None = None
_redis_pubsub_client: Redis | None = None

# Pub/Sub pool size — each open SSE tab = 1 connection held.
# For SaaS scale: (max_concurrent_doctors × avg_tabs) + headroom.
# 200 supports ~150 concurrent SSE streams comfortably.
PUBSUB_MAX_CONNECTIONS = 200


async def connect_redis() -> None:
    global _redis_client, _redis_pubsub_client

    _redis_client = Redis.from_url(
        settings.REDIS_URL,
        max_connections=settings.REDIS_MAX_CONNECTIONS,
        socket_timeout=settings.REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=settings.REDIS_SOCKET_CONNECT_TIMEOUT,
        retry_on_timeout=settings.REDIS_RETRY_ON_TIMEOUT,
        decode_responses=True,
    )
    await _redis_client.ping()

    # Dedicated client for Pub/Sub — SSE subscribers hold connections for the
    # lifetime of the stream (minutes/hours), so they need:
    #   - A separate pool so they don't starve cache/rate-limit connections
    #   - No socket_timeout — long-lived idle connections are expected
    #   - A generous pool size (each open SSE tab = 1 connection held)
    _redis_pubsub_client = Redis.from_url(
        settings.REDIS_URL,
        max_connections=PUBSUB_MAX_CONNECTIONS,
        socket_timeout=None,
        socket_connect_timeout=settings.REDIS_SOCKET_CONNECT_TIMEOUT,
        retry_on_timeout=False,
        decode_responses=True,
    )
    await _redis_pubsub_client.ping()
    logger.info(
        "Redis connected — general pool=%d, pubsub pool=%d",
        settings.REDIS_MAX_CONNECTIONS,
        PUBSUB_MAX_CONNECTIONS,
    )

    # Warn at startup if Redis has no memory limit. Without maxmemory set,
    # unbounded key growth will eventually OOM the process and also starve
    # the Celery broker (which shares the same Redis instance).
    try:
        mem_config = await _redis_client.config_get("maxmemory")
        maxmemory = int(mem_config.get("maxmemory", 0))
        if maxmemory == 0:
            logger.warning(
                "Redis maxmemory is not configured (unlimited). "
                "Set maxmemory and maxmemory-policy in redis.conf to prevent "
                "OOM crashes and Celery broker starvation under cache growth. "
                "Recommended: maxmemory-policy=allkeys-lru"
            )
    except Exception:
        logger.debug("Could not read Redis maxmemory config", exc_info=True)


def get_redis() -> Redis:
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Startup event not executed.")
    return _redis_client


def get_redis_pubsub() -> Redis:
    """Return the Redis client dedicated to Pub/Sub operations (SSE streams)."""
    if _redis_pubsub_client is None:
        raise RuntimeError("Redis Pub/Sub client not initialized. Startup event not executed.")
    return _redis_pubsub_client


async def close_redis() -> None:
    global _redis_client, _redis_pubsub_client
    if _redis_pubsub_client is not None:
        await _redis_pubsub_client.aclose()
    _redis_pubsub_client = None
    if _redis_client is not None:
        await _redis_client.aclose()
    _redis_client = None
