import json
import uuid

from app.core.redis import get_redis
from app.core.redis_keys import payment_order_key, payment_order_lock_key

PAYMENT_ORDER_TTL_SECONDS = 600
PAYMENT_ORDER_LOCK_TTL_SECONDS = 15

_UNLOCK_IF_OWNER_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


async def get_cached_order(appointment_id: str) -> dict | None:
    redis = get_redis()
    payload = await redis.get(payment_order_key(appointment_id))
    if not payload:
        return None
    try:
        data = json.loads(payload)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


async def set_cached_order(appointment_id: str, data: dict, ttl_seconds: int = PAYMENT_ORDER_TTL_SECONDS) -> None:
    redis = get_redis()
    await redis.set(payment_order_key(appointment_id), json.dumps(data), ex=ttl_seconds)


async def acquire_order_lock(appointment_id: str, ttl_seconds: int = PAYMENT_ORDER_LOCK_TTL_SECONDS) -> str | None:
    redis = get_redis()
    owner = str(uuid.uuid4())
    ok = await redis.set(payment_order_lock_key(appointment_id), owner, ex=ttl_seconds, nx=True)
    if not ok:
        return None
    return owner


async def release_order_lock(appointment_id: str, owner: str) -> None:
    redis = get_redis()
    await redis.eval(_UNLOCK_IF_OWNER_LUA, 1, payment_order_lock_key(appointment_id), owner)

