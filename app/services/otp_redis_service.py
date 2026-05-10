from app.core.redis import get_redis
from app.core.redis_keys import (
    otp_attempts_key,
    otp_key,
    otp_resend_count_key,
    otp_resend_lock_key,
)

OTP_TTL_SECONDS = 300
OTP_ATTEMPTS_TTL_SECONDS = 300  # Lockout duration: 5 minutes
OTP_MAX_ATTEMPTS = 3            # Tightened for production security (especially TOTP)

# Atomically GET the OTP hash then immediately DELETE it so the key cannot be
# reused in a concurrent request between the caller's GET and its own DEL.
_CONSUME_OTP_LUA = """
local val = redis.call('GET', KEYS[1])
if not val then
  return nil
end
redis.call('DEL', KEYS[1])
return val
"""


async def store_otp(
    *,
    channel: str,
    identity: str,
    purpose: str,
    code_hash: str,
    ttl_seconds: int = OTP_TTL_SECONDS,
) -> None:
    redis = get_redis()
    await redis.set(otp_key(channel, identity, purpose), code_hash, ex=ttl_seconds)
    await redis.delete(otp_attempts_key(channel, identity, purpose))


async def get_otp_hash(*, channel: str, identity: str, purpose: str) -> str | None:
    redis = get_redis()
    return await redis.get(otp_key(channel, identity, purpose))


async def consume_otp_hash(*, channel: str, identity: str, purpose: str) -> str | None:
    """Atomically GET the OTP hash and DELETE the key in one Lua script.

    Use this instead of get_otp_hash() at the point of final verification so
    that two concurrent requests with the same OTP cannot both pass — only the
    first to execute wins. The caller should still call clear_otp() on success
    to clean up the attempts counter and resend state.
    """
    redis = get_redis()
    return await redis.eval(_CONSUME_OTP_LUA, 1, otp_key(channel, identity, purpose))


async def get_attempts(*, channel: str, identity: str, purpose: str) -> int:
    try:
        redis = get_redis()
        value = await redis.get(otp_attempts_key(channel, identity, purpose))
    except Exception:
        # Redis unavailable — fail closed: treat as locked so OTP cannot be brute-forced.
        return OTP_MAX_ATTEMPTS
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


async def increment_attempts(
    *,
    channel: str,
    identity: str,
    purpose: str,
    ttl_seconds: int = OTP_ATTEMPTS_TTL_SECONDS,
) -> int:
    redis = get_redis()
    key = otp_attempts_key(channel, identity, purpose)
    # Atomic pipeline: always re-apply TTL so keys never linger without
    # an expiry (e.g. after a Redis restart between INCR and EXPIRE).
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, ttl_seconds)
        results = await pipe.execute()
    return int(results[0])


async def clear_otp(*, channel: str, identity: str, purpose: str) -> None:
    redis = get_redis()
    await redis.delete(
        otp_key(channel, identity, purpose),
        otp_attempts_key(channel, identity, purpose),
        otp_resend_lock_key(channel, identity, purpose),
        otp_resend_count_key(channel, identity, purpose),
    )


async def acquire_resend_cooldown(
    *,
    channel: str,
    identity: str,
    purpose: str,
    ttl_seconds: int,
) -> bool:
    redis = get_redis()
    return bool(
        await redis.set(
            otp_resend_lock_key(channel, identity, purpose),
            "1",
            ex=ttl_seconds,
            nx=True,
        )
    )


async def get_resend_cooldown_remaining(*, channel: str, identity: str, purpose: str) -> int:
    redis = get_redis()
    ttl = await redis.ttl(otp_resend_lock_key(channel, identity, purpose))
    if ttl is None or ttl < 0:
        return 0
    return int(ttl)


async def increment_resend_count(
    *,
    channel: str,
    identity: str,
    purpose: str,
    ttl_seconds: int,
) -> int:
    redis = get_redis()
    key = otp_resend_count_key(channel, identity, purpose)
    async with redis.pipeline(transaction=True) as pipe:
        pipe.incr(key)
        pipe.expire(key, ttl_seconds)
        results = await pipe.execute()
    return int(results[0])


async def clear_resend_state(*, channel: str, identity: str, purpose: str) -> None:
    redis = get_redis()
    await redis.delete(
        otp_resend_lock_key(channel, identity, purpose),
        otp_resend_count_key(channel, identity, purpose),
    )


def is_locked(attempts: int) -> bool:
    return attempts >= OTP_MAX_ATTEMPTS
