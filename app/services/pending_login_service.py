import json
import secrets
from typing import Literal

from app.core.redis import get_redis
from app.core.redis_keys import pending_login_key

PENDING_LOGIN_TTL_SECONDS = 600

_CONSUME_LUA = """
local val = redis.call('GET', KEYS[1])
if not val then
  return nil
end
redis.call('DEL', KEYS[1])
return val
"""


def _normalize_ua(user_agent: str | None) -> str:
    return (user_agent or "").strip()[:512]


def _normalize_ip(ip: str | None) -> str:
    return (ip or "").strip()[:64]


async def create_pending_login(
    *,
    doctor_id: str,
    email: str,
    role: str,
    method: Literal["email_otp", "totp"],
    ip: str | None,
    user_agent: str | None,
    ttl_seconds: int = PENDING_LOGIN_TTL_SECONDS,
) -> str:
    redis = get_redis()
    temp_token = secrets.token_urlsafe(32)
    payload = {
        "doctor_id": doctor_id,
        "email": email,
        "role": role,
        "method": method,
        "ip": _normalize_ip(ip),
        "user_agent": _normalize_ua(user_agent),
    }
    await redis.set(
        pending_login_key(temp_token),
        json.dumps(payload),
        ex=ttl_seconds,
    )
    return temp_token


async def get_pending_login(temp_token: str) -> dict | None:
    redis = get_redis()
    raw = await redis.get(pending_login_key(temp_token))
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


async def consume_pending_login(temp_token: str) -> dict | None:
    redis = get_redis()
    raw = await redis.eval(_CONSUME_LUA, 1, pending_login_key(temp_token))
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


async def clear_pending_login(temp_token: str) -> None:
    redis = get_redis()
    await redis.delete(pending_login_key(temp_token))
