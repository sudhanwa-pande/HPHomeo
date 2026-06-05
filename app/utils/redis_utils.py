import json
import logging
from typing import Any
from app.core.config import settings

logger = logging.getLogger(__name__)

class RedisCorruptionError(Exception):
    """Raised when retrieved Redis JSON payload is corrupted or invalid."""
    def __init__(self, key: str, details: str = ""):
        msg = f"Corrupted data contract detected for Redis key '{key}'"
        if details:
            msg += f": {details}"
        super().__init__(msg)
        self.key = key

class RedisKeys:
    """Centralized builders and TTL policies for all Redis keys to avoid leaks."""
    TTL_POLICY = {
        "join_lock": 3,
        "prev_token": 10,
        "active_session": 600,
        "call_leader": 5,
        "call_state": 7200,
        "disconnect_key": 900,
        "reconcile_cooldown": 10,
        "kill_switch": 7200,
        "call_created_at": 7200,
        "session_epoch": 600,
        "lock_counter": None,
        "call_version": None,
        "call_log": 7200,
        "metrics:call": 7200,
    }

    @staticmethod
    def join_lock(app_id: str) -> str:
        return f"join_lock:{app_id}"

    @staticmethod
    def lock_counter(app_id: str) -> str:
        return f"lock_counter:{app_id}"

    @staticmethod
    def call_version(app_id: str) -> str:
        return f"call_version:{app_id}"

    @staticmethod
    def call_created_at(app_id: str) -> str:
        return f"call_created_at:{app_id}"

    @staticmethod
    def active_session(session_id: str) -> str:
        return f"active_session:{session_id}"

    @staticmethod
    def call_leader(app_id: str) -> str:
        return f"call_leader:{app_id}"

    @staticmethod
    def session_epoch(app_id: str, session_id: str) -> str:
        return f"session_epoch:{app_id}:{session_id}"

    @staticmethod
    def active_token(app_id: str, role: str) -> str:
        return f"active_token:{app_id}:{role}"

    @staticmethod
    def prev_token(app_id: str, role: str) -> str:
        return f"prev_token:{app_id}:{role}"

    @staticmethod
    def call_state(app_id: str) -> str:
        return f"call:{app_id}"

    @staticmethod
    def disconnect_key(app_id: str) -> str:
        return f"call:disconnected:{app_id}"

    @staticmethod
    def heartbeat_key(app_id: str) -> str:
        return f"call:heartbeat:{app_id}"

    @staticmethod
    def heartbeat_doctor_key(doctor_id: str) -> str:
        return f"call:heartbeat:doctor:{doctor_id}"

    @staticmethod
    def reconcile_cooldown(app_id: str) -> str:
        return f"call:reconcile:cooldown:{app_id}"

    @staticmethod
    def kill_switch(app_id: str) -> str:
        return f"call:terminated:{app_id}"

def ensure_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)

def parse_value(v: Any) -> Any:
    """Only deserializes values starting with '{' or '['.
    
    Prevents primitive integers/strings (like numeric IDs) from being parsed to python ints.
    """
    s = ensure_str(v)
    if s is None:
        return None
    if s.startswith("{") or s.startswith("["):
        try:
            return json.loads(s)
        except Exception:
            return s
    return s

def to_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))

def validate_leader_data(key: str, data: Any) -> None:
    """Enforce schema contract validation for coordination leases."""
    if not isinstance(data, dict):
        raise RedisCorruptionError(key, "data must be a dict")
    if "session_id" not in data or "authority_version" not in data or "lock_token" not in data:
        raise RedisCorruptionError(key, "missing session_id, authority_version or lock_token fields")

LUA_ACQUIRE_LOCK = """
local lock_key = KEYS[1]
local version_key = KEYS[2]
local leader_key = KEYS[3]
local expected_version = ARGV[1] ~= "" and tonumber(ARGV[1]) or nil
local token_id = ARGV[2]
local session_id = ARGV[3]
local role = ARGV[4]

local curr_version = tonumber(redis.call("GET", version_key) or "0")
if expected_version and curr_version > expected_version then
    return {-1, curr_version}
end

local next_version = curr_version + 1
if expected_version and next_version <= expected_version then
    next_version = expected_version + 1
end
redis.call("SET", version_key, next_version)

local lock_token = redis.call("INCR", lock_key .. ":counter")
redis.call("SET", lock_key, lock_token, "PX", 3000)

redis.call("SET", leader_key, cjson.encode({session_id = session_id, authority_version = next_version, lock_token = lock_token, role = role}), "EX", 5)
return {1, next_version, lock_token}
"""

class SafeRedis:
    UNSAFE_READS = {"get", "hget", "hgetall", "smembers", "mget"}

    def __init__(self, redis):
        self.redis = redis

    def __getattr__(self, name):
        if name in self.UNSAFE_READS:
            logger.warning("METRIC unsafe_read_count method=%s", name)
            logger.warning("Unsafe raw Redis read access: '%s'. Use SafeRedis helpers instead.", name)
        return getattr(self.redis, name)

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        return await self.redis.eval(script, len(keys), *keys, *args)

    async def get_str(self, key: str) -> str | None:
        return ensure_str(await self.redis.get(key))

    async def hget_str(self, key: str, field: str) -> str | None:
        return ensure_str(await self.redis.hget(key, field))

    async def mget_str(self, keys: list[str]) -> list[str | None]:
        values = await self.redis.mget(keys)
        return [ensure_str(v) for v in values]

    async def mget_parsed(self, keys: list[str]) -> list[Any]:
        values = await self.redis.mget(keys)
        return [parse_value(v) for v in values]

    async def get_json(self, key: str) -> Any | None:
        raw = await self.get_str(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.error("METRIC redis_corruption key=%s", key)
            raise RedisCorruptionError(key, "invalid JSON format")

    async def set_json(self, key: str, value: Any, ex: int | None = None) -> Any:
        # Verify key-class TTL contracts
        prefix = key.split(":")[0] if ":" in key else key
        policy_ttl = RedisKeys.TTL_POLICY.get(prefix)
        
        if ex is None and policy_ttl is not None:
            ex = policy_ttl
        
        if ex is None:
            logger.warning("METRIC missing_ttl_count key=%s", key)
            if getattr(settings, "REDIS_STRICT_TTL", False):
                raise RuntimeError(f"Strict TTL Contract Violation: TTL required for key: {key}")
            logger.warning("Write contract warning: missing TTL/Expiry (ex=None) for key '%s'", key)
        return await self.redis.set(key, to_json(value), ex=ex)

    async def hgetall_parsed(self, key: str) -> dict[str, Any]:
        data = await self.redis.hgetall(key)
        return {ensure_str(k): parse_value(v) for k, v in data.items()}

    async def smembers_str(self, key: str) -> list[str | None]:
        data = await self.redis.smembers(key)
        return [ensure_str(v) for v in data]
