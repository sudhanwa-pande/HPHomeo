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
        "call_leader": 15,
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
        "call_epoch": 7200,
        "call:last_seen": 7200,
        "call:last_ts": 5,
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
    def call_leader(app_id: str, role: str) -> str:
        return f"call_leader:{app_id}:{role}"

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

    @staticmethod
    def epoch_key(app_id: str, role: str) -> str:
        return f"call_epoch:{app_id}:{role}"

    @staticmethod
    def last_seen_key(app_id: str, role: str) -> str:
        return f"call:last_seen:{app_id}:{role}"

    @staticmethod
    def last_ts_key(app_id: str, role: str) -> str:
        return f"call:last_ts:{app_id}:{role}"

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
    for field in ["session_id", "epoch", "token_id"]:
        if field not in data:
            raise RedisCorruptionError(key, f"missing {field} in leader data")
    try:
        epoch = int(data["epoch"])
        if epoch < 0:
            raise ValueError()
    except (ValueError, TypeError):
        raise RedisCorruptionError(key, "invalid epoch in leader data")

LUA_ACQUIRE_LOCK = """
local lock_key = KEYS[1]
local version_key = KEYS[2]
local leader_key = KEYS[3] -- call_leader:{app_id}:{role}
local epoch_key = KEYS[4]
local expected_version = ARGV[1] ~= "" and tonumber(ARGV[1]) or 0
local token_id = ARGV[2]
local session_id = ARGV[3]

local curr_version = tonumber(redis.call("GET", version_key) or "0")
if expected_version < curr_version then
    return {-1, curr_version}
end

if expected_version > curr_version then
    redis.call("SET", version_key, expected_version)
end

local next_epoch = redis.call("INCR", epoch_key)
if next_epoch > 1000000 then
    next_epoch = 1
    redis.call("SET", epoch_key, next_epoch)
end
redis.call("EXPIRE", epoch_key, 7200)

local lock_token = redis.call("INCR", lock_key .. ":counter")
redis.call("SET", lock_key, lock_token, "PX", 3000)

local new_data = {
    session_id = session_id,
    epoch = next_epoch,
    token_id = token_id,
    lock_token = lock_token,
    authority_version = expected_version
}

redis.call("SET", leader_key, cjson.encode(new_data), "EX", 15)
return {1, next_epoch, lock_token, expected_version}
"""

LUA_REFRESH_LEASE = """
local leader_key = KEYS[1] -- call_leader:{app_id}:{role}
local active_token_key = KEYS[2]
local epoch_key = KEYS[3]
local kill_switch_key = KEYS[4]
local token_id = ARGV[1]
local session_id = ARGV[2]
local epoch = tonumber(ARGV[3])
local db_version = tonumber(ARGV[4])
local lease_ttl = tonumber(ARGV[5]) or 15

-- 1. Kill Switch Check
local kill = redis.call("EXISTS", kill_switch_key)
if kill == 1 then
    return -4
end

-- 2. Active Token Check
local active = redis.call("GET", active_token_key)
if active ~= token_id then
    return -3
end

-- 3. Stale Epoch Check
local stored_epoch = tonumber(redis.call("GET", epoch_key) or "0")
if stored_epoch and epoch < stored_epoch then
    return -2
end

local raw = redis.call("GET", leader_key)
local final_epoch = math.max(epoch, stored_epoch)
if raw then
    local role_data = cjson.decode(raw)
    -- Strict Session Binding: token_id and session_id must match
    if role_data and role_data.token_id == token_id and role_data.session_id == session_id then
        redis.call("EXPIRE", leader_key, lease_ttl)
        return {1, final_epoch, role_data.session_id}
    end
end

-- Recovery path (active token matches, but no valid lease matches the session/token)
local new_data = {
    session_id = session_id,
    epoch = final_epoch,
    token_id = token_id,
    lock_token = 0,
    authority_version = db_version
}
redis.call("SET", leader_key, cjson.encode(new_data), "EX", lease_ttl)
return {2, final_epoch, session_id}
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
