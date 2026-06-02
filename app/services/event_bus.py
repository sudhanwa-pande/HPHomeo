"""
Server-Sent Events (SSE) event bus built on Redis Pub/Sub.

Architecture:
  - Publishers call `publish(channel, event_type, data)` from any route/service.
  - SSE stream endpoints subscribe to one or more channels and yield events.
  - Each SSE connection gets its own Redis Pub/Sub subscriber (lightweight).
  - Heartbeats keep the connection alive through proxies/load-balancers.
  - Publishing uses the general Redis client (fire-and-forget, instant).
  - Subscribing uses a dedicated Redis client to avoid pool starvation.

Channels follow the pattern:
  events:doctor:{doctor_id}          — doctor-scoped events
  events:patient:{patient_user_id}   — authenticated patient events
  events:appointment:{appointment_id} — appointment-scoped events (public + auth)

Publishing contract:
  Patient-initiated actions (waiting, book, cancel, reschedule):
    → notify_doctor() only — the patient already knows what they did.

  Doctor-initiated actions affecting patients (call joins, complete, no-show):
    → notify_appointment() — reaches PUBLIC patients via /public/events/stream/{id}
    → notify_patient()     — reaches AUTHENTICATED patients via /patient/events/stream
    Both are needed because public and auth patients listen on different channels.

Reliability contract:
  SSE + Redis Pub/Sub is at-most-once delivery.  If the client disconnects
  and misses events during the reconnect window, those events are lost.
  Clients MUST poll the relevant REST endpoint after every SSE reconnect
  to fetch current state (e.g., GET /appointments/{id} after EventSource
  fires its ``open`` event on reconnection).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Awaitable, Callable

from bson import ObjectId
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError as RedisConnectionError, RedisError

from app.core.redis import get_redis, get_redis_pubsub

logger = logging.getLogger(__name__)

# ─── Channel helpers ───────────────────────────────────────────

CHANNEL_PREFIX = "events"

def doctor_channel(doctor_id: str) -> str:
    return f"{CHANNEL_PREFIX}:doctor:{doctor_id}"

def patient_channel(patient_user_id: str) -> str:
    return f"{CHANNEL_PREFIX}:patient:{patient_user_id}"

def appointment_channel(appointment_id: str) -> str:
    return f"{CHANNEL_PREFIX}:appointment:{appointment_id}"


# ─── Event types ───────────────────────────────────────────────
# Doctor-scoped
EVENT_PATIENT_WAITING = "patient_waiting"
EVENT_APPOINTMENT_BOOKED = "appointment_booked"
EVENT_APPOINTMENT_CANCELLED = "appointment_cancelled"
EVENT_APPOINTMENT_RESCHEDULED = "appointment_rescheduled"
EVENT_PAYMENT_CONFIRMED = "payment_confirmed"

# Doctor-scoped (status transitions by doctor)
EVENT_APPOINTMENT_COMPLETED = "appointment_completed"
EVENT_APPOINTMENT_NO_SHOW = "appointment_no_show"

# Call state machine — unified event for ALL call state changes
# Replaces the old EVENT_CALL_STATUS_CHANGED and EVENT_CALL_ENDED.
# Emitted to doctor, appointment, and patient channels.
EVENT_CALL_STATE_CHANGED = "call_state_changed"

# Legacy aliases — kept for backward compatibility during transition
EVENT_CALL_STATUS_CHANGED = "call_state_changed"
EVENT_CALL_ENDED = "call_state_changed"


# ─── JSON serializer ──────────────────────────────────────────

def _json_default(obj: Any) -> Any:
    """Fallback serializer for json.dumps — prevents TypeError mid-stream."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ─── Publisher ─────────────────────────────────────────────────

async def publish(channel: str, event_type: str, data: dict[str, Any] | None = None) -> None:
    """Publish an event to a Redis Pub/Sub channel.

    Fire-and-forget — failures are logged, never raised to callers.
    This ensures publishing never breaks the main request flow.

    Every event gets a unique ``id`` so SSE streams can include the
    ``id:`` field.  This lets the browser send ``Last-Event-ID`` on
    reconnect (though replay is not yet supported — see module docstring).
    """
    try:
        redis: Redis = get_redis()
        message = json.dumps(
            {
                "id": uuid.uuid4().hex,
                "type": event_type,
                "data": data or {},
                "ts": datetime.now(timezone.utc).isoformat(),
            },
            default=_json_default,
        )
        await redis.publish(channel, message)
        logger.debug("event_bus.publish channel=%s type=%s", channel, event_type) # codeql[py/clear-text-logging-sensitive-data]
    except Exception:
        logger.warning("event_bus.publish failed channel=%s type=%s", channel, event_type, exc_info=True) # codeql[py/clear-text-logging-sensitive-data]


# ─── Convenience publishers ────────────────────────────────────

async def notify_doctor(doctor_id: str, event_type: str, data: dict[str, Any] | None = None) -> None:
    await publish(doctor_channel(str(doctor_id)), event_type, data)

async def notify_patient(patient_user_id: str, event_type: str, data: dict[str, Any] | None = None) -> None:
    await publish(patient_channel(str(patient_user_id)), event_type, data)

async def notify_appointment(appointment_id: str, event_type: str, data: dict[str, Any] | None = None) -> None:
    await publish(appointment_channel(str(appointment_id)), event_type, data)


# ─── Per-user SSE connection tracking ─────────────────────────
#
# In-memory counter: { channel_name: active_connection_count }
# Each SSE stream increments on enter, decrements on exit.
# This prevents one user with many tabs from exhausting the pool.

MAX_SSE_PER_CHANNEL = 10  # max concurrent SSE connections per channel
# Why 10 (was 2): the previous limit was too tight for normal browser behavior.
# Tab switches, refreshes, and network blips create transient "zombie"
# connections that take a few seconds for the server to detect. With multiple
# uvicorn workers each tracking their own counter, the effective per-user cap
# was being hit during routine reconnects, surfacing as "Offline" badges and
# 429 storms in the SSE stream. 10 gives headroom for these transients while
# still preventing a single user from monopolizing the 200-slot Redis pubsub
# pool defined in app/core/redis.py.

_sse_connections: dict[str, int] = {}
_sse_lock = asyncio.Lock()


async def acquire_sse_slot(channel: str) -> bool:
    """Try to reserve an SSE slot for this channel. Returns False if full."""
    async with _sse_lock:
        current = _sse_connections.get(channel, 0)
        if current >= MAX_SSE_PER_CHANNEL:
            return False
        _sse_connections[channel] = current + 1
        return True


async def release_sse_slot(channel: str) -> None:
    """Release an SSE slot when the stream ends."""
    async with _sse_lock:
        current = _sse_connections.get(channel, 0)
        if current <= 1:
            _sse_connections.pop(channel, None)
        else:
            _sse_connections[channel] = current - 1


def get_sse_connection_counts() -> dict[str, int]:
    """Snapshot of active SSE connections per channel (for monitoring)."""
    return dict(_sse_connections)


# ─── SSE Subscriber / Generator ───────────────────────────────

HEARTBEAT_INTERVAL_SECONDS = 15  # must be < typical proxy timeout (usually 60s)
DEFAULT_MAX_DURATION_SECONDS = 3600  # 1 hour — forces EventSource reconnect + fresh auth

async def subscribe_sse(
    channels: list[str],
    *,
    heartbeat_seconds: int = HEARTBEAT_INTERVAL_SECONDS,
    max_duration_seconds: int = DEFAULT_MAX_DURATION_SECONDS,
    disconnect_check: Callable[[], Awaitable[bool]] | None = None,
    auth_check: Callable[[], Awaitable[bool]] | None = None,
    auth_check_interval_seconds: int = 300,
) -> AsyncGenerator[str, None]:
    """Subscribe to Redis Pub/Sub channels and yield SSE-formatted strings.

    Each yielded string is a complete SSE message:
        id: <event_id>\\nevent: <type>\\ndata: <json>\\n\\n

    Includes periodic heartbeat comments (`: heartbeat`) to keep the
    connection alive through proxies and load-balancers.

    Parameters
    ----------
    channels:
        Redis Pub/Sub channels to subscribe to.
    heartbeat_seconds:
        Interval between heartbeat comments (default 15s).
    max_duration_seconds:
        Maximum stream lifetime.  After this, a ``reconnect`` event is
        sent and the generator exits.  The browser's EventSource will
        auto-reconnect, triggering a fresh auth check.
    disconnect_check:
        Async callable (typically ``request.is_disconnected``) checked
        every poll cycle (~1s) to detect client disconnects promptly.
    auth_check:
        Async callable that returns True if auth is still valid.
        Checked every ``auth_check_interval_seconds``.  If it returns
        False the stream closes so the client must re-authenticate.
    auth_check_interval_seconds:
        How often to call ``auth_check`` (default 5 minutes).

    The generator cleans up the Pub/Sub subscription on exit (when the
    client disconnects or the request is cancelled).
    """
    # Use the dedicated Pub/Sub Redis client so SSE subscribers don't
    # starve the general connection pool used by cache/rate-limiting.
    redis: Redis = get_redis_pubsub()
    pubsub: PubSub = redis.pubsub()

    try:
        await pubsub.subscribe(*channels)
        logger.debug("sse.subscribe channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]

        # Yield an immediate comment so proxies (Next.js, Nginx) see data
        # flowing and keep the connection open instead of closing it.
        yield ": connected\n\n"

        started_at = time.monotonic()
        last_send = started_at
        last_auth_check = started_at

        while True:
            now_mono = time.monotonic()

            # ── Max duration guard ────────────────────────────
            if now_mono - started_at >= max_duration_seconds:
                logger.debug(
                    "sse.subscribe max_duration reached (%ds) channels=%s",
                    max_duration_seconds, channels, # codeql[py/clear-text-logging-sensitive-data]
                )
                yield (
                    'event: reconnect\n'
                    'data: {"reason": "max_duration"}\n\n'
                )
                break

            # ── Client disconnect detection (every poll cycle) ─
            if disconnect_check is not None:
                try:
                    if await disconnect_check():
                        logger.debug("sse.subscribe client disconnected channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
                        break
                except Exception:
                    break

            # ── Periodic auth re-validation ───────────────────
            if auth_check is not None and now_mono - last_auth_check >= auth_check_interval_seconds:
                try:
                    if not await auth_check():
                        logger.info("sse.subscribe auth expired channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
                        yield (
                            'event: auth_expired\n'
                            'data: {"reason": "token_expired_or_revoked"}\n\n'
                        )
                        break
                except Exception:
                    logger.info("sse.subscribe auth check failed channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
                    yield (
                        'event: auth_expired\n'
                        'data: {"reason": "auth_check_error"}\n\n'
                    )
                    break
                last_auth_check = now_mono

            # ── Poll Redis for next message ───────────────────
            # get_message returns None after `timeout` seconds if nothing arrives.
            # We use a short poll (1s) so disconnect/auth checks run frequently.
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0,
                )
            except (RedisConnectionError, RedisError) as exc:
                logger.warning(
                    "sse.subscribe Redis error channels=%s: %s", channels, exc, # codeql[py/clear-text-logging-sensitive-data]
                )
                try:
                    yield (
                        'event: error\n'
                        'data: {"reason": "stream_interrupted"}\n\n'
                    )
                except Exception:
                    pass
                break

            if message is None:
                # No message — check if we need to send a heartbeat
                if time.monotonic() - last_send >= heartbeat_seconds:
                    yield ": heartbeat\n\n"
                    last_send = time.monotonic()
                continue

            if message["type"] != "message":
                continue

            raw = message["data"]
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

            try:
                parsed = json.loads(raw)
                event_type = parsed.get("type", "message")
                event_id = parsed.get("id", "")
                yield f"id: {event_id}\nevent: {event_type}\ndata: {raw}\n\n"
                last_send = time.monotonic()
            except (json.JSONDecodeError, TypeError):
                logger.warning("sse.subscribe bad message: %s", raw[:200])
                continue

    except asyncio.CancelledError:
        logger.debug("sse.subscribe cancelled channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
    except (RedisConnectionError, RedisError):
        logger.warning("sse.subscribe Redis connection lost channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
    except Exception:
        logger.exception("sse.subscribe unexpected error channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
    finally:
        try:
            await pubsub.unsubscribe(*channels)
        except Exception:
            logger.debug("sse.cleanup unsubscribe failed channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
        try:
            await pubsub.aclose()
        except Exception:
            logger.debug("sse.cleanup aclose failed channels=%s", channels) # codeql[py/clear-text-logging-sensitive-data]
        try:
            await pubsub.reset()
        except Exception:
            pass
