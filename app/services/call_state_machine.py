"""
Call State Machine — single source of truth for video call lifecycle.

States:
  idle         → No participants in the room yet.
  waiting      → Exactly 1 participant connected (patient OR doctor).
  connected    → 2+ participants connected.
  disconnected → Was connected, now <2 participants. Auto-ends after timeout.
  ended        → Call terminated (manual or timeout).

State transitions are driven ONLY by:
  1. LiveKit webhooks (participant_joined / participant_left / room_finished)
  2. Manual end-call action (doctor clicks "End Call")
  3. Celery timeout task (disconnected → ended after CALL_DISCONNECT_TIMEOUT_SECONDS)

Token generation does NOT change state — only actual room presence does.

All transitions are idempotent: duplicate/out-of-order webhooks are safe.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from app.core.config import settings
from app.core.database import get_db
from app.core.redis import get_redis
from app.utils.redis_utils import SafeRedis, RedisKeys
from app.services.event_bus import (
    notify_appointment,
    notify_doctor,
    notify_patient,
)
from app.services.cache_service import invalidate_doctor_cache
from app.utils.time import ensure_utc, utc_now

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────

CALL_STATES = ("idle", "waiting", "connected", "disconnected", "ended")

EVENT_CALL_STATE_CHANGED = "call_state_changed"

# Redis keys for disconnect timeout tracking
def _disconnect_key(appointment_id: str) -> str:
    return RedisKeys.disconnect_key(appointment_id)


# Redis keys for heartbeat-based waiting room presence
def _heartbeat_key(appointment_id: str) -> str:
    return RedisKeys.heartbeat_key(appointment_id)


def _heartbeat_doctor_key(doctor_id: str) -> str:
    return RedisKeys.heartbeat_doctor_key(doctor_id)


# ─── Allowed transitions ─────────────────────────────────────
# Map of (current_state, event) → new_state
# The actual new state also depends on participant count; this map
# defines which transitions are valid so we can reject invalid ones.

_VALID_TRANSITIONS: dict[tuple[str, str], set[str]] = {
    # participant_joined events
    ("idle", "participant_joined"): {"waiting", "connected"},
    ("waiting", "participant_joined"): {"connected", "waiting"},
    ("disconnected", "participant_joined"): {"waiting", "connected"},
    # participant_left events
    ("waiting", "participant_left"): {"idle", "waiting"},
    ("connected", "participant_left"): {"waiting", "disconnected"},
    # room_finished events
    ("idle", "room_finished"): {"idle"},
    ("waiting", "room_finished"): {"ended"},
    ("connected", "room_finished"): {"ended"},
    ("disconnected", "room_finished"): {"ended"},
    # manual end
    ("idle", "manual_end"): {"ended"},
    ("waiting", "manual_end"): {"ended"},
    ("connected", "manual_end"): {"ended"},
    ("disconnected", "manual_end"): {"ended"},
    # timeout
    ("disconnected", "timeout"): {"ended"},
    # initializing state transitions (token generated, waiting for join webhooks)
    ("initializing", "participant_joined"): {"waiting", "connected"},
    ("initializing", "participant_left"): {"idle", "waiting"},
    ("initializing", "room_finished"): {"ended"},
    ("initializing", "manual_end"): {"ended"},
}


def _derive_state_from_count(
    current_state: str,
    event: str,
    participant_count: int,
) -> str | None:
    """Derive the new state based on participant count and event type."""
    if event == "manual_end" or event == "timeout" or event == "room_finished":
        if event == "room_finished" and current_state == "idle":
            return "idle"
        return "ended"

    if event == "participant_joined":
        if participant_count == 0:
            return "idle"
        if participant_count == 1:
            return "waiting"
        return "connected"  # >= 2

    if event == "participant_left":
        if participant_count == 0:
            if current_state == "connected":
                return "disconnected"
            return "idle"
        if participant_count == 1:
            if current_state == "connected":
                return "disconnected"
            return "waiting"
        return "connected"  # still >= 2

    return None


def _is_valid_transition(current: str, event: str, new: str) -> bool:
    """Check if the transition is allowed."""
    key = (current, event)
    allowed = _VALID_TRANSITIONS.get(key)
    if allowed is None:
        return False
    return new in allowed


# ─── Core transition function ─────────────────────────────────

async def handle_participant_joined(
    appointment_id: str,
    identity: str,
    role: str,
    participant_sid: str,
) -> dict[str, Any] | None:
    """Handle a LiveKit participant_joined webhook.

    Returns the updated appointment dict if a state transition occurred, None otherwise.
    """
    db = get_db()
    now = utc_now()
    from pymongo import ReturnDocument

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception: # codeql[py/clear-text-logging-sensitive-data]
        logger.warning("handle_participant_joined: invalid appointment_id=%s", appointment_id)
        return None

    appt = await db.appointments.find_one({"_id": appt_oid})
    if appt:
        existing_participant = appt.get(f"{role}_participant")
        if existing_participant and existing_participant.get("sid") == participant_sid:
            logger.info("Duplicate participant_joined webhook, skipping")
            return appt

    participant_obj = {
        "identity": identity, # codeql[py/clear-text-logging-sensitive-data]
        "role": role,
        "sid": participant_sid,
        "joined_at": now,
        "last_seen": now,
        "connected": True,
    }

    # Atomically set the participant and return the updated document
    updated = await db.appointments.find_one_and_update(
        {
            "_id": appt_oid,
            "call_status": {"$ne": "ended"},
            "status": "confirmed"
        }, # codeql[py/clear-text-logging-sensitive-data]
        {
            "$set": {
                f"{role}_participant": participant_obj,
                "updated_at": now,
                "call_last_activity_at": now
            }
        },
        return_document=ReturnDocument.AFTER
    )
    
    if not updated:
        logger.warning("handle_participant_joined: appointment not found, ended, or not confirmed id=%s", appointment_id)
        return None

    current_state = updated.get("call_status", "idle") # codeql[py/clear-text-logging-sensitive-data]
    patient_p = updated.get("patient_participant")
    doctor_p = updated.get("doctor_participant")
    count = int(bool(patient_p)) + int(bool(doctor_p))
     # codeql[py/clear-text-logging-sensitive-data]
    # Deriving state from DB purely (No LiveKit API polling in hot path)
    correct_state = _derive_state_from_count(current_state, "participant_joined", count)
    if not correct_state or not _is_valid_transition(current_state, "participant_joined", correct_state):
        correct_state = "connected" if count >= 2 else "waiting" if count == 1 else "idle"

    state_update: dict[str, Any] = {}
    if current_state != correct_state:
        state_update["call_status"] = correct_state
        state_update["call_participant_count"] = count
        if correct_state in ("idle", "ended"):
            state_update["session_locked"] = False
        
        # Set call_connected_at on first connection
        if correct_state == "connected" and not updated.get("call_connected_at"):
            state_update["call_connected_at"] = now
            
        # Clear disconnect timeout if reconnecting
        if current_state == "disconnected" and correct_state in ("waiting", "connected"):
            state_update["call_disconnected_at"] = None
            redis = get_redis()
            await redis.delete(_disconnect_key(appointment_id))

        redis = get_redis()
        if correct_state in ("waiting", "connected"):
            await transition_call_redis_state(redis, appointment_id, "active")

        await db.appointments.update_one({"_id": appt_oid}, {"$set": state_update})

    logger.info(
        "call_state_machine: %s → %s appointment_id=%s identity=%s participants=%d",
        current_state, correct_state, appointment_id, identity, count,
    )

    participants = []
    if role == "patient":
        participants.append(participant_obj)
        if doctor_p: participants.append(doctor_p)
    else:
        if patient_p: participants.append(patient_p)
        participants.append(participant_obj)

    final_appt = {**updated, **state_update}
    await _emit_state_change(final_appt, correct_state, count, participants, now)
    return final_appt


async def handle_participant_left(
    appointment_id: str,
    identity: str,
    participant_sid: str,
) -> dict[str, Any] | None:
    """Handle a LiveKit participant_left webhook."""
    db = get_db()
    now = utc_now()
    from pymongo import ReturnDocument

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        logger.warning("handle_participant_left: invalid appointment_id=%s", appointment_id)
        return None

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt or appt.get("call_status") == "ended":
        return None

    patient_p = appt.get("patient_participant")
    doctor_p = appt.get("doctor_participant")
    role = None # codeql[py/clear-text-logging-sensitive-data]
    
    if patient_p and patient_p.get("identity") == identity:
        stored_sid = patient_p.get("sid")
        if stored_sid != participant_sid:
            logger.warning(
                "participant_left for stale SID: stored=%s received=%s role=patient identity=%s",
                stored_sid, participant_sid, identity
            )
            return None
        role = "patient"
    elif doctor_p and doctor_p.get("identity") == identity:
        stored_sid = doctor_p.get("sid")
        if stored_sid != participant_sid:
            logger.warning(
                "participant_left for stale SID: stored=%s received=%s role=doctor identity=%s",
                stored_sid, participant_sid, identity
            )
            return None
        role = "doctor"
    
    if not role:
        logger.info("Ignoring stale participant_left for identity=%s sid=%s", identity, participant_sid)
        return None

    # Atomically remove the participant
    updated = await db.appointments.find_one_and_update(
        {"_id": appt_oid},
        {"$set": {
            f"{role}_participant": None,
            "updated_at": now,
            "call_last_activity_at": now
        }},
        return_document=ReturnDocument.AFTER
    )
    if not updated:
        return None

    current_state = updated.get("call_status", "idle")
    patient_p = updated.get("patient_participant")
    doctor_p = updated.get("doctor_participant")
    count = int(bool(patient_p)) + int(bool(doctor_p))
    
    # DB count is truth # codeql[py/clear-text-logging-sensitive-data]
    correct_state = _derive_state_from_count(current_state, "participant_left", count)
    if not correct_state or not _is_valid_transition(current_state, "participant_left", correct_state):
        if count == 0:
            correct_state = "disconnected" if current_state == "connected" else "idle"
        elif count == 1: # codeql[py/clear-text-logging-sensitive-data]
            correct_state = "disconnected" if current_state == "connected" else "waiting"
        else:
            correct_state = "connected"

    state_update: dict[str, Any] = {}
    if current_state != correct_state:
        state_update["call_status"] = correct_state
        state_update["call_participant_count"] = count
        if correct_state in ("idle", "ended"):
            state_update["session_locked"] = False

        if correct_state == "disconnected":
            state_update["call_disconnected_at"] = now
            redis = get_redis()
            await redis.setex(
                _disconnect_key(appointment_id),
                settings.CALL_DISCONNECT_TIMEOUT_SECONDS,
                "1",
            )
            # Schedule Celery task for timeout enforcement
            from app.worker.tasks.appointment_tasks import enforce_disconnect_timeout # codeql[py/clear-text-logging-sensitive-data]
            enforce_disconnect_timeout.apply_async(
                args=[appointment_id],
                countdown=settings.CALL_DISCONNECT_TIMEOUT_SECONDS
            )

        redis = get_redis()
        if correct_state == "disconnected":
            await transition_call_redis_state(redis, appointment_id, "ending")

        await db.appointments.update_one({"_id": appt_oid}, {"$set": state_update})

    logger.info(
        "call_state_machine: %s → %s appointment_id=%s identity=%s participants=%d",
        current_state, correct_state, appointment_id, identity, count,
    )

    participants = [] # codeql[py/clear-text-logging-sensitive-data]
    if patient_p: participants.append(patient_p)
    if doctor_p: participants.append(doctor_p)

    final_appt = {**updated, **state_update}
    await _emit_state_change(final_appt, correct_state, count, participants, now)
    return final_appt


async def handle_room_finished(room_name: str) -> None:
    """Handle a LiveKit room_finished webhook — all participants gone, room closed."""
    db = get_db()
    now = utc_now()

    appt = await db.appointments.find_one({"video_room": room_name})
    if not appt:
        logger.debug("handle_room_finished: no appointment for room=%s", room_name)
        return

    current_state = appt.get("call_status", "idle")
    if current_state in ("ended", "idle"):
        return

    appointment_id = str(appt["_id"])

    update_set: dict[str, Any] = {
        "call_status": "ended",
        "session_locked": False,
        "patient_participant": None,
        "doctor_participant": None,
        "call_participant_count": 0,
        "call_ended_at": now,
        "updated_at": now,
    }

    result = await db.appointments.update_one(
        {"_id": appt["_id"], "call_status": {"$nin": ["ended", "idle"]}},
        {
            "$set": update_set,
            "$inc": {"session_version": 1}
        },
    )

    if result.modified_count == 0:
        return

    # Clear all video call coordination Redis keys
    redis = get_redis()
    keys = get_call_coordination_keys(appointment_id)
    try:
        await redis.delete(*keys)
    except Exception as e:
        logger.warning("handle_room_finished: failed to delete Redis keys: %s", str(e))

    doctor_id = str(appt.get("doctor_id")) if appt.get("doctor_id") else None
    if doctor_id:
        await _clear_heartbeat(redis, appointment_id, doctor_id)

    await transition_call_redis_state(redis, appointment_id, "ended")

    logger.info(
        "call_state_machine: %s → ended (room_finished) appointment_id=%s room=%s",
        current_state, appointment_id, room_name,
    )

    await _emit_state_change(appt, "ended", 0, [], now)


async def reconcile_call_state(appointment_id: str) -> dict[str, Any] | None:
    """Reconcile the call state in DB with the actual state in LiveKit."""
    from livekit import api
    db = get_db()
    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        return None

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt or appt.get("call_status") == "ended":
        return appt

    # Cooldown check to prevent spamming LiveKit API
    redis = SafeRedis(get_redis())
    room_name = appt.get("video_room")
    if not room_name or not settings.VIDEO_ENABLED:
        return appt

    cooldown_key = RedisKeys.reconcile_cooldown(appointment_id)
    if await redis.get(cooldown_key):
        logger.info("reconcile_call_state: skipping due to cooldown for appointment_id=%s", appointment_id)
        return appt

    room_name = appt.get("video_room")
    if not room_name or not settings.VIDEO_ENABLED:
        return appt

    try:
        async with api.LiveKitAPI(
            settings.LIVEKIT_URL,
            settings.LIVEKIT_API_KEY,
            settings.LIVEKIT_API_SECRET.get_secret_value()
        ) as lkapi:
            # Fetch participants in LiveKit room
            res = await lkapi.room.list_participants(api.ListParticipantsRequest(room=room_name))
            lk_participants = res.participants
            # Set reconciliation cooldown
            await redis.setex(cooldown_key, 10, "1")
    except Exception as e:
        logger.warning("reconcile_call_state: failed to list participants for room %s: %s", room_name, str(e))
        return appt

    # Check if there is a mismatch
    lk_identities = {p.identity for p in lk_participants}
    
    patient_p = appt.get("patient_participant")
    doctor_p = appt.get("doctor_participant")
    
    db_identities = set()
    if patient_p:
        db_identities.add(patient_p.get("identity"))
    if doctor_p:
        db_identities.add(doctor_p.get("identity"))

    current_state = appt.get("call_status", "idle")

    # Handle stale "initializing" state: a token was generated (setting
    # call_status="initializing" and session_locked=True) but the participant
    # never actually joined the LiveKit room (e.g. user pressed back, page
    # refreshed). Without this check the lock is held forever, blocking
    # all subsequent join attempts.
    if current_state == "initializing" and len(lk_participants) == 0:
        now = utc_now()
        updated_at = ensure_utc(appt.get("updated_at") or now)
        if (now - updated_at).total_seconds() > 30:
            stale_update = {
                "call_status": "idle",
                "session_locked": False,
                "patient_participant": None,
                "doctor_participant": None,
                "call_participant_count": 0,
                "updated_at": now,
            }
            await db.appointments.update_one({"_id": appt_oid}, {"$set": stale_update})
            logger.info(
                "reconcile_call_state: cleared stale 'initializing' state for appointment_id=%s",
                appointment_id,
            )
            return {**appt, **stale_update}

    if lk_identities != db_identities:
        logger.info(
            "reconcile_call_state: mismatch detected for appointment_id=%s. DB=%s, LiveKit=%s",
            appointment_id, db_identities, lk_identities
        )
        
        # Build corrected participant objects
        new_patient_p = None
        new_doctor_p = None
        
        # Sort by joined_at ascending so that in case of duplicates, the latest participant wins
        sorted_participants = sorted(lk_participants, key=lambda x: x.joined_at or 0)
        for p in sorted_participants:
            role = None
            if p.identity.startswith("doctor:"):
                role = "doctor"
            elif p.identity.startswith("patient:") or p.identity.startswith("public:"):
                role = "patient"
                
            p_obj = {
                "identity": p.identity,
                "role": role or "patient",
                "sid": p.sid,
                "joined_at": datetime.fromtimestamp(p.joined_at, tz=timezone.utc) if p.joined_at else utc_now(),
                "last_seen": utc_now(),
                "connected": True,
            }
            if role == "doctor":
                new_doctor_p = p_obj
            else:
                new_patient_p = p_obj
                
        now = utc_now()
        count = int(bool(new_patient_p)) + int(bool(new_doctor_p))
        
        correct_state = "connected" if count >= 2 else "waiting" if count == 1 else "idle"
        
        state_update = {
            "patient_participant": new_patient_p,
            "doctor_participant": new_doctor_p,
            "call_participant_count": count,
            "updated_at": now,
            "call_last_activity_at": now,
        }
        
        if current_state != correct_state:
            state_update["call_status"] = correct_state
            if correct_state in ("idle", "ended"):
                state_update["session_locked"] = False
            if correct_state == "connected" and not appt.get("call_connected_at"):
                state_update["call_connected_at"] = now
            if current_state == "disconnected" and correct_state in ("waiting", "connected"):
                state_update["call_disconnected_at"] = None
                redis = get_redis()
                await redis.delete(_disconnect_key(appointment_id))
                
        logger.info(
            "reconcile_call_state: corrected state for appointment_id=%s. Old state=%s, new state=%s, count=%d",
            appointment_id, current_state, correct_state, count
        )
        try:
            redis = get_redis()
            await redis.incr("call:reconcile:corrections_count")
        except Exception as e:
            logger.warning("failed to increment reconcile corrections counter: %s", str(e))
        await db.appointments.update_one({"_id": appt_oid}, {"$set": state_update})
        
        # Emit event to notify SSE clients of state change
        updated_appt = {**appt, **state_update}
        participants = []
        if new_patient_p: participants.append(new_patient_p)
        if new_doctor_p: participants.append(new_doctor_p)
        await _emit_state_change(updated_appt, correct_state, count, participants, now)
        return updated_appt

    return appt


async def handle_manual_end(appointment_id: str, doctor_id: str) -> dict[str, Any]:
    """Doctor manually ends the call."""
    db = get_db()
    now = utc_now()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise ValueError("Invalid appointment_id")

    appt = await db.appointments.find_one({"_id": appt_oid, "doctor_id": ObjectId(doctor_id)})
    if not appt:
        raise ValueError("Appointment not found")

    current_state = appt.get("call_status", "idle")
    if current_state == "ended":
        return {
            "message": "already_ended",
            "appointment_id": appointment_id,
            "call_status": "ended", # codeql[py/clear-text-logging-sensitive-data]
        }

    update_set: dict[str, Any] = {
        "call_status": "ended",
        "session_locked": False,
        "patient_participant": None,
        "doctor_participant": None,
        "call_participant_count": 0,
        "call_ended_at": now,
        "updated_at": now,
    }

    await db.appointments.update_one(
        {"_id": appt_oid, "doctor_id": ObjectId(doctor_id)},
        {
            "$set": update_set,
            "$inc": {"session_version": 1}
        },
    )

    await cleanup_call_resources(appointment_id, str(appt.get("doctor_id")), appt.get("video_room"))

    logger.info(
        "call_state_machine: %s → ended (manual) appointment_id=%s",
        current_state, appointment_id,
    )

    await _emit_state_change(appt, "ended", 0, [], now)

    # Invalidate caches
    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(doctor_id, day=scheduled_at.date().isoformat())

    return {
        "message": "call_ended",
        "appointment_id": appointment_id,
        "call_status": "ended",
        "call_ended_at": now.isoformat(),
    }


async def handle_disconnect_timeout(appointment_id: str) -> None:
    """Called by Celery when disconnect timeout expires."""
    db = get_db()
    now = utc_now()
 # codeql[py/clear-text-logging-sensitive-data]
    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        return

    result = await db.appointments.update_one(
        {"_id": appt_oid, "call_status": "disconnected"},
        {
            "$set": {
                "call_status": "ended",
                "session_locked": False,
                "patient_participant": None,
                "doctor_participant": None,
                "call_participant_count": 0,
                "call_ended_at": now,
                "updated_at": now,
            },
            "$inc": {"session_version": 1}
        },
    )

    if result.modified_count == 0:
        return  # not disconnected anymore (reconnected or already ended)

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        return

    logger.info(
        "call_state_machine: disconnected → ended (timeout) appointment_id=%s",
        appointment_id,
    )

    await _emit_state_change(appt, "ended", 0, [], now)

def get_call_coordination_keys(appointment_id: str) -> list[str]:
    return [
        _disconnect_key(appointment_id),
        RedisKeys.join_lock(appointment_id),
        RedisKeys.call_version(appointment_id),
        RedisKeys.epoch_key(appointment_id, "doctor"),
        RedisKeys.epoch_key(appointment_id, "patient"),
        RedisKeys.call_leader(appointment_id, "doctor"),
        RedisKeys.call_leader(appointment_id, "patient"),
        RedisKeys.active_token(appointment_id, "doctor"),
        RedisKeys.active_token(appointment_id, "patient"),
        RedisKeys.prev_token(appointment_id, "doctor"),
        RedisKeys.prev_token(appointment_id, "patient"),
        RedisKeys.call_state(appointment_id),
        RedisKeys.call_created_at(appointment_id),
        RedisKeys.reconcile_cooldown(appointment_id),
        RedisKeys.kill_switch(appointment_id),
        RedisKeys.last_seen_key(appointment_id, "doctor"),
        RedisKeys.last_seen_key(appointment_id, "patient"),
        RedisKeys.last_ts_key(appointment_id, "doctor"),
        RedisKeys.last_ts_key(appointment_id, "patient"),
        f"{RedisKeys.join_lock(appointment_id)}:counter"
    ]

async def cleanup_call_resources(appointment_id: str, doctor_id: str | None, room_name: str | None) -> None:
    """Helper to clean up LiveKit room and Redis keys."""
    redis = get_redis()
    keys = get_call_coordination_keys(appointment_id)
    try:
        await redis.delete(*keys)
    except Exception as e:
        logger.warning("cleanup_call_resources: failed to delete some Redis keys: %s", str(e))

    if doctor_id:
        await _clear_heartbeat(redis, appointment_id, doctor_id)
    
    if room_name:
        from livekit import api
        try:
            async with api.LiveKitAPI(settings.LIVEKIT_URL, settings.LIVEKIT_API_KEY, settings.LIVEKIT_API_SECRET.get_secret_value()) as lkapi:
                await lkapi.room.delete_room(api.DeleteRoomRequest(room=room_name))
        except Exception as e:
            logger.warning("cleanup_call_resources: failed to delete LiveKit room %s: %s", room_name, str(e))


# ─── Heartbeat-based waiting room presence ────────────────────

async def record_heartbeat(appointment_id: str, doctor_id: str, patient_name: str) -> None:
    """Record a heartbeat from a patient in the waiting room (pre-call)."""
    redis = get_redis()
    import json

    ttl = settings.CALL_HEARTBEAT_TTL_SECONDS
    meta = json.dumps({
        "appointment_id": appointment_id,
        "patient_name": patient_name,
        "since": utc_now().isoformat(),
    })

    pipe = redis.pipeline(transaction=False)
    pipe.setex(_heartbeat_key(appointment_id), ttl, meta)
    pipe.sadd(_heartbeat_doctor_key(doctor_id), appointment_id)
    pipe.expire(_heartbeat_doctor_key(doctor_id), ttl) # codeql[py/clear-text-logging-sensitive-data]
    await pipe.execute()


async def remove_heartbeat(appointment_id: str, doctor_id: str) -> None:
    """Remove heartbeat when patient leaves waiting room or joins call."""
    redis = get_redis()
    await _clear_heartbeat(redis, appointment_id, doctor_id)


async def get_waiting_patients(doctor_id: str) -> list[dict[str, Any]]:
    """Get patients in the waiting room (heartbeat-based pre-call presence)."""
    import json

    redis = get_redis()
    safe_redis = SafeRedis(redis)
    db = get_db()

    # Get appointment IDs from doctor's heartbeat set
    appt_ids = await safe_redis.smembers_str(_heartbeat_doctor_key(doctor_id))
    if not appt_ids:
        return []

    # Check which heartbeats are still alive
    alive_ids: list[str] = []
    stale_ids: list[str] = []

    for appt_id in appt_ids:
        exists = await redis.exists(_heartbeat_key(appt_id))
        if exists:
            alive_ids.append(appt_id)
        else:
            stale_ids.append(appt_id)

    # Clean stale entries
    if stale_ids:
        pipe = redis.pipeline(transaction=False)
        for sid in stale_ids:
            pipe.srem(_heartbeat_doctor_key(doctor_id), sid)
        await pipe.execute()

    if not alive_ids:
        return []

    # Fetch appointment details for alive heartbeats
    valid_oids = []
    for aid in alive_ids:
        try:
            valid_oids.append(ObjectId(aid))
        except Exception:
            pass

    if not valid_oids:
        return []

    docs = await db.appointments.find(
        {
            "_id": {"$in": valid_oids},
            "doctor_id": ObjectId(doctor_id),
            "mode": "online",
            "video_enabled": True, # codeql[py/clear-text-logging-sensitive-data]
            "status": {"$in": ["confirmed", "pending_payment"]},
            "call_status": {"$in": ["idle", "waiting"]},
        },
        {
            "patient_name": 1,
            "scheduled_at": 1,
            "patient_joined_at": 1,
            "call_status": 1,
        },
    ).to_list(length=50)

    results = []
    for doc in docs:
        hb_raw = await safe_redis.get_str(_heartbeat_key(str(doc["_id"])))
        meta = {}
        if hb_raw:
            try:
                meta = json.loads(hb_raw)
            except Exception:
                pass

        scheduled_at = ensure_utc(doc.get("scheduled_at"))
        results.append({
            "appointment_id": str(doc["_id"]),
            "patient_name": meta.get("patient_name") or doc.get("patient_name"),
            "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
            "waiting_since": meta.get("since")
            or (doc["patient_joined_at"].isoformat() if doc.get("patient_joined_at") else None),
            "call_status": doc.get("call_status", "idle"),
        })

    results.sort(key=lambda r: r.get("scheduled_at") or "")
    return results


async def _clear_heartbeat(redis, appointment_id: str, doctor_id: str) -> None:
    """Remove heartbeat keys for an appointment."""
    pipe = redis.pipeline(transaction=False)
    pipe.delete(_heartbeat_key(appointment_id))
    pipe.srem(_heartbeat_doctor_key(doctor_id), appointment_id)
    await pipe.execute()


# ─── Dashboard query ──────────────────────────────────────────

async def get_calls_dashboard(doctor_id: str, day: str) -> dict[str, Any]:
    """Single endpoint for the doctor's calls dashboard.

    Returns all call-related data in one response:
      - waiting: patients in pre-call waiting room (heartbeat) + call_status="waiting"
      - active: appointments with call_status="connected"
      - disconnected: appointments with call_status="disconnected"
      - scheduled: today's confirmed video appointments still in "idle"
    """
    from app.utils.time import day_window_to_utc

    db = get_db()
    doctor_oid = ObjectId(doctor_id)

    avail = await db.doctor_availability.find_one({"doctor_id": doctor_oid})
    avail_tz = avail.get("timezone", "Asia/Kolkata") if avail else "Asia/Kolkata"

    try:
        start_utc, end_utc = day_window_to_utc(day, tz_name=avail_tz)
    except ValueError:
        start_utc, end_utc = day_window_to_utc(utc_now().date().isoformat(), tz_name=avail_tz)

    # Fetch all today's video appointments in one query
    appointments = await db.appointments.find(
        {
            "doctor_id": doctor_oid,
            "scheduled_at": {"$gte": start_utc, "$lt": end_utc},
            "mode": "online",
            "video_enabled": True,
            "status": {"$in": ["confirmed", "completed", "pending_payment"]},
        },
    ).sort("scheduled_at", 1).to_list(length=200)

    # Get heartbeat-based waiting patients
    waiting_heartbeat = await get_waiting_patients(doctor_id)
    waiting_appt_ids = {w["appointment_id"] for w in waiting_heartbeat}

    # Categorize
    waiting = []
    active = []
    disconnected = []
    scheduled = []

    for a in appointments:
        appt_id = str(a["_id"])
        call_status = a.get("call_status", "idle")
        scheduled_at = ensure_utc(a.get("scheduled_at"))

        serialized = {
            "appointment_id": appt_id,
            "patient_name": a.get("patient_name"),
            "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
            "duration_min": a.get("duration_min"),
            "call_status": call_status,
            "call_participant_count": a.get("call_participant_count", 0),
            "call_participants": [
                {"role": p.get("role"), "identity": p.get("identity")}
                for p in [a.get("patient_participant"), a.get("doctor_participant")] if p
            ],
            "call_connected_at": (
                a["call_connected_at"].isoformat() if a.get("call_connected_at") else None
            ),
            "status": a.get("status"),
            "payment_status": a.get("payment_status"),
        }

        if call_status == "connected":
            active.append(serialized)
        elif call_status == "disconnected":
            serialized["call_disconnected_at"] = (
                a["call_disconnected_at"].isoformat() if a.get("call_disconnected_at") else None
            )
            disconnected.append(serialized)
        elif call_status == "waiting" or appt_id in waiting_appt_ids:
            # Add waiting_since from heartbeat data if available
            hb_match = next((w for w in waiting_heartbeat if w["appointment_id"] == appt_id), None)
            serialized["waiting_since"] = (
                hb_match["waiting_since"] if hb_match else
                (a["patient_joined_at"].isoformat() if a.get("patient_joined_at") else None)
            )
            waiting.append(serialized)
        elif call_status == "idle" and a.get("status") in ("confirmed", "pending_payment"):
            scheduled.append(serialized)
        # "ended" appointments are not shown in any active section

    # Add heartbeat-only waiting patients (not yet in today's appointments query)
    seen = {w["appointment_id"] for w in waiting}
    for hb in waiting_heartbeat:
        if hb["appointment_id"] not in seen:
            waiting.append({
                "appointment_id": hb["appointment_id"],
                "patient_name": hb["patient_name"],
                "scheduled_at": hb["scheduled_at"],
                "waiting_since": hb["waiting_since"],
                "call_status": hb.get("call_status", "idle"),
                "call_participant_count": 0,
                "call_participants": [],
                "call_connected_at": None,
                "status": "confirmed",
                "payment_status": None,
                "duration_min": None,
            })

    return {
        "doctor_id": doctor_id,
        "day": day,
        "waiting": waiting,
        "active": active,
        "disconnected": disconnected,
        "scheduled": scheduled,
        "counts": {
            "waiting": len(waiting),
            "active": len(active),
            "disconnected": len(disconnected),
            "scheduled": len(scheduled),
        },
    }


# ─── SSE emission helper ─────────────────────────────────────

async def _emit_state_change(
    appt: dict,
    new_state: str,
    participant_count: int,
    participants: list[dict],
    now: datetime,
) -> None:
    """Emit SSE event to both doctor and patient on any state change."""
    appointment_id = str(appt["_id"])
    doctor_id = str(appt.get("doctor_id"))
    patient_user_id = appt.get("patient_user_id")

    event_data = {
        "appointment_id": appointment_id,
        "call_status": new_state,
        "call_participant_count": participant_count,
        "call_participants": [
            {"role": p.get("role"), "identity": p.get("identity")}
            for p in participants
        ],
        "patient_name": appt.get("patient_name"),
        "scheduled_at": (
            appt["scheduled_at"].isoformat() if appt.get("scheduled_at") else None
        ),
    }

    # Notify doctor
    await notify_doctor(doctor_id, EVENT_CALL_STATE_CHANGED, event_data)

    # Notify patient (both channels for public + authenticated)
    await notify_appointment(appointment_id, EVENT_CALL_STATE_CHANGED, event_data)
    if patient_user_id:
        await notify_patient(str(patient_user_id), EVENT_CALL_STATE_CHANGED, event_data)


async def handle_participant_timeout(appointment_id: str, role: str) -> dict[str, Any] | None:
    """Handle a participant timeout (heartbeat stopped)."""
    db = get_db()
    now = utc_now()
    from pymongo import ReturnDocument

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        return None

    # Check if participant is still registered and connected
    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt or appt.get("call_status") == "ended":
        return None

    participant = appt.get(f"{role}_participant")
    if not participant or not participant.get("connected"):
        return None

    logger.info("call_state_machine: participant timeout role=%s appointment_id=%s", role, appointment_id)

    # Mark participant as disconnected
    updated = await db.appointments.find_one_and_update(
        {"_id": appt_oid},
        {
            "$set": {
                f"{role}_participant.connected": False,
                "updated_at": now,
                "call_last_activity_at": now
            }
        },
        return_document=ReturnDocument.AFTER
    )
    if not updated:
        return None

    # Recompute call status
    current_state = updated.get("call_status", "idle")
    patient_p = updated.get("patient_participant")
    doctor_p = updated.get("doctor_participant")
    
    patient_alive = patient_p and patient_p.get("connected")
    doctor_alive = doctor_p and doctor_p.get("connected")
    
    count = int(bool(patient_alive)) + int(bool(doctor_alive))
    
    if patient_alive and doctor_alive:
        correct_state = "connected"
    elif patient_alive or doctor_alive:
        correct_state = "waiting"
    else:
        correct_state = "disconnected"

    state_update: dict[str, Any] = {}
    if current_state != correct_state:
        state_update["call_status"] = correct_state
        state_update["call_participant_count"] = count

        if correct_state == "disconnected":
            state_update["call_disconnected_at"] = now
            redis = get_redis()
            await redis.setex(
                _disconnect_key(appointment_id),
                settings.CALL_DISCONNECT_TIMEOUT_SECONDS,
                "1",
            )
            # Schedule Celery task for timeout enforcement
            from app.worker.tasks.appointment_tasks import enforce_disconnect_timeout
            enforce_disconnect_timeout.apply_async(
                args=[appointment_id],
                countdown=settings.CALL_DISCONNECT_TIMEOUT_SECONDS
            )

        redis = get_redis()
        if correct_state == "disconnected":
            await transition_call_redis_state(redis, appointment_id, "ending")

        await db.appointments.update_one({"_id": appt_oid}, {"$set": state_update})

    # Emit event to notify SSE clients of state change
    final_appt = {**updated, **state_update}
    participants = []
    if patient_p and patient_alive: participants.append(patient_p)
    if doctor_p and doctor_alive: participants.append(doctor_p)
    await _emit_state_change(final_appt, correct_state, count, participants, now)
    return final_appt


# ─── Final Safety and Telemetry Helpers ───────────────────────

async def transition_call_redis_state(redis, appointment_id: str, next_state: str, version: int | None = None) -> bool:
    """Validate and transition call state in Redis to ensure Mongo/Redis consistency."""
    safe_redis = SafeRedis(redis)
    key = RedisKeys.call_state(appointment_id)
    allowed_transitions = {
        "idle": {"connecting"},
        "connecting": {"active", "ended"},
        "active": {"ending", "ended"},
        "ending": {"ended"},
        "ended": set()
    }
    try:
        curr_state_str = await safe_redis.hget_str(key, "state")
        if not curr_state_str:
            curr_state_str = "idle"
            
        if curr_state_str == next_state:
            if version is not None:
                await redis.hset(key, "version", str(version))
            return True
            
        allowed = allowed_transitions.get(curr_state_str, set())
        if next_state not in allowed:
            logger.warning("Redis FSM: Invalid transition from %s to %s for appointment %s", curr_state_str, next_state, appointment_id)
            return False
            
        mapping = {"state": next_state}
        if version is not None:
            mapping["version"] = str(version)
            
        await redis.hset(key, mapping=mapping)
        await redis.expire(key, 7200) # 2 hours TTL
        logger.info("Redis FSM: Transitioned %s → %s for appointment %s", curr_state_str, next_state, appointment_id)
        return True
    except Exception as exc:
        logger.warning("Redis FSM: Error transitioning state to %s for appointment %s: %s", next_state, appointment_id, str(exc))
        return False


async def log_call_timeline(redis, appointment_id: str, event: str, session_id: str | None = None, epoch: int | None = None, strategy: str | None = None, rtt: float | None = None, packet_loss: float | None = None):
    """Log an operational event to a capped timeline list in Redis."""
    try:
        import time
        import json
        log_entry = {
            "ts": time.time(),
            "event": event,
            "session_id": session_id,
            "epoch": epoch,
            "strategy": strategy,
            "rtt": rtt,
            "packet_loss": packet_loss
        }
        log_key = f"call_log:{appointment_id}"
        await redis.lpush(log_key, json.dumps(log_entry))
        await redis.ltrim(log_key, 0, 199) # Limit to 200 entries
        await redis.expire(log_key, 7200)
    except Exception as exc:
        logger.warning("Observability: Failed to log call timeline: %s", str(exc))


async def record_metric(redis, appointment_id: str, metric_name: str, increment: int = 1):
    """Record metrics in Redis and check alert thresholds dynamically."""
    try:
        # Per-call metrics
        call_metrics_key = f"metrics:call:{appointment_id}"
        await redis.hincrby(call_metrics_key, metric_name, increment)
        await redis.expire(call_metrics_key, 7200)
        
        # System metrics
        system_metrics_key = "metrics:system"
        await redis.hincrby(system_metrics_key, metric_name, increment)
        
        # Trigger dynamic checks
        await check_and_log_alerts(redis)
    except Exception as exc:
        logger.warning("Telemetry: Failed to record metrics: %s", str(exc))


async def check_and_log_alerts(redis):
    """Analyze system metrics and emit alerts on high failure rates or storms."""
    safe_redis = SafeRedis(redis)
    try:
        system_metrics_key = "metrics:system"
        metrics_decoded = await safe_redis.hgetall_parsed(system_metrics_key)
        if metrics_decoded:
            failures = float(metrics_decoded.get("failures", 0))
            heartbeats = float(metrics_decoded.get("heartbeats", 0))
            reconnects = float(metrics_decoded.get("reconnects", 0))
            
            total = heartbeats + failures
            if total > 50:
                failure_rate = failures / total
                if failure_rate > 0.2:
                    logger.error("🚨 CALL SYSTEM UNSTABLE: HIGH_FAILURE_RATE = %.2f%%", failure_rate * 100, extra=metrics_decoded)
                    
            if reconnects > 100:
                logger.error("🚨 CALL SYSTEM UNSTABLE: RECONNECT_STORM detected", extra=metrics_decoded)
    except Exception as exc:
        logger.warning("Telemetry: Failed to parse alerts: %s", str(exc))


async def startup_reconcile_calls():
    """Perform startup boot reconciliation on Mongo calls and sync to Redis state cache."""
    logger.info("Reconciliation: Starting boot-time call state reconciliation job...")
    db = get_db()
    safe_redis = SafeRedis(get_redis())
    now = utc_now()
    
    try:
        # Scan non-ended calls in MongoDB
        active_appointments = await db.appointments.find({
            "call_status": {"$in": ["initializing", "waiting", "connected", "disconnected"]}
        }).to_list(length=None)
        
        for appt in active_appointments:
            appointment_id = str(appt["_id"])
            updated_at = ensure_utc(appt.get("updated_at") or now)
            age_seconds = (now - updated_at).total_seconds()
            
            if age_seconds < 30:
                # Rehydrate Redis status
                call_state_key = RedisKeys.call_state(appointment_id)
                redis_state = "connecting" if appt.get("call_status") == "initializing" else "active"
                version = appt.get("session_version", 0)
                try:
                    await safe_redis.hset(call_state_key, mapping={
                        "state": redis_state,
                        "version": str(version),
                        "doctor_id": str(appt.get("doctor_id")),
                        "patient_user_id": str(appt.get("patient_user_id") or "")
                    })
                    await safe_redis.expire(call_state_key, 7200)
                    logger.info("Reconciliation: Rehydrated active call Redis state for appt %s", appointment_id)
                except Exception as exc:
                    logger.warning("Reconciliation: Failed to write Redis cache for appt %s: %s", appointment_id, str(exc))
            else:
                # Force end stale calls
                room_name = appt.get("video_room")
                if room_name:
                    logger.info("Reconciliation: Terminating stale call on startup for appt %s (room %s)", appointment_id, room_name)
                    await handle_room_finished(room_name)
        logger.info("Reconciliation: Boot-time call state reconciliation finished.")
    except Exception as exc:
        logger.error("Reconciliation: Boot-time check failed: %s", str(exc))
