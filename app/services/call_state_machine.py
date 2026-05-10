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
from datetime import datetime
from typing import Any

from bson import ObjectId

from app.core.config import settings
from app.core.database import get_db
from app.core.redis import get_redis
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
_DISCONNECT_KEY_PREFIX = "call:disconnected:"


def _disconnect_key(appointment_id: str) -> str:
    return f"{_DISCONNECT_KEY_PREFIX}{appointment_id}"


# Redis keys for heartbeat-based waiting room presence
_HEARTBEAT_KEY_PREFIX = "call:heartbeat:"
_HEARTBEAT_DOCTOR_PREFIX = "call:heartbeat:doctor:"


def _heartbeat_key(appointment_id: str) -> str:
    return f"{_HEARTBEAT_KEY_PREFIX}{appointment_id}"


def _heartbeat_doctor_key(doctor_id: str) -> str:
    return f"{_HEARTBEAT_DOCTOR_PREFIX}{doctor_id}"


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

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        logger.warning("handle_participant_joined: invalid appointment_id=%s", appointment_id)
        return None

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        logger.warning("handle_participant_joined: appointment not found id=%s", appointment_id)
        return None

    if appt.get("call_status") == "ended":
        return None

    current_state = appt.get("call_status", "idle")

    # Status drift check
    if appt.get("status") != "confirmed":
        logger.warning(
            "handle_participant_joined: status drift detected (status=%s). Ending call appointment_id=%s",
            appt.get("status"), appointment_id,
        )
        await db.appointments.update_one(
            {"_id": appt_oid},
            {"$set": {"call_status": "ended", "updated_at": now}}
        )
        await _emit_state_change(appt, "ended", 0, [], now)
        return None

    patient_p = appt.get("patient_participant")
    doctor_p = appt.get("doctor_participant")

    if role == "patient" and patient_p:
        if patient_p.get("sid") == participant_sid:
            return None
        logger.warning("Duplicate patient connection detected appointment_id=%s identity=%s", appointment_id, identity)
    if role == "doctor" and doctor_p:
        if doctor_p.get("sid") == participant_sid:
            return None
        logger.warning("Duplicate doctor connection detected appointment_id=%s identity=%s", appointment_id, identity)

    participant_obj = {
        "identity": identity,
        "role": role,
        "sid": participant_sid,
        "joined_at": now,
    }

    # First: atomically set the participant
    await db.appointments.update_one(
        {"_id": appt_oid},
        {"$set": {
            f"{role}_participant": participant_obj,
            "updated_at": now
        }}
    )

    # Second: fetch the updated document to derive truth
    updated = await db.appointments.find_one({"_id": appt_oid})
    if not updated:
        return None

    patient_p = updated.get("patient_participant")
    doctor_p = updated.get("doctor_participant")

    correct_state = "connected" if (patient_p and doctor_p) else "waiting" if (patient_p or doctor_p) else "idle"
    count = int(bool(patient_p)) + int(bool(doctor_p))

    state_update: dict[str, Any] = {}
    
    if updated.get("call_status") != correct_state:
        state_update["call_status"] = correct_state
        state_update["call_participant_count"] = count
        
        # Set call_connected_at on first connection
        if correct_state == "connected" and not updated.get("call_connected_at"):
            state_update["call_connected_at"] = now
            
        # Clear disconnect timeout if reconnecting
        if current_state == "disconnected" and correct_state in ("waiting", "connected"):
            state_update["call_disconnected_at"] = None
            redis = get_redis()
            await redis.delete(_disconnect_key(appointment_id))

        await db.appointments.update_one(
            {"_id": appt_oid},
            {"$set": state_update}
        )

    logger.info(
        "call_state_machine: %s → %s appointment_id=%s identity=%s participants=%d",
        current_state, correct_state, appointment_id, identity, count,
    )

    # Construct array for SSE only
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

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        logger.warning("handle_participant_left: invalid appointment_id=%s", appointment_id)
        return None

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        logger.warning("handle_participant_left: appointment not found id=%s", appointment_id)
        return None

    current_state = appt.get("call_status", "idle")
    if current_state == "ended":
        return None

    patient_p = appt.get("patient_participant")
    doctor_p = appt.get("doctor_participant")

    # Detect role — validate identity AND SID together to reject stale webhooks.
    # A stale participant_left (from an old connection replaced by reconnect)
    # would match identity but carry the old SID. Without this check, it would
    # clear the current active participant.
    role = None
    if patient_p and patient_p.get("identity") == identity:
        if not patient_p or patient_p.get("sid") != participant_sid:
            logger.info(
                "Ignoring stale participant_left (patient SID mismatch)",
                extra={
                    "appointment_id": appointment_id,
                    "identity": identity,
                    "stored_sid": patient_p.get("sid") if patient_p else None,
                    "incoming_sid": participant_sid,
                },
            )
            return None
        role = "patient"
    elif doctor_p and doctor_p.get("identity") == identity:
        if not doctor_p or doctor_p.get("sid") != participant_sid:
            logger.info(
                "Ignoring stale participant_left (doctor SID mismatch)",
                extra={
                    "appointment_id": appointment_id,
                    "identity": identity,
                    "stored_sid": doctor_p.get("sid") if doctor_p else None,
                    "incoming_sid": participant_sid,
                },
            )
            return None
        role = "doctor"

    if not role:
        logger.warning("handle_participant_left: ignoring leave for unmatched identity=%s", identity)
        return None

    # First: atomically remove the participant
    await db.appointments.update_one(
        {"_id": appt_oid},
        {"$set": {
            f"{role}_participant": None,
            "updated_at": now
        }}
    )

    # Second: fetch the updated document to derive truth
    updated = await db.appointments.find_one({"_id": appt_oid})
    if not updated:
        return None

    patient_p = updated.get("patient_participant")
    doctor_p = updated.get("doctor_participant")

    count = int(bool(patient_p)) + int(bool(doctor_p))

    if count == 0:
        if current_state == "connected":
            correct_state = "disconnected"
        else:
            correct_state = "idle"
    elif count == 1:
        if current_state == "connected":
            correct_state = "disconnected"
        else:
            correct_state = "waiting"
    else:
        correct_state = "connected"

    state_update: dict[str, Any] = {}
    if updated.get("call_status") != correct_state:
        state_update["call_status"] = correct_state
        state_update["call_participant_count"] = count

        if correct_state == "disconnected":
            state_update["call_disconnected_at"] = now
            # Start disconnect timeout in Redis
            redis = get_redis()
            await redis.setex(
                _disconnect_key(appointment_id),
                settings.CALL_DISCONNECT_TIMEOUT_SECONDS,
                "1",
            )

        await db.appointments.update_one(
            {"_id": appt_oid},
            {"$set": state_update}
        )

    logger.info(
        "call_state_machine: %s → %s appointment_id=%s identity=%s participants=%d",
        current_state, correct_state, appointment_id, identity, count,
    )

    participants = []
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
        "patient_participant": None,
        "doctor_participant": None,
        "call_participant_count": 0,
        "call_ended_at": now,
        "updated_at": now,
    }

    result = await db.appointments.update_one(
        {"_id": appt["_id"], "call_status": {"$nin": ["ended", "idle"]}},
        {"$set": update_set},
    )

    if result.modified_count == 0:
        return

    # Clear any disconnect timeout
    redis = get_redis()
    await redis.delete(_disconnect_key(appointment_id))

    logger.info(
        "call_state_machine: %s → ended (room_finished) appointment_id=%s room=%s",
        current_state, appointment_id, room_name,
    )

    await _emit_state_change(appt, "ended", 0, [], now)


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
            "call_status": "ended",
        }

    update_set: dict[str, Any] = {
        "call_status": "ended",
        "patient_participant": None,
        "doctor_participant": None,
        "call_participant_count": 0,
        "call_ended_at": now,
        "updated_at": now,
    }

    await db.appointments.update_one(
        {"_id": appt_oid, "doctor_id": ObjectId(doctor_id)},
        {"$set": update_set},
    )

    # Clear Redis keys
    redis = get_redis()
    await redis.delete(_disconnect_key(appointment_id))
    await _clear_heartbeat(redis, appointment_id, str(appt.get("doctor_id")))

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

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        return

    result = await db.appointments.update_one(
        {"_id": appt_oid, "call_status": "disconnected"},
        {
            "$set": {
                "call_status": "ended",
                "patient_participant": None,
                "doctor_participant": None,
                "call_participant_count": 0,
                "call_ended_at": now,
                "updated_at": now,
            }
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
    pipe.expire(_heartbeat_doctor_key(doctor_id), ttl)
    await pipe.execute()


async def remove_heartbeat(appointment_id: str, doctor_id: str) -> None:
    """Remove heartbeat when patient leaves waiting room or joins call."""
    redis = get_redis()
    await _clear_heartbeat(redis, appointment_id, doctor_id)


async def get_waiting_patients(doctor_id: str) -> list[dict[str, Any]]:
    """Get patients in the waiting room (heartbeat-based pre-call presence)."""
    import json

    redis = get_redis()
    db = get_db()

    # Get appointment IDs from doctor's heartbeat set
    appt_ids = await redis.smembers(_heartbeat_doctor_key(doctor_id))
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
            "video_enabled": True,
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
        hb_raw = await redis.get(_heartbeat_key(str(doc["_id"])))
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
