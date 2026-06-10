from datetime import timedelta
import logging
from zoneinfo import ZoneInfo

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.security import set_public_patient_access_cookie
from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
from app.schemas.appointment_schema import (
    PatientCancelIn,
    PatientRescheduleIn,
    PaymentCreateOrderIn,
    PaymentVerifyIn,
    PublicAppointmentAccessIn,
    PublicAppointmentView,
    normalize_call_status,
)
from app.services.payment_order_service import create_payment_order_for_appointment, verify_payment_signature_and_confirm
from app.services.refund_service import enqueue_refund_processing
from app.services.doctor_availability_service import get_candidate_slots_for_date
from app.services.email_service import (
    safe_send_email,
    send_cancellation_email,
    send_doctor_cancellation,
    send_doctor_reschedule,
    send_reschedule_confirmation,
)
from app.services.whatsapp_service import (
    safe_send_whatsapp,
    send_patient_appointment_update_whatsapp,
)
from app.services.video_service import (
    check_video_payment,
    create_video_token,
    ensure_video_room,
)
from app.utils.appointment_rules import (
    build_patient_access_expiry,
    get_patient_access_token,
    is_within_booking_window,
    is_within_cancel_window,
    validate_patient_token,
)
from app.utils.magic_token import encrypt_magic_token, generate_magic_token, hash_magic_token
from app.utils.time import ensure_utc, parse_client_datetime_to_utc, utc_now
from app.utils.video import check_join_window
from app.services.event_bus import (
    notify_doctor,
    notify_appointment,
    EVENT_PATIENT_WAITING,
    EVENT_APPOINTMENT_CANCELLED,
    EVENT_APPOINTMENT_RESCHEDULED,
)

router = APIRouter(prefix="/public", tags=["Public Appointment Actions"])
logger = logging.getLogger(__name__)


@router.post(
    "/appointments/{appointment_id}/access-session",
    dependencies=[rl(settings.RL_PUBLIC_ACTION_TIMES, settings.RL_PUBLIC_ACTION_SECONDS)],
)
async def create_public_appointment_access_session(
    appointment_id: str,
    payload: PublicAppointmentAccessIn,
    response: Response,
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appointment = await db.appointments.find_one({"_id": appt_oid})
    if not appointment:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(appointment, payload.token, now)

    expires_at = ensure_utc(appointment.get("patient_access_expires_at"))
    if not expires_at:
        raise HTTPException(status_code=403, detail="Token expired")
    max_age = max(1, int((expires_at - now).total_seconds()))
    set_public_patient_access_cookie(response, token=payload.token, max_age=max_age)

    return {
        "message": "access_session_created",
        "appointment_id": appointment_id,
        "expires_at": expires_at.isoformat(),
    }


@router.post(
    "/access-by-token",
    dependencies=[rl(settings.RL_PUBLIC_ACTION_TIMES, settings.RL_PUBLIC_ACTION_SECONDS)],
)
async def access_by_token(payload: PublicAppointmentAccessIn, response: Response):
    """Token-only magic link landing.

    Looks up the appointment by hashing the supplied magic token and matching
    against `patient_access_token_hash`, sets the public-patient access cookie,
    and returns the appointment id so the frontend can redirect.

    Used by WhatsApp template buttons where Meta URL-encodes the dynamic
    substitution — the static URL `/m/{{1}}` plus the URL-safe token works
    around that, since the token only contains [A-Za-z0-9_-].
    """
    db = get_db()
    token = (payload.token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Token required")

    token_hash = hash_magic_token(token)
    appt = await db.appointments.find_one(
        {"patient_access_token_hash": token_hash},
        {"_id": 1, "patient_access_expires_at": 1},
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Invalid or revoked link")

    now = utc_now()
    expires_at = ensure_utc(appt.get("patient_access_expires_at"))
    if not expires_at or expires_at <= now:
        raise HTTPException(status_code=403, detail="Link expired")

    max_age = max(1, int((expires_at - now).total_seconds()))
    set_public_patient_access_cookie(response, token=token, max_age=max_age)

    return {
        "message": "access_session_created",
        "appointment_id": str(appt["_id"]),
        "expires_at": expires_at.isoformat(),
    }


@router.get(
    "/appointments/{appointment_id}",
    response_model=PublicAppointmentView,
    dependencies=[rl(settings.RL_PUBLIC_APPOINTMENT_READ_TIMES, settings.RL_PUBLIC_APPOINTMENT_READ_SECONDS)],
)
async def view_public_appointment(
    appointment_id: str,
    token: str = Depends(get_patient_access_token),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appointment = await db.appointments.find_one({"_id": appt_oid})
    if not appointment:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(appointment, token, now)
    cancel_window_hours = int(getattr(settings, "CANCEL_WINDOW_HOURS", 2))
    scheduled_at = ensure_utc(appointment.get("scheduled_at"))
    status = appointment.get("status")
    action_blocked = status in ("cancelled", "completed", "no_show")
    within_cancel_window = not scheduled_at or is_within_cancel_window(
        scheduled_at,
        now,
        hours=cancel_window_hours,
    )

    return {
        "appointment_id": str(appointment["_id"]),
        "doctor_id": str(appointment["doctor_id"]),
        "doctor_name": appointment.get("doctor_name"),
        "patient_name": appointment.get("patient_name"),
        "scheduled_at": scheduled_at,
        "duration_min": appointment["duration_min"],
        "mode": appointment["mode"],
        "status": status,
        "payment_choice": appointment["payment_choice"],
        "consultation_fee": appointment.get("consultation_fee"),
        "video_enabled": appointment.get("video_enabled", False),
        "call_status": normalize_call_status(appointment.get("call_status", "idle")),
        "appointment_type": appointment.get("appointment_type", "new"),
        "follow_up_of_appointment_id": (
            str(appointment["follow_up_of_appointment_id"])
            if appointment.get("follow_up_of_appointment_id")
            else None
        ),
        "can_cancel": not action_blocked and not within_cancel_window,
        "can_reschedule": not action_blocked and not within_cancel_window,
        "cancel_window_hours": cancel_window_hours,
    }


@router.post(
    "/payments/create-order",
    dependencies=[rl(settings.RL_PUBLIC_PAYMENT_CREATE_TIMES, settings.RL_PUBLIC_PAYMENT_CREATE_SECONDS)],
)
async def create_payment_order(
    payload: PaymentCreateOrderIn,
    token: str = Depends(get_patient_access_token),
):
    db = get_db()

    try:
        appt_oid = ObjectId(payload.appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(appt, token, now)
    return await create_payment_order_for_appointment(
        db,
        appointment=appt,
        appointment_id=payload.appointment_id,
        now=now,
    )


@router.post(
    "/payments/verify",
    dependencies=[rl(settings.RL_PUBLIC_PAYMENT_CREATE_TIMES, settings.RL_PUBLIC_PAYMENT_CREATE_SECONDS)],
)
async def public_verify_payment(
    payload: PaymentVerifyIn,
    token: str = Depends(get_patient_access_token),
):
    db = get_db()

    try:
        appt_oid = ObjectId(payload.appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(appt, token, now)
    
    return await verify_payment_signature_and_confirm(
        db,
        appointment_id=payload.appointment_id,
        razorpay_payment_id=payload.razorpay_payment_id,
        razorpay_order_id=payload.razorpay_order_id,
        razorpay_signature=payload.razorpay_signature,
    )


from pydantic import BaseModel

class VideoTokenRequest(BaseModel):
    recovery_reason: str | None = None
    session_id: str | None = None

@router.post(
    "/appointments/{appointment_id}/video-token",
    dependencies=[rl(settings.RL_PUBLIC_VIDEO_JOIN_TIMES, settings.RL_PUBLIC_VIDEO_JOIN_SECONDS)],
)
async def public_video_token(
    appointment_id: str,
    payload: VideoTokenRequest = None,
    token: str = Depends(get_patient_access_token),
):
    import logging
    import uuid
    from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
    from app.utils.time import ensure_utc
    logger = logging.getLogger(__name__)

    """Generate a LiveKit token for a public (unauthenticated) patient.

    Token generation acts as a soft state transition. Webhooks are the primary
    source of truth for state.
    """
    if not settings.VIDEO_ENABLED:
        raise HTTPException(status_code=503, detail="Video is disabled")

    db = get_db()
    now = utc_now()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    from app.services.call_state_machine import reconcile_call_state
    await reconcile_call_state(appointment_id)
    from pymongo import ReadPreference
    from pymongo.read_concern import ReadConcern
    appointments_col = db.appointments.with_options(
        read_preference=ReadPreference.PRIMARY,
        read_concern=ReadConcern(level="majority")
    )

    appt = await appointments_col.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    validate_patient_token(appt, token, now)

    if appt.get("mode") != "online" or not appt.get("video_enabled", False):
        raise HTTPException(status_code=403, detail="Video is not enabled for this appointment")

    if appt.get("status") != "confirmed":
        raise HTTPException(status_code=409, detail="Invalid appointment state")
        
    if appt.get("call_status") == "ended":
        raise HTTPException(status_code=409, detail="Call already ended")

    check_video_payment(appt, role="patient")
    check_join_window(appt, now, role="patient")

    # Participant limits
    call_status = appt.get("call_status", "idle")
    count = appt.get("call_participant_count", 0)
    if call_status in ["waiting", "connected"]:
        if count >= 2:
            logger.warning("participant_limit_warning: count>=2 appointment_id=%s role=patient(public)", appointment_id) # codeql[py/clear-text-logging-sensitive-data]
        if count >= 3:
            logger.warning("participant_limit_breach: count>=3 appointment_id=%s role=patient(public)", appointment_id) # codeql[py/clear-text-logging-sensitive-data]
            raise HTTPException(status_code=409, detail="Too many participants in the room")

    # Token replay protection (burst limit)
    last_issued = appt.get("patient_last_token_issued_at")
    if last_issued and (now - ensure_utc(last_issued)).total_seconds() < 2:
        logger.warning("token_replay_burst: appointment_id=%s role=patient(public)", appointment_id) # codeql[py/clear-text-logging-sensitive-data]

    # Hard guarantee room reuse
    if not appt.get("video_room"):
        room = await ensure_video_room(db, appt)
    else:
        room = appt["video_room"]

    # Log recovery reason if present
    recovery_reason = payload.recovery_reason if payload else None
    if recovery_reason:
        logger.info(
            "public_reconnecting_recovery_reason",
            extra={"appointment_id": appointment_id, "recovery_reason": recovery_reason}
        )

    # Record patient_joined_at for analytics only — does NOT change call state.
    # State transitions happen only via LiveKit webhooks when participants actually join.
    from pymongo import ReturnDocument
    update_set = {
        "patient_last_token_issued_at": now,
        "updated_at": now,
    }
    if not appt.get("patient_joined_at"):
        update_set["patient_joined_at"] = now

    updated_appt = await appointments_col.find_one_and_update(
        {
            "_id": appt_oid,
            "call_status": {"$in": ["idle", "ended"]},
            "session_locked": {"$ne": True}
        },
        {
            "$set": {
                "call_status": "initializing",
                "session_locked": True,
                **update_set
            }
        },
        return_document=ReturnDocument.AFTER
    )
    if not updated_appt:
        updated_appt = await appointments_col.find_one_and_update(
            {"_id": appt_oid},
            {"$set": update_set},
            return_document=ReturnDocument.AFTER
        )

    session_version = updated_appt.get("session_version") if updated_appt and updated_appt.get("session_version") is not None else 0

    session_id = payload.session_id if payload else None

    from app.core.redis import get_safe_redis
    from app.utils.redis_utils import RedisKeys, LUA_ACQUIRE_LOCK
    redis = get_safe_redis()

    import time
    import json
    
    # 1. Redis Quorum Health / Authority Mode Check
    authority_mode = "redis"
    health_key = "system:redis:health"
    try:
        await redis.ping()
        await redis.redis.set(health_key, str(time.time()), ex=10)
        health_ts_str = await redis.get_str(health_key)
        if health_ts_str:
            health_ts = float(health_ts_str)
            if time.time() - health_ts > 3.0:
                authority_mode = "degraded"
        else:
            authority_mode = "degraded"
    except Exception:
        authority_mode = "mongo"

    # Define variables
    token_id = str(uuid.uuid4())
    epoch = 1

    if authority_mode != "mongo":
        # 2. Call Hard Timeout Check / Creation time
        created_at_key = RedisKeys.call_created_at(appointment_id)
        try:
            # Check hard timeout
            created_at_str = await redis.get_str(created_at_key)
            if created_at_str:
                created_at = float(created_at_str)
                if time.time() - created_at > 7200: # 2 hours hard timeout
                    logger.warning("call_hard_timeout_reached: appt_id=%s", appointment_id)
                    from app.services.call_state_machine import handle_room_finished
                    if appt.get("video_room"):
                        await handle_room_finished(appt["video_room"])
                    raise HTTPException(status_code=403, detail="Call hard timeout reached (max 2 hours).")
            else:
                await redis.redis.set(created_at_key, str(time.time()), nx=True, ex=7200)
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning("Failed to verify call hard timeout or write created_at: %s", str(exc))

        # 3. Session validation and rotation
        if session_id:
            try:
                created_at_str = await redis.get_str(RedisKeys.active_session(session_id))
                if created_at_str:
                    created_at = float(created_at_str)
                    # Session rotation: if older than 10 minutes, force generation of new session_id
                    if time.time() - created_at > 600:
                        session_id = None
                else:
                    session_id = None
            except Exception as exc:
                logger.warning("Failed to check active session in Redis: %s", str(exc))
                session_id = None

        if not session_id:
            session_id = f"csm-{uuid.uuid4().hex[:8]}"
            try:
                await redis.redis.set(RedisKeys.active_session(session_id), str(time.time()), ex=600)
            except Exception as exc:
                logger.warning("Failed to write active session to Redis: %s", str(exc))

        # 4. Atomic Lock & Version Acquisition (Lua)
        join_lock_key = RedisKeys.join_lock(appointment_id)
        call_version_key = RedisKeys.call_version(appointment_id)
        leader_key = RedisKeys.call_leader(appointment_id, "patient")
        epoch_key = RedisKeys.epoch_key(appointment_id, "patient")
        try:
            result = await redis.eval(
                LUA_ACQUIRE_LOCK,
                [join_lock_key, call_version_key, leader_key, epoch_key],
                [str(session_version), token_id, session_id]
            )
            if result and result[0] == -1:
                logger.warning("public_token_request_failed_version_mismatch: current=%s expected=%s", result[1], session_version)
                raise HTTPException(
                    status_code=409,
                    detail="Connection attempt failed due to state mismatch. Please refresh."
                )
            elif result:
                epoch = int(result[1])
                lock_token = int(result[2])
            else:
                raise RuntimeError("Lua acquire lock script returned empty result")
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning("Lua lock and version acquisition failed: %s", str(exc))
            epoch = 1

        # 5. Per-Role fencing with 10-second grace window
        active_token_key = RedisKeys.active_token(appointment_id, "patient")
        prev_token_key = RedisKeys.prev_token(appointment_id, "patient")
        try:
            old_token = await redis.get_str(active_token_key)
            if old_token:
                await redis.redis.set(prev_token_key, old_token, ex=10)
            await redis.redis.set(active_token_key, token_id, ex=7200)
        except Exception as exc:
            logger.warning("Failed to write active fencing tokens: %s", str(exc))

        # 6. Transition Redis State Machine to connecting
        from app.services.call_state_machine import transition_call_redis_state
        await transition_call_redis_state(redis.redis, appointment_id, "connecting", version=session_version)

        # 7. Logs & Metrics
        from app.services.call_state_machine import log_call_timeline, record_metric
        is_reconnect = 1 if payload and payload.session_id else 0
        await log_call_timeline(redis.redis, appointment_id, "token_request", session_id, epoch)
        await record_metric(redis.redis, appointment_id, "reconnects" if is_reconnect else "token_requests")
    else:
        # Fallback in mongo mode
        if not session_id:
            session_id = f"csm-{uuid.uuid4().hex[:8]}"
        epoch = 1

    trace_id = uuid.uuid4().hex
    identity = f"patient:{appointment_id}"
    logger.info("video_token_issued", extra={"appointment_id": appointment_id, "role": "patient", "identity": identity, "trace_id": trace_id, "session_version": session_version, "public": True, "session_id": session_id, "epoch": epoch})

    try:
        join_token = create_video_token(
            room=room,
            identity=identity,
            name=appt.get("patient_name") or "Patient",
            metadata={
                "appointment_id": appointment_id,
                "role": "patient",
                "trace_id": trace_id,
                "session_version": session_version,
                "session_id": session_id,
                "epoch": epoch,
                "token_id": token_id
            },
            ttl_seconds=7200,
        )
    except Exception as e:
        logger.error("livekit_token_generation_failed", extra={"appointment_id": appointment_id, "role": "patient", "public": True, "error": str(e), "trace_id": trace_id})
        raise HTTPException(status_code=500, detail="Failed to generate video token")

    return {
        "provider": "livekit",
        "server_url": settings.LIVEKIT_URL,
        "room": room,
        "token": join_token,
        "session_version": session_version,
        "session_id": session_id,
        "epoch": epoch
    }


@router.get("/appointments/{appointment_id}/reconcile")
async def public_reconcile_call(
    appointment_id: str,
    token: str = Depends(get_patient_access_token),
):
    """Exposes call state reconciliation to resolve potential webhook/state drift."""
    db = get_db()
    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(appt, token, now)

    from app.services.call_state_machine import reconcile_call_state
    reconciled = await reconcile_call_state(appointment_id)
    if not reconciled:
        return appt

    return {
        "appointment_id": str(reconciled["_id"]),
        "call_status": reconciled.get("call_status", "idle"),
        "call_participant_count": reconciled.get("call_participant_count", 0),
        "patient_participant": reconciled.get("patient_participant"),
        "doctor_participant": reconciled.get("doctor_participant"),
    }


class CallHeartbeatRequest(BaseModel):
    session_version: int | None = None
    session_id: str | None = None
    epoch: int | None = None
    seq: int | None = None
    token_id: str | None = None
    sent_at: float | None = None
    rtt: float | None = None

@router.post("/appointments/{appointment_id}/call/heartbeat")
async def public_call_heartbeat(
    appointment_id: str,
    payload: CallHeartbeatRequest = None,
    token: str = Depends(get_patient_access_token),
):
    """Periodic active call heartbeat from public patient to track presence."""
    import time
    import json
    db = get_db()
    now = utc_now()
    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    from app.core.redis import get_safe_redis
    from app.utils.redis_utils import RedisKeys, LUA_REFRESH_LEASE, validate_leader_data, RedisCorruptionError
    redis = get_safe_redis()

    # Retrieve local variables
    session_id = payload.session_id if payload else None
    epoch = payload.epoch if payload else None
    seq = payload.seq if payload else None
    token_id = payload.token_id if payload else None
    incoming_version = payload.session_version if payload else None
    sent_at = payload.sent_at if payload else None
    client_rtt = payload.rtt if payload else None

    # Load db metadata early for consistency
    from pymongo import ReadPreference
    from pymongo.read_concern import ReadConcern
    appointments_col = db.appointments.with_options(
        read_preference=ReadPreference.PRIMARY,
        read_concern=ReadConcern(level="majority")
    )
    
    call_state_key = RedisKeys.call_state(appointment_id)
    db_version = 0
    db_call_status = "idle"
    try:
        redis_meta = await redis.hgetall_parsed(call_state_key)
        if redis_meta:
            db_version = int(redis_meta.get("version", 0))
            db_call_status = str(redis_meta.get("state", "idle"))
        else:
            appt = await appointments_col.find_one({"_id": appt_oid})
            if appt:
                db_version = appt.get("session_version", 0)
                db_call_status = appt.get("call_status", "idle")
    except Exception as exc:
        logger.warning("Failed to read Redis call metadata, falling back to Mongo: %s", str(exc))
        appt = await appointments_col.find_one({"_id": appt_oid})
        if appt:
            db_version = appt.get("session_version", 0)
            db_call_status = appt.get("call_status", "idle")

    # Sequence/Rate check early (Rate-Limit duplicate heartbeat packets within 500ms - return cached response)
    last_ts_key = RedisKeys.last_ts_key(appointment_id, "patient")
    now_ms = int(time.time() * 1000)
    if token_id:
        try:
            last_ts_str = await redis.get_str(last_ts_key)
            if last_ts_str:
                last_ts = int(last_ts_str)
                if 0 <= now_ms - last_ts < 500:
                    resp_key = f"heartbeat_response:{session_id}"
                    cached_res = await redis.get_str(resp_key)
                    if cached_res:
                        res_dict = json.loads(cached_res)
                        res_dict["server_time"] = time.time()
                        return res_dict
                    return {
                        "status": "ok",
                        "call_status": db_call_status,
                        "session_version": db_version,
                        "epoch": epoch or 1,
                        "reconnect": { "strategy": "normal", "retry_after_ms": 1500 },
                        "media_policy": "normal",
                        "terminate": False,
                        "server_time": time.time(),
                        "mode": "degraded" if authority_mode != "redis" else settings.CALL_RECOVERY_MODE
                    }
        except Exception as exc:
            logger.warning("Failed duplicate ts check: %s", str(exc))

    # 1. Heartbeat Deadline Awareness check (Adaptive RTT Clamped Timeout)
    if sent_at:
        effective_rtt = min(client_rtt or 0.0, 2.0)
        deadline = max(5.0, 3.0 * effective_rtt)
        age = time.time() - sent_at
        if age > deadline:
            logger.warning("heartbeat_deadline_exceeded: age=%fs deadline=%fs RTT=%fs appt_id=%s", age, deadline, client_rtt or 0.0, appointment_id)
            return {
                "status": "ok",
                "call_status": db_call_status,
                "session_version": db_version,
                "epoch": epoch or 1,
                "reconnect": { "strategy": "normal", "retry_after_ms": 1500 },
                "media_policy": "normal",
                "terminate": False,
                "ignored": True,
                "server_time": time.time()
            }

    # 2. Control-Plane Kill Switch
    if not settings.CALL_CONTROL_ENABLED:
        return {
            "status": "ok",
            "call_status": "active",
            "reconnect": { "strategy": "client_only" },
            "media_policy": "none",
            "terminate": False,
            "server_time": time.time()
        }

    # 3. Redis Quorum Health / Authority Mode Check
    authority_mode = "redis"
    health_key = "system:redis:health"
    try:
        await redis.ping()
        await redis.redis.set(health_key, str(time.time()), ex=10)
        health_ts_str = await redis.get_str(health_key)
        if health_ts_str and (time.time() - float(health_ts_str) <= 3.0):
            authority_mode = "redis"
        else:
            authority_mode = "degraded"
    except Exception:
        authority_mode = "mongo"

    # 4. Sequence Deduplication (wrapped in try/except)
    if session_id and seq is not None and authority_mode == "redis":
        try:
            seq_key = f"heartbeat_seq:{session_id}"
            resp_key = f"heartbeat_response:{session_id}"
            last_seq = await redis.get_str(seq_key)
            if last_seq is not None and int(seq) <= int(last_seq):
                cached_res = await redis.get_str(resp_key)
                if cached_res:
                    res_dict = json.loads(cached_res)
                    res_dict["server_time"] = time.time()
                    return res_dict
        except Exception as exc:
            logger.warning("Auxiliary sequence deduplication error: %s", str(exc))

    # 5. Bootstrap Mode: Rehydrate Redis call state if missing
    if authority_mode == "redis":
        try:
            call_exists = await redis.exists(call_state_key)
            if not call_exists:
                appt = await appointments_col.find_one({"_id": appt_oid})
                if appt:
                    validate_patient_token(appt, token, now)
                    await redis.hset(call_state_key, mapping={
                        "state": appt.get("call_status", "idle"),
                        "version": str(appt.get("session_version", 0)),
                        "doctor_id": str(appt.get("doctor_id")),
                        "patient_user_id": str(appt.get("patient_user_id") or "")
                    })
                    await redis.expire(call_state_key, 7200)
        except Exception as exc:
            logger.warning("Redis rehydration failed: %s", str(exc))

    # 6. Validate session version if provided (Strict clock check: incoming >= db_version)
    if incoming_version is not None and incoming_version < db_version:
        logger.warning(
            "public_heartbeat_session_version_mismatch: db=%s incoming=%s appt_id=%s",
            db_version, incoming_version, appointment_id
        )
        raise HTTPException(status_code=409, detail="Outdated session version")

    # 7. Epoch Fencing & Timeout Check
    leader_key = RedisKeys.call_leader(appointment_id, "patient")
    terminate = False
    terminate_reason = "none"
    leader_epoch = epoch if epoch is not None else 1
    is_zombie = False
    media_policy = "normal"

    if db_call_status == "ended":
        logger.warning("public_heartbeat_call_already_ended: appt_id=%s", appointment_id)
        terminate = True
        terminate_reason = "call_already_ended"

    if authority_mode != "mongo":
        # 7a. Control-Plane Kill Switch verification key check
        try:
            if await redis.get_str(RedisKeys.kill_switch(appointment_id)):
                logger.warning("call_kill_switch_triggered_heartbeat: appt_id=%s", appointment_id)
                terminate = True
                terminate_reason = "kill_switch"
        except Exception as exc:
            logger.warning("Failed to check control plane kill switch: %s", str(exc))

        # 7b. Call Hard Timeout Check
        if not terminate:
            created_at_key = RedisKeys.call_created_at(appointment_id)
            try:
                created_at_str = await redis.get_str(created_at_key)
                if created_at_str:
                    created_at = float(created_at_str)
                    if time.time() - created_at > 7200: # 2 hours hard timeout
                        logger.warning("call_hard_timeout_reached_heartbeat: appt_id=%s", appointment_id)
                        from app.services.call_state_machine import handle_room_finished
                        appt = await appointments_col.find_one({"_id": appt_oid})
                        if appt:
                            validate_patient_token(appt, token, now)
                            if appt.get("video_room"):
                                await handle_room_finished(appt["video_room"])
                        terminate = True
                        terminate_reason = "hard_timeout"
            except Exception as exc:
                logger.warning("Failed to check hard timeout on heartbeat: %s", str(exc))

        # 7c. Heartbeat Silence Timeout Check with degraded grace tiers
        last_seen_key = RedisKeys.last_seen_key(appointment_id, "patient")
        if not terminate and token_id:
            try:
                last_seen_str = await redis.get_str(last_seen_key)
                if last_seen_str:
                    last_seen = float(last_seen_str)
                    effective_rtt = min(client_rtt or 0.0, 2.0)
                    deadline = max(5.0, 3.0 * effective_rtt)
                    silence_duration = max(0.0, time.time() - last_seen)
                    if silence_duration > 4 * deadline:
                        logger.warning("public_heartbeat_silence_exceeded: silence=%fs limit=%fs appt_id=%s", silence_duration, 4 * deadline, appointment_id)
                        terminate = True
                        terminate_reason = "silence_timeout"
                    elif silence_duration > 2 * deadline:
                        logger.warning("public_heartbeat_silence_degraded: silence=%fs limit=%fs appt_id=%s - restricting media", silence_duration, 2 * deadline, appointment_id)
                        media_policy = "restricted"
            except Exception as exc:
                logger.warning("Failed to check heartbeat silence limit: %s", str(exc))

        # 7d. Atomic Lease Refresh and Recovery (Lua)
        if not terminate and token_id and session_id and epoch is not None:
            try:
                active_token_key = RedisKeys.active_token(appointment_id, "patient")
                epoch_key = RedisKeys.epoch_key(appointment_id, "patient")
                kill_switch_key = RedisKeys.kill_switch(appointment_id)
                leader_key = RedisKeys.call_leader(appointment_id, "patient")
                
                effective_rtt = min(client_rtt or 0.0, 2.0)
                deadline = max(5.0, 3.0 * effective_rtt)
                lease_ttl = int(max(15.0, 3.0 * deadline))

                result = await redis.eval(
                    LUA_REFRESH_LEASE,
                    [leader_key, active_token_key, epoch_key, kill_switch_key],
                    [token_id, session_id, str(epoch), str(db_version), str(lease_ttl)]
                )
                
                if result:
                    status = int(result[0])
                    if status == -4:
                        logger.warning("public_heartbeat_kill_switch_triggered_lua: appt_id=%s", appointment_id)
                        terminate = True
                        terminate_reason = "kill_switch_lua"
                    elif status == -3:
                        prev_token_key = RedisKeys.prev_token(appointment_id, "patient")
                        prev_token_str = await redis.get_str(prev_token_key)
                        if token_id == prev_token_str:
                            is_zombie = True
                            media_policy = "none"
                            logger.info("public_heartbeat_zombie_isolated: token_id=%s appt_id=%s", token_id, appointment_id)
                        else:
                            logger.warning("public_heartbeat_token_fenced_out: expected_active got=%s appt_id=%s", token_id, appointment_id)
                            terminate = True
                            terminate_reason = "token_mismatch"
                    elif status == -2:
                        logger.warning("public_heartbeat_stale_epoch: incoming=%s stored_epoch=%s appt_id=%s", epoch, result[1], appointment_id)
                        terminate = True
                        terminate_reason = "stale_epoch"
                    elif status <= 0:
                        logger.warning("public_heartbeat_lease_failed_status: status=%s appt_id=%s", status, appointment_id)
                        terminate = True
                        terminate_reason = "lua_reject"
                    else:
                        leader_epoch = int(result[1])
                        # Dynamic epoch clamping if client is ahead of leader
                        if epoch > leader_epoch:
                            leader_epoch = epoch
                        leader_session_id = result[2]
                        if isinstance(leader_session_id, bytes):
                            leader_session_id = leader_session_id.decode("utf-8")
                else:
                    logger.warning("LUA_REFRESH_LEASE returned empty result")
                    terminate = True
                    terminate_reason = "lua_empty"
            except Exception as exc:
                logger.error("METRIC redis_corruption details=%s key=%s", str(exc), leader_key)
                terminate = True
                terminate_reason = "json_corruption"

    if terminate:
        logger.warning(
            "METRIC event=heartbeat_failed reason=%s session_id=%s token_id=%s epoch=%s leader_epoch=%s appt_id=%s",
            terminate_reason, session_id or "", token_id or "", epoch or "", leader_epoch, appointment_id
        )
        if authority_mode != "mongo":
            from app.services.call_state_machine import log_call_timeline, record_metric
            await log_call_timeline(redis.redis, appointment_id, "terminate", session_id, epoch)
            await record_metric(redis.redis, appointment_id, "failures")
        return {
            "status": "terminated",
            "terminate": True,
            "call_status": "ended",
            "session_version": db_version,
            "epoch": leader_epoch,
            "server_time": time.time(),
            "mode": "degraded" if authority_mode != "redis" else settings.CALL_RECOVERY_MODE
        }

    # 8. Update successful heartbeat timestamps & telemetry (Skip if zombie client)
    if authority_mode != "mongo" and not is_zombie:
        try:
            await redis.redis.setex(last_seen_key, 120, str(time.time()))
            await redis.redis.set(last_ts_key, str(now_ms), ex=5)
            await redis.redis.setex(f"call_participant:{appointment_id}:patient", 15, "1")
        except Exception as exc:
            logger.warning("Failed to write participant timestamps/liveness lease to Redis: %s", str(exc))

    # 9. Offload side-effects to background task
    from app.worker.tasks.appointment_tasks import process_call_heartbeat
    process_call_heartbeat.apply_async(args=[appointment_id, "patient", session_id, epoch, db_version, not is_zombie, is_zombie])

    # 10. Build response
    response_data = {
        "status": "ok",
        "call_status": db_call_status,
        "session_version": db_version,
        "epoch": leader_epoch,
        "reconnect": {
            "strategy": "normal",
            "retry_after_ms": 1500
        },
        "media_policy": media_policy,
        "terminate": False,
        "server_time": time.time(),
        "mode": "degraded" if authority_mode != "redis" else settings.CALL_RECOVERY_MODE
    }

    # 11. Structured logs & cache
    leader_session_id = ""
    if authority_mode != "mongo":
        from app.services.call_state_machine import log_call_timeline, record_metric
        await log_call_timeline(redis.redis, appointment_id, "heartbeat", session_id, epoch, rtt=client_rtt)
        await record_metric(redis.redis, appointment_id, "heartbeats")
        try:
            leader_val = await redis.get_str(leader_key)
            if leader_val:
                leader_session_id = json.loads(leader_val).get("session_id", "")
        except Exception:
            pass

    # structured tracing metric log
    logger.info(
        "METRIC event=%s session_id=%s token_id=%s authority_version=%d role=%s leader_session_id=%s trace_id=%s",
        "heartbeat", session_id or "", token_id or "", db_version, "patient", leader_session_id, token_id or ""
    )

    if session_id and seq is not None and authority_mode == "redis":
        try:
            await redis.redis.setex(f"heartbeat_seq:{session_id}", 30, str(seq))
            await redis.redis.setex(f"heartbeat_response:{session_id}", 30, json.dumps(response_data))
        except Exception as exc:
            logger.warning("Failed to cache heartbeat response: %s", str(exc))

    return response_data


@router.post(
    "/appointments/{appointment_id}/cancel",
    dependencies=[rl(settings.RL_PUBLIC_ACTION_TIMES, settings.RL_PUBLIC_ACTION_SECONDS)],
)
async def cancel_public_appointment(
    appointment_id: str,
    payload: PatientCancelIn,
    token: str = Depends(get_patient_access_token),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(appt, token, now)

    status = appt.get("status")

    if status == "cancelled":
        return {
            "message": "already_cancelled",
            "appointment_id": appointment_id,
            "status": "cancelled",
            "refund_status": appt.get("refund_status", "none"),
        }

    if status in ("completed", "no_show"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel appointment with status={status}",
        )

    cancel_hours = int(getattr(settings, "CANCEL_WINDOW_HOURS", 2))
    if is_within_cancel_window(appt["scheduled_at"], now, hours=cancel_hours):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel within {cancel_hours} hours of appointment",
        )

    update_set = {
        "status": "cancelled",
        "cancel_reason": payload.reason,
        "cancelled_at": now,
        "cancelled_by": "patient",
        "cancelled_by_id": None,
        "updated_at": now,
    }

    if status == "pending_payment":
        update_set["payment_status"] = "failed"
        update_set["pending_payment_expires_at"] = None

    if status == "confirmed" and appt.get("payment_status") == "paid":
        update_set["refund_status"] = "pending"

    update_res = await db.appointments.update_one(
        {"_id": appt_oid, "status": status},
        {"$set": update_set},
    )
    if update_res.modified_count == 0:
        raise HTTPException(status_code=409, detail="Appointment state changed. Please retry.")

    updated_appt = await db.appointments.find_one({"_id": appt_oid})
    if updated_appt:
        await safe_send_email(send_cancellation_email(updated_appt), "cancellation")
        await safe_send_whatsapp(
            send_patient_appointment_update_whatsapp(updated_appt, "cancelled"),
            "public cancellation",
        )

    if updated_appt and updated_appt.get("doctor_email"):
        await safe_send_email(
            send_doctor_cancellation(
                updated_appt,
                updated_appt["doctor_email"],
                cancelled_by="patient (magic link)",
            ),
            "doctor cancellation",
        )

    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(appt.get("doctor_id")), day=scheduled_at.date().isoformat())
    if appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))

    # SSE: notify doctor of cancellation
    await notify_doctor(str(appt["doctor_id"]), EVENT_APPOINTMENT_CANCELLED, {
        "appointment_id": appointment_id,
        "patient_name": appt.get("patient_name"),
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
    })

    if updated_appt and updated_appt.get("refund_status") == "pending":
        enqueue_refund_processing(str(appt_oid))

    return {
        "message": "cancelled",
        "appointment_id": appointment_id,
        "status": "cancelled",
        "cancelled_at": now.isoformat(),
        "refund_status": update_set.get("refund_status", "none"),
    }


@router.post(
    "/appointments/{appointment_id}/reschedule",
    dependencies=[rl(settings.RL_PUBLIC_ACTION_TIMES, settings.RL_PUBLIC_ACTION_SECONDS)],
)
async def reschedule_public_appointment_phase_a(
    appointment_id: str,
    payload: PatientRescheduleIn,
    response: Response,
    token: str = Depends(get_patient_access_token),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    old = await db.appointments.find_one({"_id": appt_oid})
    if not old:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()
    validate_patient_token(old, token, now)

    old_status = old.get("status")

    if old_status in ("cancelled", "completed", "no_show"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot reschedule appointment with status={old_status}",
        )

    cancel_hours = int(getattr(settings, "CANCEL_WINDOW_HOURS", 2))
    if is_within_cancel_window(old["scheduled_at"], now, hours=cancel_hours):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot reschedule within {cancel_hours} hours of appointment",
        )

    doctor_id = old["doctor_id"]

    doctor = await db.doctors.find_one(
        {"_id": doctor_id, "verification_status": "approved", "is_suspended": {"$ne": True}}
    )
    if not doctor:
        raise HTTPException(status_code=404, detail="Doctor not found")

    avail = await db.doctor_availability.find_one({"doctor_id": doctor_id})
    if not avail:
        raise HTTPException(status_code=400, detail="Doctor hasn't set availability yet")
    avail_tz = avail.get("timezone", "Asia/Kolkata")

    try:
        new_scheduled_at = parse_client_datetime_to_utc(payload.new_scheduled_at)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        raise HTTPException(
            status_code=422,
            detail="new_scheduled_at must be ISO format with UTC offset e.g. 2026-03-02T09:00:00+05:30",
        )

    if new_scheduled_at <= now:
        raise HTTPException(status_code=409, detail="Cannot reschedule to a past time")
    if not is_within_booking_window(
        new_scheduled_at,
        now,
        days=settings.BOOKING_WINDOW_DAYS,
        tz_name=avail_tz,
    ):
        raise HTTPException(
            status_code=409,
            detail=f"Appointments can only be rescheduled within the next {settings.BOOKING_WINDOW_DAYS} days",
        )

    if new_scheduled_at == old["scheduled_at"]:
        raise HTTPException(
            status_code=409,
            detail="New slot must be different from current slot",
        )

    slot_minutes = int(old.get("duration_min") or avail.get("slot_duration_min", 20))

    target_date = new_scheduled_at.astimezone(ZoneInfo(avail_tz)).date()
    candidate_slots, _, _ = await get_candidate_slots_for_date(
        db=db,
        doctor_id=doctor_id,
        target_date=target_date,
    )
    if new_scheduled_at not in candidate_slots:
        raise HTTPException(status_code=400, detail="Selected slot is not available in doctor's schedule")

    existing = await db.appointments.find_one(
        {
            "_id": {"$ne": appt_oid},
            "doctor_id": doctor_id,
            "scheduled_at": new_scheduled_at,
            "$or": [
                {"status": "confirmed"},
                {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now}},
            ],
        },
        {"_id": 1},
    )
    if existing:
        raise HTTPException(status_code=409, detail="Slot already booked")

    payment_choice = old.get("payment_choice")
    mode = old.get("mode")
    consultation_fee = old.get("consultation_fee")
    carried_payment_id = old.get("payment_id")
    carried_order_id = old.get("payment_order_id")

    # Validate that the doctor's current fee has not changed since the patient
    # paid. Carrying a payment forward when the fee differs means the patient
    # either underpays (gets the slot for less than the current fee) or overpays
    # (gets no refund for the excess). Neither outcome is acceptable.
    current_fee = doctor.get("consultation_fee")
    if (
        payment_choice == "pay_now"
        and old.get("payment_status") == "paid"
        and bool(carried_payment_id)
        and current_fee is not None
        and int(current_fee) != int(consultation_fee or 0)
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                "The doctor's consultation fee has changed since your original booking. "
                "Please cancel this appointment for a refund and book again at the updated fee."
            ),
        )

    is_payment_carried = (
        payment_choice == "pay_now"
        and old.get("payment_status") == "paid"
        and bool(carried_payment_id)
    )

    if is_payment_carried:
        new_status = "confirmed"
        new_payment_status = "paid"
        pending_expires = None
    elif payment_choice == "pay_now":
        new_status = "pending_payment"
        new_payment_status = "pending"
        pending_expires = now + timedelta(minutes=int(settings.PAYMENT_HOLD_MINUTES))
    else:
        new_status = "confirmed"
        new_payment_status = "unpaid"
        pending_expires = None

    new_token = generate_magic_token()
    new_token_hash = hash_magic_token(new_token)
    new_token_enc = encrypt_magic_token(new_token)
    new_token_expires_at = build_patient_access_expiry(
        new_scheduled_at,
        duration_min=slot_minutes,
        video_enabled=mode == "online",
    )
    access_max_age = max(1, int((new_token_expires_at - now).total_seconds()))

    new_doc = {
        "doctor_id": doctor_id,
        "doctor_name": old.get("doctor_name"),
        "doctor_email": old.get("doctor_email"),
        "doctor_phone": old.get("doctor_phone"),
        "patient_user_id": old.get("patient_user_id"),
        "patient_id": old.get("patient_id"),
        "patient_phone": old.get("patient_phone"),
        "patient_name": old.get("patient_name"),
        "patient_email": old.get("patient_email"),
        "patient_age": old.get("patient_age"),
        "patient_sex": old.get("patient_sex"),
        "scheduled_at": new_scheduled_at,
        "duration_min": slot_minutes,
        "mode": mode,
        "video_enabled": mode == "online",
        "video_provider": "livekit" if mode == "online" else None,
        "consultation_fee": consultation_fee,
        "payment_choice": payment_choice,
        "payment_status": new_payment_status,
        "refund_status": "none",
        "pending_payment_expires_at": pending_expires,
        "payment_provider": old.get("payment_provider") if is_payment_carried else None,
        "payment_id": carried_payment_id if is_payment_carried else None,
        "payment_order_id": carried_order_id if is_payment_carried else None,
        "payment_signature": old.get("payment_signature") if is_payment_carried else None,
        "payment_amount_paise": old.get("payment_amount_paise") if is_payment_carried else None,
        "payment_root_appointment_id": (
            old.get("payment_root_appointment_id") or old["_id"]
        ) if is_payment_carried else old.get("payment_root_appointment_id"),
        "payment_carried_from_appointment_id": old["_id"] if is_payment_carried else None,
        "status": new_status,
        "confirmed_at": None,
        "patient_access_token_hash": new_token_hash,
        "patient_access_token_enc": new_token_enc,
        "patient_access_expires_at": new_token_expires_at,
        "cancel_reason": None,
        "cancelled_at": None,
        "cancelled_by": None,
        "cancelled_by_id": None,
        "completed_at": None,
        "rescheduled_at": now,
        "rescheduled_from": appt_oid,
        "no_show_at": None,
        "appointment_type": old.get("appointment_type", "new"),
        "follow_up_of_appointment_id": old.get("follow_up_of_appointment_id"),
        "is_follow_up_eligible": old.get("is_follow_up_eligible", False),
        "follow_up_eligible_until": old.get("follow_up_eligible_until"),
        "follow_up_used": old.get("follow_up_used", False),
        "email_reminder_24hr_sent": False,
        "wa_reminder_12hr_sent": False,
        "created_at": now,
        "updated_at": now,
    }

    old_update = {
        "status": "cancelled",
        "cancel_reason": payload.reason or "rescheduled",
        "cancelled_at": now,
        "cancelled_by": "patient",
        "cancelled_by_id": None,
        "rescheduled_at": now,
        "updated_at": now,
    }

    if is_payment_carried:
        old_update.update(
            {
                "payment_status": "transferred",
                "original_payment_id": carried_payment_id,
                "payment_id": None,
                "original_payment_order_id": carried_order_id,
                "payment_order_id": None,
                "payment_signature": None,
                "refund_status": "none",
                "payment_carried_at": now,
            }
        )
    elif old_status == "pending_payment":
        old_update["payment_status"] = "failed"
        old_update["pending_payment_expires_at"] = None

    if (
        not is_payment_carried
        and old_status == "confirmed"
        and old.get("payment_status") == "paid"
    ):
        old_update["refund_status"] = "pending"

    rollback_set = {
        "status": old.get("status"),
        "cancel_reason": old.get("cancel_reason"),
        "cancelled_at": old.get("cancelled_at"),
        "cancelled_by": old.get("cancelled_by"),
        "cancelled_by_id": old.get("cancelled_by_id"),
        "rescheduled_at": old.get("rescheduled_at"),
        "updated_at": old.get("updated_at", now),
        "payment_status": old.get("payment_status"),
        "pending_payment_expires_at": old.get("pending_payment_expires_at"),
        "refund_status": old.get("refund_status", "none"),
        "payment_id": old.get("payment_id"),
        "payment_order_id": old.get("payment_order_id"),
        "payment_signature": old.get("payment_signature"),
        "original_payment_id": old.get("original_payment_id"),
        "original_payment_order_id": old.get("original_payment_order_id"),
        "payment_carried_at": old.get("payment_carried_at"),
        "payment_carried_to_appointment_id": old.get("payment_carried_to_appointment_id"),
    }

    new_id = None

    cancel_res = await db.appointments.update_one(
        {"_id": appt_oid, "status": old.get("status")},
        {"$set": old_update},
    )
    if cancel_res.modified_count == 0:
        raise HTTPException(status_code=409, detail="Appointment state changed. Please retry.")

    try:
        new_res = await db.appointments.insert_one(new_doc)
        new_id = new_res.inserted_id
        if is_payment_carried:
            await db.appointments.update_one(
                {"_id": appt_oid},
                {"$set": {"payment_carried_to_appointment_id": new_id, "updated_at": now}},
            )
    except Exception:
        await db.appointments.update_one({"_id": appt_oid}, {"$set": rollback_set})
        raise HTTPException(
            status_code=500,
            detail="Reschedule failed, please try again",
        )

    new_appt = await db.appointments.find_one({"_id": new_id})
    if new_appt and new_appt.get("patient_email"):
        await safe_send_email(send_reschedule_confirmation(new_appt), "reschedule confirmation")
    if new_appt:
        await safe_send_whatsapp(
            send_patient_appointment_update_whatsapp(new_appt, "rescheduled"),
            "public reschedule",
        )

    doctor_email = new_doc.get("doctor_email")
    if doctor_email:
        new_doc["_id"] = new_id
        await safe_send_email(
            send_doctor_reschedule(old, new_doc, doctor_email, rescheduled_by="patient"),
            "doctor reschedule",
        )

    old_scheduled = ensure_utc(old.get("scheduled_at"))
    new_scheduled = ensure_utc(new_doc.get("scheduled_at"))
    if old_scheduled:
        await invalidate_doctor_cache(str(doctor_id), day=old_scheduled.date().isoformat())
    if new_scheduled:
        await invalidate_doctor_cache(str(doctor_id), day=new_scheduled.date().isoformat())
    if old.get("patient_user_id"):
        await invalidate_patient_cache(str(old["patient_user_id"]))
    if old_update.get("refund_status") == "pending":
        enqueue_refund_processing(str(appt_oid))
    set_public_patient_access_cookie(
        response,
        token=new_token,
        max_age=access_max_age,
    )

    # SSE: notify doctor of reschedule
    await notify_doctor(str(doctor_id), EVENT_APPOINTMENT_RESCHEDULED, {
        "old_appointment_id": appointment_id,
        "new_appointment_id": str(new_id),
        "patient_name": old.get("patient_name"),
        "old_scheduled_at": old_scheduled.isoformat() if old_scheduled else None,
        "new_scheduled_at": new_scheduled.isoformat() if new_scheduled else None,
    })

    return {
        "message": "rescheduled",
        "old_appointment_id": appointment_id,
        "new_appointment_id": str(new_id),
        "new_status": new_status,
        "payment_choice": payment_choice,
        "patient_access_token": new_token,
        "patient_access_expires_at": new_token_expires_at.isoformat(),
    }
