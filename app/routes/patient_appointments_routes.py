from datetime import timedelta, timezone

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from bson import ObjectId
from pymongo.errors import DuplicateKeyError
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.patient_auth_routes import get_current_patient
from app.services.doctor_availability_service import get_candidate_slots_for_date
from app.services.cache_service import (
    TTL_5_MINUTES,
    cache_get_json,
    cache_set_json,
    invalidate_doctor_cache,
    invalidate_patient_cache,
    patient_appointment_detail_key,
    patient_appointments_list_key,
)
from app.schemas.appointment_schema import PatientRescheduleIn, PatientAppointmentBookIn, PaymentCreateOrderIn, PaymentVerifyIn
from app.services.payment_order_service import create_payment_order_for_appointment, verify_payment_signature_and_confirm
from app.services.refund_service import enqueue_refund_processing
from app.utils.appointment_rules import (
    build_patient_access_expiry,
    is_within_booking_window,
    is_within_cancel_window,
)
from app.utils.time import utc_now, parse_client_datetime_to_utc, ensure_utc
from app.utils.appointment_serializers import _review_out, _reminder_prefs_out
from app.utils.magic_token import encrypt_magic_token, generate_magic_token, hash_magic_token
from app.utils.video import check_join_window
from app.services.email_service import (
    safe_send_email,
    send_booking_confirmation,
    send_cancellation_email,
    send_doctor_cancellation,
    send_doctor_new_booking,
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
from app.services.call_state_machine import (
    record_heartbeat,
    remove_heartbeat,
)
from app.services.event_bus import (
    notify_doctor,
    notify_patient,
    EVENT_PATIENT_WAITING,
    EVENT_APPOINTMENT_BOOKED,
    EVENT_APPOINTMENT_CANCELLED,
    EVENT_APPOINTMENT_RESCHEDULED,
)

router = APIRouter(prefix="/patient", tags=["Patient Appointments"])

VISIBLE_TO_PATIENT_STATUSES = ["pending_payment", "confirmed", "completed", "cancelled", "no_show"]


def _appt_to_patient_out(a: dict) -> dict:
    scheduled_at = ensure_utc(a.get("scheduled_at"))
    cancelled_at = ensure_utc(a.get("cancelled_at"))
    follow_up_eligible_until = ensure_utc(a.get("follow_up_eligible_until"))

    return {
        "appointment_id": str(a["_id"]),
        "doctor_id": str(a["doctor_id"]),
        "doctor_name": a.get("doctor_name"),
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
        "duration_min": a.get("duration_min"),
        "mode": a.get("mode"),
        "status": a.get("status"),
        "payment_choice": a.get("payment_choice"),
        "payment_status": a.get("payment_status"),
        "refund_status": a.get("refund_status", "none"),
        "consultation_fee": a.get("consultation_fee"),

        "video_enabled": a.get("video_enabled", False),
        "call_status": a.get("call_status", "idle"),

        "cancel_reason": a.get("cancel_reason"),
        "cancelled_at": cancelled_at.isoformat() if cancelled_at else None,

        "patient_phone": a.get("patient_phone"),
        "patient_name": a.get("patient_name"),
        "patient_email": a.get("patient_email"),

        # ✅ Follow-up fields
        "appointment_type": a.get("appointment_type", "new"),
        "follow_up_of_appointment_id": str(a["follow_up_of_appointment_id"])
        if a.get("follow_up_of_appointment_id") else None,
        "is_follow_up_eligible": a.get("is_follow_up_eligible", False),
        "follow_up_eligible_until": follow_up_eligible_until.isoformat()
        if follow_up_eligible_until else None,
        "follow_up_used": a.get("follow_up_used", False),

        # ✅ Patient notes
        "notes": a.get("patient_notes"),

        # ✅ Review
        "review": _review_out(a.get("review")),

        # ✅ Reminder preferences
        "reminder_preferences": _reminder_prefs_out(a.get("reminder_preferences")),

        # ✅ Creation timestamp (for 'Booked On' display)
        "created_at": ensure_utc(a.get("created_at")).isoformat() if a.get("created_at") else None,
    }


@router.get(
    "/appointments",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def list_patient_appointments(
    request: Request,
    upcoming: bool = Query(False, description="If true, only future appointments; else all"),
    limit: int = Query(20, ge=1, le=100),
    skip: int = Query(0, ge=0, le=5000),
    current=Depends(get_current_patient),
):
    db = get_db()
    now = utc_now()
    patient_cache_id = str(current["_id"])
    cache_key = patient_appointments_list_key(
        patient_cache_id,
        upcoming=upcoming,
        limit=limit,
        skip=skip,
    )
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    q = {
        "patient_user_id": current["_id"],
        "status": {"$in": VISIBLE_TO_PATIENT_STATUSES},
    }

    if upcoming:
        q["scheduled_at"] = {"$gte": now}

    cursor = (
        db.appointments.find(q)
        .sort("scheduled_at", 1 if upcoming else -1)
        .skip(skip)
        .limit(limit)
    )

    items = await cursor.to_list(length=limit)

    response = {
        "items": [_appt_to_patient_out(a) for a in items],
        "skip": skip,
        "limit": limit,
        "count": len(items),
        "upcoming": upcoming,
    }
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


@router.get(
    "/appointments/{appointment_id}",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def get_patient_appointment(
    request: Request,
    appointment_id: str,
    current=Depends(get_current_patient),
):
    db = get_db()
    cache_key = patient_appointment_detail_key(str(current["_id"]), appointment_id)
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    a = await db.appointments.find_one(
        {
            "_id": appt_oid,
            "patient_user_id": current["_id"],
            "status": {"$in": VISIBLE_TO_PATIENT_STATUSES},
        }
    )
    if not a:
        raise HTTPException(status_code=404, detail="Appointment not found")

    response = _appt_to_patient_out(a)
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


@router.post(
    "/appointments/{appointment_id}/join-waiting-room",
    dependencies=[rl(settings.RL_PATIENT_VIDEO_JOIN_TIMES, settings.RL_PATIENT_VIDEO_JOIN_SECONDS)],
)
async def patient_join_waiting_room(
    appointment_id: str,
    current=Depends(get_current_patient),
):
    """Register patient presence in the waiting room via heartbeat.

    This is called when the patient enters the waiting room UI. It records
    a heartbeat and notifies the doctor. The patient should call
    /heartbeat periodically (every 15s) to stay visible.

    Idempotent — safe to call multiple times.
    """
    if not settings.VIDEO_ENABLED:
        raise HTTPException(status_code=503, detail="Video is disabled")

    db = get_db()
    now = utc_now()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid, "patient_user_id": current["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    if appt.get("mode") != "online" or not appt.get("video_enabled", False):
        raise HTTPException(status_code=403, detail="Video is not enabled for this appointment")

    if appt.get("status") != "confirmed":
        raise HTTPException(status_code=409, detail="Appointment is not confirmed")

    check_video_payment(appt, role="patient")

    # Record patient_joined_at for analytics (does not change call state)
    await db.appointments.update_one(
        {"_id": appt_oid, "patient_joined_at": None},
        {"$set": {"patient_joined_at": now, "updated_at": now}},
    )

    # Record heartbeat presence
    await record_heartbeat(
        appointment_id=appointment_id,
        doctor_id=str(appt["doctor_id"]),
        patient_name=appt.get("patient_name", "Patient"),
    )

    # Notify doctor: patient is in the waiting room
    await notify_doctor(str(appt["doctor_id"]), EVENT_PATIENT_WAITING, {
        "appointment_id": appointment_id,
        "patient_name": appt.get("patient_name"),
        "scheduled_at": appt["scheduled_at"].isoformat() if appt.get("scheduled_at") else None,
    })

    return {"message": "joined_waiting_room", "appointment_id": appointment_id}


@router.post(
    "/appointments/{appointment_id}/leave-waiting-room",
    dependencies=[rl(settings.RL_PATIENT_VIDEO_JOIN_TIMES, settings.RL_PATIENT_VIDEO_JOIN_SECONDS)],
)
async def patient_leave_waiting_room(
    appointment_id: str,
    current=Depends(get_current_patient),
):
    """Patient left the waiting room — remove heartbeat presence."""
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid, "patient_user_id": current["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    await remove_heartbeat(appointment_id, str(appt["doctor_id"]))

    # Notify doctor so waiting list updates
    from app.services.event_bus import EVENT_CALL_STATE_CHANGED
    await notify_doctor(str(appt["doctor_id"]), EVENT_CALL_STATE_CHANGED, {
        "appointment_id": appointment_id,
        "call_status": appt.get("call_status", "idle"),
        "event": "patient_left_waiting_room",
    })

    return {"message": "left_waiting_room", "appointment_id": appointment_id}


@router.post(
    "/appointments/{appointment_id}/heartbeat",
    dependencies=[rl(settings.RL_PATIENT_VIDEO_JOIN_TIMES, settings.RL_PATIENT_VIDEO_JOIN_SECONDS)],
)
async def patient_heartbeat(
    appointment_id: str,
    current=Depends(get_current_patient),
):
    """Periodic heartbeat from patient in waiting room (call every 15s).

    Keeps the patient visible in the doctor's waiting list. If no heartbeat
    arrives for 45s, the patient is automatically removed.
    """
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one(
        {"_id": appt_oid, "patient_user_id": current["_id"]},
        {"doctor_id": 1, "patient_name": 1},
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    await record_heartbeat(
        appointment_id=appointment_id,
        doctor_id=str(appt["doctor_id"]),
        patient_name=appt.get("patient_name", "Patient"),
    )

    return {"status": "ok"}


@router.post(
    "/appointments/{appointment_id}/video-token",
    dependencies=[rl(settings.RL_PATIENT_VIDEO_JOIN_TIMES, settings.RL_PATIENT_VIDEO_JOIN_SECONDS)],
)
async def patient_video_token(
    request: Request,
    appointment_id: str,
    current=Depends(get_current_patient),
):
    import logging
    import uuid
    from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
    from app.utils.time import ensure_utc
    
    logger = logging.getLogger(__name__)

    """Generate a LiveKit token for the authenticated patient.

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

    appt = await db.appointments.find_one({"_id": appt_oid, "patient_user_id": current["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    if appt.get("mode") != "online" or not appt.get("video_enabled", False):
        raise HTTPException(status_code=403, detail="Video is not enabled for this appointment")

    if appt.get("status") != "confirmed":
        raise HTTPException(status_code=409, detail="Invalid appointment state")
        
    if appt.get("call_status") == "ended":
        raise HTTPException(status_code=409, detail="Call already ended")

    check_video_payment(appt, role="patient")
    check_join_window(appt, now, role="patient")
    
    # Duplicate tab/device soft protection
    call_status = appt.get("call_status", "idle")
    count = appt.get("call_participant_count", 0)
    if call_status in ["waiting", "connected"]:
        if count >= 2:
            logger.warning("participant_limit_warning: count>=2 appointment_id=%s role=patient", appointment_id)
        if count >= 3:
            logger.warning("participant_limit_breach: count>=3 appointment_id=%s role=patient", appointment_id)
            raise HTTPException(status_code=409, detail="Too many participants in the room")

    # Token replay protection (burst limit)
    last_issued = appt.get("patient_last_token_issued_at")
    if last_issued and (now - ensure_utc(last_issued)).total_seconds() < 2:
        logger.warning("token_replay_burst: appointment_id=%s role=patient", appointment_id)

    # Hard guarantee room reuse
    if not appt.get("video_room"):
        room = await ensure_video_room(db, appt)
    else:
        room = appt["video_room"]

    # Record patient_joined_at for analytics only — does NOT change call state.
    # State transitions happen only via LiveKit webhooks when participants actually join.
    await db.appointments.update_one(
        {"_id": appt_oid},
        {"$set": {"patient_joined_at": now, "patient_last_token_issued_at": now, "updated_at": now}},
    )
 # codeql[py/clear-text-logging-sensitive-data]
    trace_id = uuid.uuid4().hex
    identity = f"patient:{appointment_id}" # codeql[py/clear-text-logging-sensitive-data]
    logger.info("video_token_issued", extra={"appointment_id": appointment_id, "role": "patient", "identity": identity, "trace_id": trace_id})

    try:
        join_token = create_video_token(
            room=room,
            identity=identity, # codeql[py/clear-text-logging-sensitive-data]
            name=appt.get("patient_name") or "Patient",
            metadata={"appointment_id": appointment_id, "role": "patient", "trace_id": trace_id},
            ttl_seconds=7200,
        )
    except Exception as e:
        logger.error("livekit_token_generation_failed", extra={"appointment_id": appointment_id, "role": "patient", "error": str(e), "trace_id": trace_id})
        raise HTTPException(status_code=500, detail="Failed to generate video token")

    return {
        "provider": "livekit",
        "server_url": settings.LIVEKIT_URL,
        "room": room,
        "token": join_token,
    }


@router.post( # codeql[py/clear-text-logging-sensitive-data]
    "/appointments/book",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def patient_book_appointment(
    request: Request,
    payload: PatientAppointmentBookIn,
    current=Depends(get_current_patient),
):
    db = get_db()
    now = utc_now()
 # codeql[py/clear-text-logging-sensitive-data]
    # ---------------------------
    # Patient identity (from auth)
    # ---------------------------
    patient_phone = current.get("phone")
    if not patient_phone:
        raise HTTPException(status_code=400, detail="Patient phone missing")

    patient_age = current.get("age")
    patient_sex = current.get("sex")

    if patient_age is None or patient_sex is None:
        raise HTTPException(
            status_code=400,
            detail="Complete your profile (age and sex) before booking.",
        )

    patient_name = current.get("full_name")
    patient_email = current.get("email")

    # ---------------------------
    # Doctor
    # ---------------------------
    try:
        doctor_oid = ObjectId(payload.doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")

    doctor = await db.doctors.find_one(
        {"_id": doctor_oid, "verification_status": "approved", "is_suspended": {"$ne": True}}
    )
    if not doctor:
        raise HTTPException(status_code=404, detail="Doctor not found")
    available_modes = doctor.get("available_modes") or []
    if available_modes and payload.mode not in available_modes:
        raise HTTPException(
            status_code=400,
            detail=f"Doctor does not accept {payload.mode} appointments currently",
        )

    avail = await db.doctor_availability.find_one({"doctor_id": doctor_oid})
    if not avail:
        raise HTTPException(status_code=400, detail="Doctor hasn't set availability yet")

    slot_minutes = int(avail.get("slot_duration_min", 20))
    avail_tz = avail.get("timezone", "Asia/Kolkata")

    # ---------------------------
    # Follow-up validation
    # ---------------------------
    follow_up_oid = None

    if payload.appointment_type == "follow_up":
        if not payload.follow_up_of_appointment_id:
            raise HTTPException(
                status_code=400,
                detail="follow_up_of_appointment_id required for follow-up booking",
            )

        try:
            follow_up_oid = ObjectId(payload.follow_up_of_appointment_id)
        except Exception:
            raise HTTPException(status_code=422, detail="Invalid follow_up_of_appointment_id")

        original = await db.appointments.find_one(
            {
                "_id": follow_up_oid,
                "doctor_id": doctor_oid,
                "patient_user_id": current["_id"],
            }
        )

        if not original:
            raise HTTPException(status_code=404, detail="Original appointment not found")

        # ✅ Only "new" appointments can have follow-ups — no follow-up of follow-up
        if original.get("appointment_type", "new") != "new":
            raise HTTPException(
                status_code=400,
                detail="Follow-up can only be booked for a new appointment",
            )

        if original.get("status") != "completed":
            raise HTTPException(
                status_code=400,
                detail="Original appointment must be completed to book follow-up",
            )

        if not original.get("is_follow_up_eligible", False):
            raise HTTPException(
                status_code=400,
                detail="Follow-up not eligible for this appointment",
            )

        # ✅ Precise window check using follow_up_eligible_until
        eligible_until = original.get("follow_up_eligible_until")

        # Normalize DB datetime to UTC-aware before comparing
        if eligible_until and eligible_until.tzinfo is None:
            eligible_until = eligible_until.replace(tzinfo=timezone.utc)

        if not eligible_until or now > eligible_until:
            raise HTTPException(
                status_code=400,
                detail="Follow-up window has expired (7 days from completion)",
            )

        # ✅ Permanent flag — cancellation does NOT reset this
        if original.get("follow_up_used"):
            raise HTTPException(
                status_code=409,
                detail="Follow-up already used for this appointment",
            )

    # ---------------------------
    # Validate mode + fee
    # ---------------------------
    if payload.appointment_type == "follow_up":
        # Free follow-up — override fee and payment
        consultation_fee = 0
        payment_choice = "pay_at_clinic"
    else:
        payment_choice = payload.payment_choice
        if payload.mode == "online":
            if payload.payment_choice != "pay_now":
                raise HTTPException(status_code=400, detail="Online appointments require pay_now")
            consultation_fee = doctor.get("online_fee")
        elif payload.mode == "walk_in":
            if payload.payment_choice not in ("pay_now", "pay_at_clinic"):
                raise HTTPException(status_code=400, detail="Invalid payment_choice")
            consultation_fee = doctor.get("walkin_fee")
        else:
            raise HTTPException(status_code=400, detail="Invalid mode")

        if consultation_fee is None:
            raise HTTPException(status_code=400, detail="Doctor has not set consultation fee")

    # ---------------------------
    # Parse time
    # ---------------------------
    try:
        scheduled_at = parse_client_datetime_to_utc(payload.scheduled_at)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        raise HTTPException(status_code=422, detail="scheduled_at must be ISO format with UTC offset e.g. 2026-03-02T09:00:00+05:30")

    if scheduled_at <= now:
        raise HTTPException(status_code=409, detail="Cannot book past time")
    if not is_within_booking_window(
        scheduled_at,
        now,
        days=settings.BOOKING_WINDOW_DAYS,
        tz_name=avail_tz,
    ):
        raise HTTPException(
            status_code=409,
            detail=f"Appointments can only be booked within the next {settings.BOOKING_WINDOW_DAYS} days",
        )

    # ---------------------------
    # Availability validation
    # ---------------------------
    target_date = scheduled_at.astimezone(ZoneInfo(avail_tz)).date()
    candidate_slots, _, _ = await get_candidate_slots_for_date(
        db=db,
        doctor_id=doctor_oid,
        target_date=target_date,
    )
    if scheduled_at not in candidate_slots:
        raise HTTPException(status_code=400, detail="Slot not valid")

    existing = await db.appointments.find_one(
        {
            "doctor_id": doctor_oid,
            "scheduled_at": scheduled_at,
            "$or": [
                {"status": "confirmed"},
                {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now}},
            ],
        },
        {"_id": 1},
    )
    if existing:
        raise HTTPException(status_code=409, detail="Slot already booked")

    # ---------------------------
    # Ensure doctor-side patient record
    # ---------------------------
    patient_match = [{"phone": patient_phone}]
    if patient_email:
        patient_match.append({"email": patient_email})
    patient = await db.patients.find_one(
        {"doctor_id": doctor_oid, "$or": patient_match}
    )

    if not patient:
        patient_doc = {
            "doctor_id": doctor_oid,
            "full_name": patient_name or "Patient",
            "age": patient_age,
            "sex": patient_sex,
            "phone": patient_phone,
            "notes": None,
            "created_by": "patient",
            "created_at": now,
            "updated_at": now,
        }
        # Only include email when it has a value — storing null would collide
        # on the sparse unique index (doctor_id, email).
        if patient_email:
            patient_doc["email"] = patient_email
        try:
            res = await db.patients.insert_one(patient_doc)
            patient_id = res.inserted_id
            patient = patient_doc
            patient["_id"] = patient_id
        except DuplicateKeyError:
            dup_match = [{"phone": patient_phone}]
            if patient_email:
                dup_match.append({"email": patient_email})
            patient = await db.patients.find_one(
                {"doctor_id": doctor_oid, "$or": dup_match}
            )
            if not patient:
                raise HTTPException(status_code=409, detail="Patient already exists but could not be fetched")
            patient_id = patient["_id"]

    else:
        update_patient = {}

        if patient_name and patient.get("full_name") != patient_name:
            update_patient["full_name"] = patient_name

        if patient_email and not patient.get("email"):
            update_patient["email"] = patient_email

        if patient.get("age") is None:
            update_patient["age"] = patient_age

        if patient.get("sex") is None:
            update_patient["sex"] = patient_sex

        if update_patient:
            update_patient["updated_at"] = now
            await db.patients.update_one({"_id": patient["_id"]}, {"$set": update_patient})

        patient_id = patient["_id"]

    # ---------------------------
    # Appointment status
    # ---------------------------
    if payload.appointment_type == "follow_up":
        # Follow-up always confirmed immediately — free, no payment needed
        appointment_status = "confirmed"
        payment_status = "unpaid"
        pending_expires = None
    elif payment_choice == "pay_now":
        appointment_status = "pending_payment"
        payment_status = "pending"
        pending_expires = now + timedelta(minutes=int(settings.PAYMENT_HOLD_MINUTES))
    else:
        appointment_status = "confirmed"
        payment_status = "unpaid"
        pending_expires = None

    patient_access_token = generate_magic_token()
    patient_access_token_hash = hash_magic_token(patient_access_token)
    patient_access_token_enc = encrypt_magic_token(patient_access_token)

    # ---------------------------
    # Appointment doc
    # ---------------------------
    appt_doc = {
        "doctor_id": doctor_oid,
        "doctor_name": doctor.get("full_name"),
        "doctor_email": doctor.get("email") or None,
        "doctor_phone": doctor.get("phone") or None,

        "patient_user_id": current["_id"],
        "patient_id": patient_id,

        "patient_phone": patient_phone,
        "patient_name": patient_name or "Patient",
        "patient_email": patient_email,
        "patient_age": patient_age,
        "patient_sex": patient_sex,

        "scheduled_at": scheduled_at,
        "duration_min": slot_minutes,
        "mode": payload.mode,
        "consultation_fee": consultation_fee,

        "payment_choice": payment_choice,
        "payment_status": payment_status,
        "refund_status": "none",
        "pending_payment_expires_at": pending_expires,

        "video_provider": "livekit" if payload.mode == "online" else None,
        "video_room": None,
        "video_enabled": payload.mode == "online",
        "call_status": "idle",
        "patient_participant": None,
        "doctor_participant": None,
        "call_participant_count": 0,
        "call_connected_at": None,
        "call_disconnected_at": None,
        "patient_joined_at": None,
        "doctor_joined_at": None,
        "call_started_at": None,
        "call_ended_at": None,
        "status": appointment_status,
        "confirmed_at": now if appointment_status == "confirmed" else None,

        "patient_access_token_hash": patient_access_token_hash,
        "patient_access_token_enc": patient_access_token_enc,
        "patient_access_expires_at": build_patient_access_expiry(
            scheduled_at,
            duration_min=slot_minutes,
            video_enabled=payload.mode == "online",
        ),

        "cancel_reason": None,
        "cancelled_at": None,
        "cancelled_by": None,
        "cancelled_by_id": None,

        "completed_at": None,
        "rescheduled_at": None,
        "rescheduled_from": None,
        "no_show_at": None,

        # ✅ Follow-up fields
        "appointment_type": payload.appointment_type,
        "follow_up_of_appointment_id": follow_up_oid,
        "is_follow_up_eligible": False,  
        "follow_up_eligible_until": None,   # set when doctor marks complete
        "follow_up_used": False,            # set True when follow-up is booked
        "email_reminder_24hr_sent": False,
        "wa_reminder_12hr_sent": False,

        "created_at": now,
        "updated_at": now,
    }

    try:
        res = await db.appointments.insert_one(appt_doc)
        appt_doc["_id"] = res.inserted_id
    except DuplicateKeyError:
        if payload.appointment_type == "follow_up":
            raise HTTPException(
                status_code=409,
                detail="Follow-up already used for this appointment",
            )
        raise HTTPException(status_code=409, detail="Slot already booked. Please retry.")

    # Mark original appointment follow_up_used = True after successful insert.
    # The unique partial index on follow_up_of_appointment_id already prevents a second
    # follow-up insert, so a DuplicateKeyError above is the primary guard. This update
    # sets the display flag and clears eligibility.
    if payload.appointment_type == "follow_up" and follow_up_oid:
        await db.appointments.update_one(
            {"_id": follow_up_oid, "follow_up_used": {"$ne": True}},
            {
                "$set": {
                    "follow_up_used": True,
                    "is_follow_up_eligible": False,
                    "updated_at": now,
                }
            },
        )

    await invalidate_doctor_cache(str(doctor_oid), day=target_date.isoformat())
    await invalidate_patient_cache(str(current["_id"]))

    access_expires_at = build_patient_access_expiry(
        scheduled_at,
        duration_min=slot_minutes,
        video_enabled=payload.mode == "online",
    )

    # Send emails if confirmed immediately
    if appointment_status == "confirmed":
        notify_doc = dict(appt_doc)
        notify_doc["patient_access_token"] = patient_access_token
        if notify_doc.get("mode") == "online" and notify_doc.get("video_enabled"):
            notify_doc["video_room"] = await ensure_video_room(db, notify_doc)
        patient_email_ok = False
        if notify_doc.get("patient_email"):
            patient_email_ok = await safe_send_email(send_booking_confirmation(notify_doc), "booking confirmation")
        wa_ok = await safe_send_whatsapp(
            send_patient_appointment_update_whatsapp(notify_doc, "booked"),
            "patient booking confirmation",
        )
        doctor_email_ok = False
        if notify_doc.get("doctor_email"):
            doctor_email_ok = await safe_send_email(
                send_doctor_new_booking(notify_doc, notify_doc["doctor_email"]),
                "doctor new booking",
            )

        await db.appointments.update_one(
            {"_id": res.inserted_id},
            {"$set": {
                "notifications.booking_patient_email_sent": patient_email_ok,
                "notifications.booking_wa_sent": wa_ok,
                "notifications.booking_doctor_email_sent": doctor_email_ok,
                "notifications.booking_attempted_at": utc_now(),
            }},
        )

        # SSE: notify doctor of new booking
        await notify_doctor(str(doctor_oid), EVENT_APPOINTMENT_BOOKED, {
            "appointment_id": str(res.inserted_id),
            "patient_name": patient_name,
            "scheduled_at": scheduled_at.isoformat(),
            "mode": payload.mode,
        })

    return {
        "message": "appointment_created",
        "appointment_id": str(res.inserted_id),
        "status": appointment_status,
        "payment_choice": payment_choice,
        "appointment_type": payload.appointment_type,
        "consultation_fee": consultation_fee,
    }


@router.post(
    "/payments/create-order",
    dependencies=[rl(settings.RL_PATIENT_MUTATION_TIMES, settings.RL_PATIENT_MUTATION_SECONDS)],
)
async def patient_create_payment_order(
    payload: PaymentCreateOrderIn,
    current=Depends(get_current_patient),
):
    db = get_db()

    try:
        appt_oid = ObjectId(payload.appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one(
        {
            "_id": appt_oid,
            "patient_user_id": current["_id"],
        }
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    return await create_payment_order_for_appointment(
        db,
        appointment=appt,
        appointment_id=payload.appointment_id,
        now=utc_now(),
    )


@router.post(
    "/payments/verify",
    dependencies=[rl(settings.RL_PATIENT_MUTATION_TIMES, settings.RL_PATIENT_MUTATION_SECONDS)],
)
async def patient_verify_payment(
    payload: PaymentVerifyIn,
    current=Depends(get_current_patient),
):
    db = get_db()

    try:
        appt_oid = ObjectId(payload.appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one(
        {
            "_id": appt_oid,
            "patient_user_id": current["_id"],
        }
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    return await verify_payment_signature_and_confirm(
        db,
        appointment_id=payload.appointment_id,
        razorpay_payment_id=payload.razorpay_payment_id,
        razorpay_order_id=payload.razorpay_order_id,
        razorpay_signature=payload.razorpay_signature,
    )


@router.post(
    "/appointments/{appointment_id}/cancel",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def patient_cancel_appointment(
    request: Request,
    appointment_id: str,
    reason: str | None = Body(None),
    current=Depends(get_current_patient),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({
        "_id": appt_oid,
        "patient_user_id": current["_id"],
    })

    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()

    if appt.get("status") in ("cancelled", "completed", "no_show"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel appointment with status={appt.get('status')}",
        )

    cancel_hours = int(getattr(settings, "CANCEL_WINDOW_HOURS", 2))
    if is_within_cancel_window(appt["scheduled_at"], now, hours=cancel_hours):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel within {cancel_hours} hours of appointment",
        )

    update_set = {
        "status": "cancelled",
        "cancel_reason": reason or "cancelled_by_patient",
        "cancelled_at": now,
        "cancelled_by": "patient",
        "cancelled_by_id": current["_id"],
        "updated_at": now,
    }

    if appt.get("status") == "pending_payment":
        update_set["payment_status"] = "failed"
        update_set["pending_payment_expires_at"] = None

    if appt.get("status") == "confirmed" and appt.get("payment_status") == "paid":
        update_set["refund_status"] = "pending"

    prev_status = appt.get("status")
    update_res = await db.appointments.update_one(
        {"_id": appt_oid, "status": prev_status},
        {"$set": update_set},
    )
    if update_res.modified_count == 0:
        raise HTTPException(status_code=409, detail="Appointment state changed. Please retry.")

    updated = await db.appointments.find_one({"_id": appt_oid})

    if updated.get("patient_email"):
        await safe_send_email(
            send_cancellation_email(updated),
            "patient cancellation"
        )
    await safe_send_whatsapp(
        send_patient_appointment_update_whatsapp(updated, "cancelled"),
        "patient cancellation",
    )

    if updated.get("doctor_email"):
        await safe_send_email(
            send_doctor_cancellation(updated, updated["doctor_email"], cancelled_by="patient"),
            "doctor cancellation"
        )

    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(appt.get("doctor_id")), day=scheduled_at.date().isoformat())
    await invalidate_patient_cache(str(current["_id"]))

    # SSE: notify doctor of cancellation
    await notify_doctor(str(appt["doctor_id"]), EVENT_APPOINTMENT_CANCELLED, {
        "appointment_id": appointment_id,
        "patient_name": appt.get("patient_name"),
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
    })

    if updated.get("refund_status") == "pending":
        enqueue_refund_processing(str(appt_oid))

    return {
        "message": "appointment_cancelled",
        "appointment_id": appointment_id,
        "status": "cancelled",
    }


@router.post(
    "/appointments/{appointment_id}/reschedule",
    dependencies=[rl(settings.RL_PATIENT_MUTATION_TIMES, settings.RL_PATIENT_MUTATION_SECONDS)],
)
async def patient_reschedule_appointment(
    request: Request,
    appointment_id: str,
    payload: PatientRescheduleIn,
    current=Depends(get_current_patient),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    old = await db.appointments.find_one({
        "_id": appt_oid,
        "patient_user_id": current["_id"],
    })

    if not old:
        raise HTTPException(status_code=404, detail="Appointment not found")

    now = utc_now()

    if old.get("status") in ("cancelled", "completed", "no_show"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot reschedule appointment with status={old.get('status')}",
        )

    cancel_hours = int(getattr(settings, "CANCEL_WINDOW_HOURS", 2))
    if is_within_cancel_window(old["scheduled_at"], now, hours=cancel_hours):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot reschedule within {cancel_hours} hours of appointment",
        )

    doctor_id = old["doctor_id"]
    doctor = await db.doctors.find_one(
        {"_id": doctor_id, "verification_status": "approved", "is_suspended": {"$ne": True}},
        {"_id": 1, "consultation_fee": 1},
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
        raise HTTPException(status_code=422, detail="new_scheduled_at must be ISO format with UTC offset e.g. 2026-03-02T09:00:00+05:30")

    if new_scheduled_at <= now:
        raise HTTPException(status_code=409, detail="Cannot reschedule to past")
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
        raise HTTPException(status_code=409, detail="Choose different slot")

    slot_minutes = int(old.get("duration_min") or avail.get("slot_duration_min", 20))
    target_date = new_scheduled_at.astimezone(ZoneInfo(avail_tz)).date()
    candidate_slots, _, _ = await get_candidate_slots_for_date(
        db=db,
        doctor_id=doctor_id,
        target_date=target_date,
    )

    if new_scheduled_at not in candidate_slots:
        raise HTTPException(status_code=400, detail="Slot not available")

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
    carried_payment_id = old.get("payment_id")
    carried_order_id = old.get("payment_order_id")
    old_fee = old.get("consultation_fee")

    # Validate that the doctor's current fee has not changed since the patient
    # paid. Carrying a payment forward when the fee differs means the patient
    # either underpays or overpays with no refund.
    current_fee = doctor.get("consultation_fee")
    if (
        payment_choice == "pay_now"
        and old.get("payment_status") == "paid"
        and bool(carried_payment_id)
        and current_fee is not None
        and int(current_fee) != int(old_fee or 0)
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

    new_patient_access_token = generate_magic_token()
    new_doc = {
        "doctor_id": doctor_id,
        "doctor_name": old.get("doctor_name"),
        "doctor_email": old.get("doctor_email"),
        "patient_user_id": old.get("patient_user_id"),
        "patient_id": old.get("patient_id"),
        "patient_phone": old.get("patient_phone"),
        "patient_name": old.get("patient_name"),
        "patient_email": old.get("patient_email"),
        "patient_age": old.get("patient_age"),
        "patient_sex": old.get("patient_sex"),
        "scheduled_at": new_scheduled_at,
        "duration_min": slot_minutes,
        "mode": old.get("mode"),
        "consultation_fee": old.get("consultation_fee"),
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
        "confirmed_at": now if new_status == "confirmed" else None,
        "patient_access_token_hash": hash_magic_token(new_patient_access_token),
        "patient_access_token_enc": encrypt_magic_token(new_patient_access_token),
        "patient_access_expires_at": build_patient_access_expiry(
            new_scheduled_at,
            duration_min=slot_minutes,
            video_enabled=old.get("mode") == "online",
        ),
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
        "cancel_reason": "rescheduled",
        "cancelled_at": now,
        "cancelled_by": "patient",
        "cancelled_by_id": current["_id"],
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
    elif old.get("status") == "pending_payment":
        old_update["payment_status"] = "failed"
        old_update["pending_payment_expires_at"] = None

    if (
        not is_payment_carried
        and old.get("status") == "confirmed"
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

    new_doc["_id"] = new_id

    if new_doc.get("patient_email"):
        await safe_send_email(send_reschedule_confirmation(new_doc), "patient reschedule")
    await safe_send_whatsapp(
        send_patient_appointment_update_whatsapp(new_doc, "rescheduled"),
        "patient reschedule",
    )

    if new_doc.get("doctor_email"):
        await safe_send_email(
            send_doctor_reschedule(old, new_doc, new_doc["doctor_email"], rescheduled_by="patient"),
            "doctor reschedule",
        )

    old_scheduled = ensure_utc(old.get("scheduled_at"))
    new_scheduled = ensure_utc(new_doc.get("scheduled_at"))
    if old_scheduled:
        await invalidate_doctor_cache(str(doctor_id), day=old_scheduled.date().isoformat())
    if new_scheduled:
        await invalidate_doctor_cache(str(doctor_id), day=new_scheduled.date().isoformat())
    await invalidate_patient_cache(str(current["_id"]))

    if old_update.get("refund_status") == "pending":
        enqueue_refund_processing(str(appt_oid))

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
        "new_appointment_id": str(new_id),
        "status": new_status,
    }

