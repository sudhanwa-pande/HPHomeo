from datetime import timedelta
import logging
from zoneinfo import ZoneInfo

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Response

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.security import set_public_patient_access_cookie
from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
from app.schemas.appointment_schema import (
    PatientCancelIn,
    PatientRescheduleIn,
    PaymentCreateOrderIn,
    PublicAppointmentAccessIn,
    PublicAppointmentView,
)
from app.services.payment_order_service import create_payment_order_for_appointment
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
        "call_status": appointment.get("call_status", "idle"),
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
    "/appointments/{appointment_id}/video-token",
    dependencies=[rl(settings.RL_PUBLIC_VIDEO_JOIN_TIMES, settings.RL_PUBLIC_VIDEO_JOIN_SECONDS)],
)
async def public_video_token(
    appointment_id: str,
    token: str = Depends(get_patient_access_token),
):
    """Generate a LiveKit token for a public (unauthenticated) patient.

    Token generation does NOT change call state — state transitions happen
    only via LiveKit webhooks when participants actually join the room.
    """
    if not settings.VIDEO_ENABLED:
        raise HTTPException(status_code=503, detail="Video is disabled")

    db = get_db()
    now = utc_now()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    validate_patient_token(appt, token, now)

    if appt.get("mode") != "online" or not appt.get("video_enabled", False):
        raise HTTPException(status_code=403, detail="Video is not enabled for this appointment")

    check_video_payment(appt, role="patient")
    check_join_window(appt, now, role="patient")

    room = await ensure_video_room(db, appt)

    # Record patient_joined_at for analytics (does not change call state)
    await db.appointments.update_one(
        {"_id": appt_oid, "patient_joined_at": None},
        {"$set": {"patient_joined_at": now, "updated_at": now}},
    )

    patient_phone = appt.get("patient_phone")
    if not patient_phone:
        raise HTTPException(status_code=400, detail="Patient phone is missing for this appointment")

    join_token = create_video_token(
        room=room,
        identity=f"patient:{patient_phone}",
        metadata={"appointment_id": str(appt["_id"]), "role": "patient"},
        ttl_seconds=settings.LIVEKIT_TOKEN_TTL_SECONDS,
    )
    return {
        "provider": "livekit",
        "server_url": settings.LIVEKIT_URL,
        "room": room,
        "token": join_token,
    }


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
