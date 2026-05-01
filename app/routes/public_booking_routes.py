from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from bson import ObjectId
from pymongo.errors import DuplicateKeyError
import logging
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.security import set_public_patient_access_cookie
from app.services.cache_service import invalidate_doctor_cache
from app.services.doctor_availability_service import get_candidate_slots_for_date
from app.schemas.appointment_schema import AppointmentBook, AppointmentBookedOut
from app.utils.appointment_rules import build_patient_access_expiry, is_within_booking_window
from app.utils.time import parse_client_datetime_to_utc, utc_now
from app.utils.magic_token import encrypt_magic_token, generate_magic_token, hash_magic_token
from app.services.email_service import (
    safe_send_email,
    send_booking_confirmation,
    send_doctor_new_booking,
)
from app.services.whatsapp_service import (
    safe_send_whatsapp,
    send_patient_appointment_update_whatsapp,
)
from app.services.video_service import ensure_video_room
from app.services.event_bus import notify_doctor, EVENT_APPOINTMENT_BOOKED

router = APIRouter(prefix="/public", tags=["Public Booking"])
logger = logging.getLogger(__name__)


async def _resolve_patient_user_id(db, phone: str):
    """If an authenticated patient account exists for this phone, return its _id."""
    if not phone:
        return None
    user = await db.patient_users.find_one({"phone": phone}, {"_id": 1})
    return user["_id"] if user else None


@router.post(
    "/appointments/book",
    response_model=AppointmentBookedOut,
    dependencies=[rl(settings.RL_PUBLIC_BOOKING_TIMES, settings.RL_PUBLIC_BOOKING_SECONDS)],
)
async def book_appointment(payload: AppointmentBook, request: Request, response: Response):
    db = get_db()

    if payload.appointment_type != "new":
        raise HTTPException(
            status_code=400,
            detail="Public booking supports only appointment_type='new'. Use authenticated patient booking for follow-up.",
        )

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

    if payload.mode == "online":
        if payload.payment_choice != "pay_now":
            raise HTTPException(status_code=400, detail="Online appointments require pay_now")
    elif payload.mode == "walk_in":
        if payload.payment_choice not in ("pay_now", "pay_at_clinic"):
            raise HTTPException(status_code=400, detail="Walk-in payment_choice must be pay_now or pay_at_clinic")
    else:
        raise HTTPException(status_code=400, detail="Invalid mode")

    if payload.mode == "online":
        consultation_fee = doctor.get("online_fee")
    else:
        consultation_fee = doctor.get("walkin_fee")

    if consultation_fee is None:
        raise HTTPException(status_code=400, detail="Doctor has not set consultation fee for this mode")

    try:
        scheduled_at = parse_client_datetime_to_utc(payload.scheduled_at)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        raise HTTPException(status_code=422, detail="scheduled_at must be ISO format with UTC offset e.g. 2026-03-02T09:00:00+05:30")

    target_date = scheduled_at.astimezone(ZoneInfo(avail_tz)).date()
    candidate_slots, _, _ = await get_candidate_slots_for_date(
        db=db,
        doctor_id=doctor_oid,
        target_date=target_date,
    )
    if scheduled_at not in candidate_slots:
        raise HTTPException(status_code=400, detail="Selected slot is not available in doctor's schedule")

    now = utc_now()
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

    # ✅ Prevent patient from booking if they already have an active appointment with this doctor
    active_appointment = await db.appointments.find_one(
        {
            "doctor_id": doctor_oid,
            "patient_phone": payload.patient_phone,
            "scheduled_at": {"$gte": now},
            "$or": [
                {"status": "confirmed"},
                {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now}},
            ],
        }
    )
    if active_appointment:
        raise HTTPException(
            status_code=409,
            detail="You already have an active appointment with this doctor. Please complete or cancel it before booking again.",
        )

    # ✅ ensure doctor-side patient record exists (patients collection)
    patient_match = [{"phone": payload.patient_phone}]
    if payload.patient_email:
        patient_match.append({"email": payload.patient_email})
    patient = await db.patients.find_one({"doctor_id": doctor_oid, "$or": patient_match})

    if not patient:
        patient_doc = {
            "doctor_id": doctor_oid,
            "full_name": payload.patient_name,
            "age": payload.patient_age,
            "sex": payload.patient_sex,
            "phone": payload.patient_phone,
            "notes": None,
            "created_by": "patient",
            "created_at": now,
            "updated_at": now,
        }
        # Only include email when it has a value — storing null would collide
        # on the sparse unique index (doctor_id, email).
        if payload.patient_email:
            patient_doc["email"] = payload.patient_email
        try:
            res = await db.patients.insert_one(patient_doc)
            patient_id = res.inserted_id
        except DuplicateKeyError:
            patient = await db.patients.find_one({"doctor_id": doctor_oid, "$or": patient_match})
            if not patient:
                raise HTTPException(status_code=409, detail="Patient already exists but could not be fetched")
            patient_id = patient["_id"]
    else:
        update_patient = {}
        if not patient.get("phone") and payload.patient_phone:
            update_patient["phone"] = payload.patient_phone
        if not patient.get("email") and payload.patient_email:
            update_patient["email"] = payload.patient_email
        if update_patient:
            update_patient["updated_at"] = now
            await db.patients.update_one({"_id": patient["_id"]}, {"$set": update_patient})
        patient_id = patient["_id"]

    if payload.payment_choice == "pay_now":
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
    video_enabled = payload.mode == "online"
    patient_access_expires_at = build_patient_access_expiry(
        scheduled_at,
        duration_min=slot_minutes,
        video_enabled=video_enabled,
    )
    access_max_age = max(1, int((patient_access_expires_at - now).total_seconds()))

    appt_doc = {
        "doctor_id": doctor_oid,
        "doctor_name": doctor.get("full_name"),
        "doctor_email": doctor.get("email") or None,
        "doctor_phone": doctor.get("phone") or None,

        # Link to authenticated patient account if one exists for this phone
        "patient_user_id": await _resolve_patient_user_id(db, payload.patient_phone),
        "patient_id": patient_id,

        "patient_phone": payload.patient_phone,
        "patient_name": payload.patient_name,
        "patient_email": payload.patient_email,
        "patient_age": payload.patient_age,
        "patient_sex": payload.patient_sex,

        "scheduled_at": scheduled_at,
        "duration_min": slot_minutes,
        "mode": payload.mode,
        "consultation_fee": consultation_fee,

        "payment_choice": payload.payment_choice,
        "payment_status": payment_status,
        "refund_status": "none",
        "pending_payment_expires_at": pending_expires,

        "video_provider": "livekit" if video_enabled else None,
        "video_room": None,
        "video_enabled": video_enabled,
        "call_status": "idle",
        "call_participants": [],
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
        "patient_access_expires_at": patient_access_expires_at,

        "cancel_reason": None,
        "cancelled_at": None,
        "cancelled_by": None,
        "cancelled_by_id": None,

        "completed_at": None,
        "rescheduled_at": None,
        "rescheduled_from": None,
        "no_show_at": None,

        "created_at": now,
        "updated_at": now,

        "appointment_type": "new",              # public booking always new
        "follow_up_of_appointment_id": None,
        "is_follow_up_eligible": False,
        "follow_up_eligible_until": None,
        "follow_up_used": False,
        "email_reminder_24hr_sent": False,
        "wa_reminder_12hr_sent": False,
    }

    try:
        res = await db.appointments.insert_one(appt_doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Slot already booked. Please retry.")

    await invalidate_doctor_cache(str(doctor_oid), day=target_date.isoformat())
    set_public_patient_access_cookie(
        response,
        token=patient_access_token,
        max_age=access_max_age,
    )

    # ✅ Send emails for pay_at_clinic (confirmed immediately)
    if appointment_status == "confirmed":
        notify_doc = dict(appt_doc)
        notify_doc["_id"] = res.inserted_id
        notify_doc["patient_access_token"] = patient_access_token
        if notify_doc.get("mode") == "online" and notify_doc.get("video_enabled"):
            notify_doc["video_room"] = await ensure_video_room(db, notify_doc)

        patient_email_ok = False
        if notify_doc.get("patient_email"):
            patient_email_ok = await safe_send_email(send_booking_confirmation(notify_doc), "booking confirmation")
        wa_ok = await safe_send_whatsapp(
            send_patient_appointment_update_whatsapp(notify_doc, "booked"),
            "public booking confirmation",
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
            "patient_name": payload.patient_name,
            "scheduled_at": scheduled_at.isoformat(),
            "mode": payload.mode,
        })

    return {
        "message": "appointment_created",
        "appointment_id": str(res.inserted_id),
        "status": appointment_status,
        "payment_choice": payload.payment_choice,
        "patient_access_token": patient_access_token,
        "patient_access_expires_at": patient_access_expires_at,
    }


