from datetime import timedelta

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.auth_routes import get_current_doctor
from app.services.cache_service import (
    TTL_2_MINUTES,
    TTL_5_MINUTES,
    cache_get_json,
    cache_set_json,
    doctor_appointments_key,
    doctor_daily_stats_key,
    doctor_stats_key,
    invalidate_doctor_cache,
    invalidate_patient_cache,
)
from app.services.video_service import (
    check_video_payment,
    create_video_token,
    ensure_video_room,
)
from app.services.call_state_machine import (
    get_calls_dashboard,
    handle_manual_end,
)
from app.utils.clinic import clinic_profile_fields
from app.utils.time import day_window_to_utc, ensure_utc, utc_now

router = APIRouter(prefix="/doctor", tags=["Doctor Appointments"])


VISIBLE_TO_DOCTOR_STATUSES = [
    "confirmed",
    "completed",
    "cancelled",
    "no_show",
    "pending_payment",
]


def _oid(x) -> ObjectId:
    try:
        return x if isinstance(x, ObjectId) else ObjectId(str(x))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid id")


@router.get(
    "/stats",
    dependencies=[rl(settings.RL_DOCTOR_STATS_TIMES, settings.RL_DOCTOR_STATS_SECONDS)],
)
async def get_doctor_stats(doctor=Depends(get_current_doctor)):
    db = get_db()
    cache_key = doctor_stats_key(str(doctor["_id"]))
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    now = utc_now()
    window_start = now - timedelta(days=30)
    next_7_days = now + timedelta(days=7)

    availability = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]})
    avail_tz = availability.get("timezone", "Asia/Kolkata") if availability else "Asia/Kolkata"
    day_start_utc, day_end_utc = day_window_to_utc(now.date().isoformat(), tz_name=avail_tz)

    base_filter = {"doctor_id": doctor["_id"], "scheduled_at": {"$gte": window_start}}
    today_filter = {
        "doctor_id": doctor["_id"],
        "scheduled_at": {"$gte": day_start_utc, "$lt": day_end_utc},
    }
    upcoming_filter = {
        "doctor_id": doctor["_id"],
        "status": "confirmed",
        "scheduled_at": {"$gte": now, "$lt": next_7_days},
    }
    paid_filter = {
        "doctor_id": doctor["_id"],
        "payment_status": "paid",
        "scheduled_at": {"$gte": window_start},
    }

    status_counts = {}
    for status in ["confirmed", "completed", "cancelled", "no_show", "pending_payment"]:
        status_counts[status] = await db.appointments.count_documents({**base_filter, "status": status})

    paid_amount = 0
    async for row in db.appointments.aggregate(
        [
            {"$match": paid_filter},
            {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$consultation_fee", 0]}}}},
        ]
    ):
        paid_amount = int(row.get("total") or 0)

    total_appointments = await db.appointments.count_documents({"doctor_id": doctor["_id"]})

    response = {
        "doctor_id": str(doctor["_id"]),
        "window_days": 30,
        "generated_at": now.isoformat(),
        "today_appointments": await db.appointments.count_documents(today_filter),
        "upcoming_7d_confirmed": await db.appointments.count_documents(upcoming_filter),
        "total_appointments": total_appointments,
        "status_counts_30d": status_counts,
        "paid_revenue_30d": paid_amount,
    }
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


@router.get(
    "/stats/daily",
    dependencies=[rl(settings.RL_DOCTOR_DAILY_STATS_TIMES, settings.RL_DOCTOR_DAILY_STATS_SECONDS)],
)
async def get_doctor_daily_stats(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    doctor=Depends(get_current_doctor),
):
    """Return per-day appointment counts and revenue for the last N days."""
    db = get_db()

    cache_key = doctor_daily_stats_key(str(doctor["_id"]), days)
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    now = utc_now()
    window_start = now - timedelta(days=days)

    # Daily appointment counts grouped by date
    appt_pipeline = [
        {
            "$match": {
                "doctor_id": doctor["_id"],
                "scheduled_at": {"$gte": window_start},
                "status": {"$in": VISIBLE_TO_DOCTOR_STATUSES},
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$scheduled_at", "timezone": "Asia/Kolkata"}
                },
                "total": {"$sum": 1},
                "completed": {"$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}},
                "cancelled": {"$sum": {"$cond": [{"$eq": ["$status", "cancelled"]}, 1, 0]}},
                "no_show": {"$sum": {"$cond": [{"$eq": ["$status", "no_show"]}, 1, 0]}},
                "confirmed": {"$sum": {"$cond": [{"$eq": ["$status", "confirmed"]}, 1, 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Daily revenue
    rev_pipeline = [
        {
            "$match": {
                "doctor_id": doctor["_id"],
                "payment_status": "paid",
                "scheduled_at": {"$gte": window_start},
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$scheduled_at", "timezone": "Asia/Kolkata"}
                },
                "revenue": {"$sum": {"$ifNull": ["$consultation_fee", 0]}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Patient age distribution
    age_pipeline = [
        {
            "$match": {
                "doctor_id": doctor["_id"],
                "scheduled_at": {"$gte": window_start},
                "status": {"$in": ["confirmed", "completed"]},
            }
        },
        {
            "$lookup": {
                "from": "patients",
                "localField": "patient_user_id",
                "foreignField": "user_id",
                "as": "patient_doc",
            }
        },
        {"$unwind": {"path": "$patient_doc", "preserveNullAndEmptyArrays": True}},
        {
            "$addFields": {
                "age": {
                    "$cond": {
                        "if": {"$gt": [{"$ifNull": ["$patient_doc.dob", None]}, None]},
                        "then": {
                            "$divide": [
                                {"$subtract": [now, "$patient_doc.dob"]},
                                365.25 * 24 * 60 * 60 * 1000,
                            ]
                        },
                        "else": None,
                    }
                }
            }
        },
        {
            "$bucket": {
                "groupBy": "$age",
                "boundaries": [0, 16, 21, 30, 46, 61, 120],
                "default": "unknown",
                "output": {"count": {"$sum": 1}},
            }
        },
    ]

    appt_data = await db.appointments.aggregate(appt_pipeline).to_list(length=days + 1)
    rev_data = await db.appointments.aggregate(rev_pipeline).to_list(length=days + 1)
    age_data = await db.appointments.aggregate(age_pipeline).to_list(length=10)

    # Map age buckets to labels
    age_labels = {0: "0-15", 16: "16-20", 21: "21-29", 30: "30-45", 46: "46-60", 61: "61+"}
    age_distribution = [
        {"range": age_labels.get(b["_id"], str(b["_id"])), "count": b["count"]}
        for b in age_data
        if b["_id"] != "unknown"
    ]

    response = {
        "days": days,
        "generated_at": now.isoformat(),
        "daily_appointments": [
            {"date": d["_id"], "total": d["total"], "completed": d["completed"],
             "cancelled": d["cancelled"], "no_show": d["no_show"], "confirmed": d["confirmed"]}
            for d in appt_data
        ],
        "daily_revenue": [
            {"date": d["_id"], "revenue": int(d["revenue"])}
            for d in rev_data
        ],
        "age_distribution": age_distribution,
    }
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


def _iso_utc(dt):
    dt = ensure_utc(dt)
    return dt.isoformat() if dt else None


def _review_out(review: dict | None) -> dict | None:
    """Serialize the embedded review sub-document."""
    if not review:
        return None
    created_at = ensure_utc(review.get("created_at"))
    return {
        "rating": review.get("rating"),
        "comment": review.get("comment"),
        "created_at": created_at.isoformat() if created_at else None,
    }


def _followup_fields(a: dict) -> dict:
    follow_up_eligible_until = ensure_utc(a.get("follow_up_eligible_until"))
    return {
        "appointment_type": a.get("appointment_type", "new"),
        "follow_up_of_appointment_id": str(a["follow_up_of_appointment_id"])
        if a.get("follow_up_of_appointment_id")
        else None,
        "is_follow_up_eligible": a.get("is_follow_up_eligible", False),
        "follow_up_eligible_until": follow_up_eligible_until.isoformat() if follow_up_eligible_until else None,
        "follow_up_used": a.get("follow_up_used", False),
    }


async def _build_prescription_status_map(db, appointment_ids: list[ObjectId]) -> dict[ObjectId, str]:
    if not appointment_ids:
        return {}

    docs = await db.prescriptions.find(
        {"appointment_id": {"$in": appointment_ids}},
        {"appointment_id": 1, "is_draft": 1},
    ).to_list(length=len(appointment_ids))

    return {
        doc["appointment_id"]: "draft" if doc.get("is_draft", True) else "final"
        for doc in docs
    }


async def _serialize_doctor_appointment(db, a: dict, prescription_status_map: dict[ObjectId, str] | None = None) -> dict:
    patient_id = a.get("patient_id")
    patient = await db.patients.find_one({"_id": patient_id}) if patient_id else None
    prescription_status = (prescription_status_map or {}).get(a["_id"], "none")

    return {
        "appointment_id": str(a["_id"]),
        "scheduled_at": _iso_utc(a.get("scheduled_at")),
        "duration_min": a.get("duration_min"),
        "mode": a.get("mode"),
        "fee": a.get("consultation_fee"),
        "payment_choice": a.get("payment_choice"),
        "payment_status": a.get("payment_status", "unpaid"),
        "refund_status": a.get("refund_status", "none"),
        "status": a.get("status"),
        "prescription_status": prescription_status,
        "video_enabled": a.get("video_enabled", False),
        "call_status": a.get("call_status", "idle"),
        "confirmed_at": _iso_utc(a.get("confirmed_at")),
        "cancel_reason": a.get("cancel_reason"),
        "cancelled_at": _iso_utc(a.get("cancelled_at")),
        "cancelled_by": a.get("cancelled_by"),
        "cancelled_by_id": str(a.get("cancelled_by_id")) if a.get("cancelled_by_id") else None,
        "completed_at": _iso_utc(a.get("completed_at")),
        "no_show_at": _iso_utc(a.get("no_show_at")),
        "rescheduled_at": _iso_utc(a.get("rescheduled_at")),
        "rescheduled_from": str(a.get("rescheduled_from")) if a.get("rescheduled_from") else None,
        "created_at": _iso_utc(a.get("created_at")),
        **_followup_fields(a),
        "review": _review_out(a.get("review")),
        "patient": {
            "id": str(patient["_id"]) if patient else None,
            "full_name": patient.get("full_name") if patient else None,
            "age": patient.get("age") if patient else None,
            "sex": patient.get("sex") if patient else None,
            "email": patient.get("email") if patient else None,
            "phone": patient.get("phone") if patient else None,
        },
    }


@router.get(
    "/appointments",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def get_appointments(
    day: str = Query(..., description="YYYY-MM-DD"),
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    cache_key = doctor_appointments_key(str(doctor["_id"]), day)
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    avail = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]})
    avail_tz = avail.get("timezone", "Asia/Kolkata") if avail else "Asia/Kolkata"

    try:
        start_utc, end_utc = day_window_to_utc(day, tz_name=avail_tz)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD")

    appointments = (
        await db.appointments.find(
            {
                "doctor_id": doctor["_id"],
                "scheduled_at": {"$gte": start_utc, "$lt": end_utc},
                "status": {"$in": VISIBLE_TO_DOCTOR_STATUSES},
            }
        )
        .sort("scheduled_at", 1)
        .to_list(length=1000)
    )

    prescription_status_map = await _build_prescription_status_map(db, [a["_id"] for a in appointments])
    results = [await _serialize_doctor_appointment(db, a, prescription_status_map) for a in appointments]

    response = {
        "doctor_id": str(doctor["_id"]),
        "day": day,
        "appointments": results,
    }
    await cache_set_json(cache_key, response, TTL_2_MINUTES)
    return response


@router.get(
    "/appointments/range",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def get_appointments_range(
    from_date: str = Query(..., alias="from", description="YYYY-MM-DD"),
    to_date: str = Query(..., alias="to", description="YYYY-MM-DD"),
    patient_id: str | None = Query(None, description="Filter by patient record id"),
    doctor=Depends(get_current_doctor),
):
    db = get_db()

    avail = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]})
    avail_tz = avail.get("timezone", "Asia/Kolkata") if avail else "Asia/Kolkata"

    try:
        start_utc, _ = day_window_to_utc(from_date, tz_name=avail_tz)
        _, end_utc = day_window_to_utc(to_date, tz_name=avail_tz)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD")

    if end_utc < start_utc:
        raise HTTPException(status_code=400, detail="to must be on or after from")

    # Allow wider range when filtering by patient (history lookup)
    max_days = 2500 if patient_id else 40
    if end_utc - start_utc > timedelta(days=max_days):
        raise HTTPException(status_code=400, detail=f"Date range cannot exceed {max_days} days")

    query: dict = {
        "doctor_id": doctor["_id"],
        "scheduled_at": {"$gte": start_utc, "$lt": end_utc},
        "status": {"$in": VISIBLE_TO_DOCTOR_STATUSES},
    }
    if patient_id:
        query["patient_id"] = _oid(patient_id)

    appointments = (
        await db.appointments.find(query)
        .sort("scheduled_at", 1)
        .to_list(length=5000)
    )

    prescription_status_map = await _build_prescription_status_map(db, [a["_id"] for a in appointments])

    return {
        "doctor_id": str(doctor["_id"]),
        "from": from_date,
        "to": to_date,
        "appointments": [await _serialize_doctor_appointment(db, a, prescription_status_map) for a in appointments],
    }


@router.get(
    "/notifications",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def get_doctor_notifications(
    limit: int = Query(25, ge=1, le=100),
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    cleared_before = ensure_utc(doctor.get("notifications_cleared_at"))

    docs = await db.appointments.find(
        {"doctor_id": doctor["_id"]},
        {
            "_id": 1,
            "patient_name": 1,
            "scheduled_at": 1,
            "status": 1,
            "created_at": 1,
            "confirmed_at": 1,
            "cancelled_at": 1,
            "cancel_reason": 1,
            "rescheduled_at": 1,
            "rescheduled_from": 1,
            "payment_status": 1,
        },
    ).sort("updated_at", -1).to_list(length=200)

    items: list[dict] = []
    for doc in docs:
        scheduled_at = _iso_utc(doc.get("scheduled_at"))
        patient_name = doc.get("patient_name") or "Patient"

        if doc.get("rescheduled_from") and doc.get("rescheduled_at"):
            event_at = ensure_utc(doc.get("rescheduled_at"))
            if cleared_before and event_at and event_at <= cleared_before:
                continue
            items.append(
                {
                    "id": f"rescheduled:{doc['_id']}",
                    "type": "rescheduled",
                    "appointment_id": str(doc["_id"]),
                    "patient_name": patient_name,
                    "scheduled_at": scheduled_at,
                    "event_at": _iso_utc(event_at),
                    "title": "Appointment rescheduled",
                    "message": f"{patient_name} rescheduled an appointment.",
                }
            )
            continue

        if doc.get("status") == "cancelled" and doc.get("cancelled_at") and doc.get("cancel_reason") != "rescheduled":
            event_at = ensure_utc(doc.get("cancelled_at"))
            if cleared_before and event_at and event_at <= cleared_before:
                continue
            items.append(
                {
                    "id": f"cancelled:{doc['_id']}",
                    "type": "cancelled",
                    "appointment_id": str(doc["_id"]),
                    "patient_name": patient_name,
                    "scheduled_at": scheduled_at,
                    "event_at": _iso_utc(event_at),
                    "title": "Appointment cancelled",
                    "message": f"{patient_name} cancelled an appointment.",
                }
            )
            continue

        if doc.get("status") == "confirmed":
            event_dt = ensure_utc(doc.get("confirmed_at")) or ensure_utc(doc.get("created_at"))
            if cleared_before and event_dt and event_dt <= cleared_before:
                continue
            event_at = _iso_utc(event_dt)
            items.append(
                {
                    "id": f"booked:{doc['_id']}",
                    "type": "booked",
                    "appointment_id": str(doc["_id"]),
                    "patient_name": patient_name,
                    "scheduled_at": scheduled_at,
                    "event_at": event_at,
                    "title": "New appointment booked",
                    "message": f"{patient_name} booked an appointment.",
                }
            )

    items.sort(key=lambda item: item.get("event_at") or "", reverse=True)
    return {"items": items[:limit]}


@router.post(
    "/notifications/mark-all-read",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def mark_all_doctor_notifications_read(doctor=Depends(get_current_doctor)):
    db = get_db()
    cleared_at = utc_now()

    await db.doctors.update_one(
        {"_id": doctor["_id"]},
        {"$set": {"notifications_cleared_at": cleared_at}},
    )

    return {"ok": True, "cleared_at": _iso_utc(cleared_at)}


@router.get(
    "/appointments/{appointment_id}",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def get_appointment_detail(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one({"_id": appt_oid, "doctor_id": doctor["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    if appt.get("status") == "pending_payment":
        raise HTTPException(status_code=404, detail="Appointment not found")

    patient_id = appt.get("patient_id")
    patient = await db.patients.find_one({"_id": patient_id}) if patient_id else None
    prescription = await db.prescriptions.find_one({"appointment_id": appt["_id"]}, {"is_draft": 1})
    doc = await db.doctors.find_one(
        {"_id": doctor["_id"]},
        {"password_hash": 0, "totp_secret": 0, "refresh_token_hash": 0},
    )

    return {
        "appointment_id": str(appt["_id"]),
        "doctor_id": str(appt["doctor_id"]),
        "scheduled_at": _iso_utc(appt.get("scheduled_at")),
        "duration_min": appt.get("duration_min"),
        "mode": appt.get("mode"),
        "fee": appt.get("consultation_fee"),
        "payment_choice": appt.get("payment_choice"),
        "payment_status": appt.get("payment_status", "unpaid"),
        "refund_status": appt.get("refund_status", "none"),
        "prescription_status": "draft" if prescription and prescription.get("is_draft", True) else "final" if prescription else "none",
        "video_enabled": appt.get("video_enabled", False),
        "call_status": appt.get("call_status", "idle"),
        "call_participant_count": appt.get("call_participant_count", 0),
        "call_participants": [
            {"role": p.get("role"), "identity": p.get("identity")}
            for p in [appt.get("patient_participant"), appt.get("doctor_participant")] if p
        ],
        "video_room": appt.get("video_room"),
        "confirmed_at": _iso_utc(appt.get("confirmed_at")),
        "status": appt.get("status"),
        "cancel_reason": appt.get("cancel_reason"),
        "cancelled_at": _iso_utc(appt.get("cancelled_at")),
        "cancelled_by": appt.get("cancelled_by"),
        "cancelled_by_id": str(appt.get("cancelled_by_id")) if appt.get("cancelled_by_id") else None,
        "completed_at": _iso_utc(appt.get("completed_at")),
        "no_show_at": _iso_utc(appt.get("no_show_at")),
        "rescheduled_at": _iso_utc(appt.get("rescheduled_at")),
        "rescheduled_from": str(appt.get("rescheduled_from")) if appt.get("rescheduled_from") else None,
        **_followup_fields(appt),
        "patient": {
            "id": str(patient["_id"]) if patient else None,
            "full_name": patient.get("full_name") if patient else None,
            "age": patient.get("age") if patient else None,
            "sex": patient.get("sex") if patient else None,
            "email": patient.get("email") if patient else None,
            "phone": patient.get("phone") if patient else None,
            "notes": patient.get("notes") if patient else None,
        },
        # Patient-submitted notes on this appointment
        "patient_notes": appt.get("patient_notes"),
        # Patient review (read-only for doctor)
        "review": _review_out(appt.get("review")),
        "doctor": {
            "id": str(doc["_id"]) if doc else None,
            "full_name": doc.get("full_name") if doc else None,
            "specialization": doc.get("specialization") if doc else None,
            "clinic_name": clinic_profile_fields()["clinic_name"],
            "city": clinic_profile_fields()["city"],
        },
        "created_at": _iso_utc(appt.get("created_at")),
        "updated_at": _iso_utc(appt.get("updated_at")),
    }


@router.post(
    "/appointments/{appointment_id}/video-token",
    dependencies=[rl(settings.RL_DOCTOR_VIDEO_JOIN_TIMES, settings.RL_DOCTOR_VIDEO_JOIN_SECONDS)],
)
async def doctor_video_token(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    import logging
    import uuid
    from app.utils.video import check_join_window
    logger = logging.getLogger(__name__)

    """Generate a LiveKit token for the doctor.

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

    appt = await db.appointments.find_one({"_id": appt_oid, "doctor_id": doctor["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    if appt.get("mode") != "online" or not appt.get("video_enabled", False):
        raise HTTPException(status_code=403, detail="Video is not enabled for this appointment")

    if appt.get("status") != "confirmed":
        raise HTTPException(status_code=409, detail="Invalid appointment state")
        
    if appt.get("call_status") == "ended":
        raise HTTPException(status_code=409, detail="Call already ended")

    check_video_payment(appt, role="doctor")
    check_join_window(appt, now, role="doctor")

    # Participant limits
    call_status = appt.get("call_status", "idle")
    count = appt.get("call_participant_count", 0)
    if call_status in ["waiting", "connected"]:
        if count >= 2:
            logger.warning("participant_limit_warning: count>=2 appointment_id=%s role=doctor", appointment_id)
        if count >= 3:
            logger.warning("participant_limit_breach: count>=3 appointment_id=%s role=doctor", appointment_id)
            raise HTTPException(status_code=409, detail="Too many participants in the room")

    # Token replay protection (burst limit)
    from app.utils.time import ensure_utc
    last_issued = appt.get("doctor_last_token_issued_at")
    if last_issued and (now - ensure_utc(last_issued)).total_seconds() < 2:
        logger.warning("token_replay_burst: appointment_id=%s role=doctor", appointment_id)

    # Hard guarantee room reuse
    if not appt.get("video_room"):
        room = await ensure_video_room(db, appt)
    else:
        room = appt["video_room"]

    # Record doctor_joined_at for analytics (does not change call state)
    await db.appointments.update_one(
        {"_id": appt_oid},
        {"$set": {"doctor_joined_at": now, "doctor_last_token_issued_at": now, "updated_at": now}},
    )

    trace_id = uuid.uuid4().hex
    identity = f"doctor:{str(doctor['_id'])}"
    logger.info("video_token_issued", extra={"appointment_id": appointment_id, "role": "doctor", "identity": identity, "trace_id": trace_id})

    try:
        join_token = create_video_token(
            room=room,
            identity=identity,
            name=doctor.get("full_name") or "Doctor",
            metadata={"appointment_id": str(appt["_id"]), "role": "doctor", "trace_id": trace_id},
            ttl_seconds=7200,
        )
    except Exception as e:
        logger.error("livekit_token_generation_failed", extra={"appointment_id": appointment_id, "role": "doctor", "error": str(e), "trace_id": trace_id})
        raise HTTPException(status_code=500, detail="Failed to generate video token")

    return {
        "provider": "livekit",
        "server_url": settings.LIVEKIT_URL,
        "room": room,
        "token": join_token,
    }


@router.get(
    "/calls/waiting",
    dependencies=[rl(settings.RL_DOCTOR_WAITING_TIMES, settings.RL_DOCTOR_WAITING_SECONDS)],
)
async def doctor_waiting_calls(doctor=Depends(get_current_doctor)):
    """Legacy endpoint — returns waiting patients from heartbeat-based presence."""
    if not settings.VIDEO_ENABLED:
        return {"waiting": []}

    from app.services.call_state_machine import get_waiting_patients
    waiting = await get_waiting_patients(str(doctor["_id"]))
    return {"waiting": waiting}


@router.get(
    "/calls/dashboard",
    dependencies=[rl(settings.RL_DOCTOR_WAITING_TIMES, settings.RL_DOCTOR_WAITING_SECONDS)],
)
async def doctor_calls_dashboard(
    day: str = Query(None, description="YYYY-MM-DD, defaults to today"),
    doctor=Depends(get_current_doctor),
):
    """Single endpoint for the entire calls dashboard.

    Returns categorized appointments:
      - waiting: patients in pre-call waiting room
      - active: connected calls (2+ participants)
      - disconnected: calls where someone disconnected (within timeout)
      - scheduled: today's confirmed video appointments not yet started
    """
    if not settings.VIDEO_ENABLED:
        return {
            "doctor_id": str(doctor["_id"]),
            "day": day,
            "waiting": [],
            "active": [],
            "disconnected": [],
            "scheduled": [],
            "counts": {"waiting": 0, "active": 0, "disconnected": 0, "scheduled": 0},
        }

    if not day:
        day = utc_now().date().isoformat()

    return await get_calls_dashboard(str(doctor["_id"]), day)


@router.post(
    "/appointments/{appointment_id}/call/end",
    dependencies=[rl(settings.RL_DOCTOR_VIDEO_END_TIMES, settings.RL_DOCTOR_VIDEO_END_SECONDS)],
)
async def doctor_end_call(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    """End a call via the state machine. Notifies all participants."""
    if not settings.VIDEO_ENABLED:
        raise HTTPException(status_code=503, detail="Video is disabled")

    try:
        result = await handle_manual_end(appointment_id, str(doctor["_id"]))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    db = get_db()
    appt = await db.appointments.find_one(
        {"_id": _oid(appointment_id), "doctor_id": doctor["_id"]},
        {"patient_user_id": 1},
    )
    if appt and appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))

    return result
