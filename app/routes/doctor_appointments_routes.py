import logging
from datetime import datetime, timedelta

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

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
    doctor_appointments_range_key,
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
from app.utils.appointment_serializers import _review_out
from app.utils.clinic import clinic_profile_fields
from app.utils.time import day_window_to_utc, ensure_utc, utc_now

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/doctor", tags=["Doctor Appointments"])


VISIBLE_TO_DOCTOR_STATUSES = [
    "confirmed",
    "completed",
    "cancelled",
    "no_show",
    "pending_payment",
]
from app.utils.mongo_utils import _oid


def _iso_utc(dt: datetime | str | None) -> str | None:
    if not dt:
        return None
    return ensure_utc(dt).isoformat()


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

    pipeline = [
        {"$match": {"doctor_id": doctor["_id"]}},
        {
            "$facet": {
                "total_appointments": [{"$count": "count"}],
                "today_appointments": [
                    {"$match": {"scheduled_at": {"$gte": day_start_utc, "$lt": day_end_utc}}},
                    {"$count": "count"}
                ],
                "upcoming_7d_confirmed": [
                    {"$match": {"status": "confirmed", "scheduled_at": {"$gte": now, "$lt": next_7_days}}},
                    {"$count": "count"}
                ],
                "status_counts_30d": [
                    {"$match": {"scheduled_at": {"$gte": window_start}}},
                    {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                ],
                "paid_revenue_30d": [
                    {"$match": {"payment_status": "paid", "scheduled_at": {"$gte": window_start}}},
                    {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$consultation_fee", 0]}}}}
                ]
            }
        }
    ]

    docs = await db.appointments.aggregate(pipeline).to_list(length=1)
    res = docs[0] if docs else {}

    total_appointments = res.get("total_appointments", [{"count": 0}])[0]["count"] if res.get("total_appointments") else 0
    today_appointments = res.get("today_appointments", [{"count": 0}])[0]["count"] if res.get("today_appointments") else 0
    upcoming_7d_confirmed = res.get("upcoming_7d_confirmed", [{"count": 0}])[0]["count"] if res.get("upcoming_7d_confirmed") else 0

    status_counts = {
        "confirmed": 0, "completed": 0, "cancelled": 0, "no_show": 0, "pending_payment": 0
    }
    for item in res.get("status_counts_30d", []):
        if item["_id"] in status_counts:
            status_counts[item["_id"]] = item["count"]

    paid_amount = int(res.get("paid_revenue_30d", [{"total": 0}])[0]["total"]) if res.get("paid_revenue_30d") else 0

    response = {
        "doctor_id": str(doctor["_id"]),
        "window_days": 30,
        "generated_at": now.isoformat(),
        "today_appointments": today_appointments,
        "upcoming_7d_confirmed": upcoming_7d_confirmed,
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

    import asyncio
    appt_data, rev_data, age_data = await asyncio.gather(
        db.appointments.aggregate(appt_pipeline).to_list(length=days + 1),
        db.appointments.aggregate(rev_pipeline).to_list(length=days + 1),
        db.appointments.aggregate(age_pipeline).to_list(length=10),
    )

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


async def _build_patient_map(db, appointments: list[dict]) -> dict[ObjectId, dict]:
    patient_ids = list({a["patient_id"] for a in appointments if a.get("patient_id")})
    if not patient_ids:
        return {}
    patients = await db.patients.find({"_id": {"$in": patient_ids}}).to_list(length=len(patient_ids))
    return {p["_id"]: p for p in patients}

def _serialize_doctor_appointment(
    a: dict,
    prescription_status_map: dict[ObjectId, str] | None = None,
    patient_map: dict[ObjectId, dict] | None = None
) -> dict:
    patient_id = a.get("patient_id")
    patient = patient_map.get(patient_id) if patient_map is not None and patient_id else None
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
            "id": str(patient["_id"]) if patient else (str(a["patient_user_id"]) if a.get("patient_user_id") else None),
            "full_name": patient.get("full_name") if patient else a.get("patient_name"),
            "age": patient.get("age") if patient else a.get("patient_age"),
            "sex": patient.get("sex") if patient else a.get("patient_sex"),
            "email": patient.get("email") if patient else a.get("patient_email"),
            "phone": patient.get("phone") if patient else a.get("patient_phone"),
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
    patient_map = await _build_patient_map(db, appointments)
    results = [_serialize_doctor_appointment(a, prescription_status_map, patient_map) for a in appointments]

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
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    
    cache_key = doctor_appointments_range_key(
        str(doctor["_id"]),
        from_date=from_date,
        to_date=to_date,
        patient_id=patient_id,
        limit=limit,
        skip=skip,
    )
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

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
    range_days = (end_utc - start_utc).days
    if range_days > max_days:
        raise HTTPException(status_code=400, detail=f"Date range cannot exceed {max_days} days")
    if range_days > 365:
        logger.warning(
            "get_appointments_range: Large date range request: %d days (doctor_id=%s, from=%s, to=%s)",
            range_days, doctor["_id"], from_date, to_date,
        )

    query: dict = {
        "doctor_id": doctor["_id"],
        "scheduled_at": {"$gte": start_utc, "$lt": end_utc},
        "status": {"$in": VISIBLE_TO_DOCTOR_STATUSES},
    }
    if patient_id:
        query["patient_id"] = _oid(patient_id)
        
    total_count_task = db.appointments.count_documents(query)
    appointments_task = (
        db.appointments.find(query)
        .sort("scheduled_at", 1)
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )
    
    import asyncio
    total, appointments = await asyncio.gather(total_count_task, appointments_task)

    prescription_status_map = await _build_prescription_status_map(db, [a["_id"] for a in appointments])
    patient_map = await _build_patient_map(db, appointments)

    response = {
        "doctor_id": str(doctor["_id"]),
        "from": from_date,
        "to": to_date,
        "total": total,
        "limit": limit,
        "skip": skip,
        "appointments": [_serialize_doctor_appointment(a, prescription_status_map, patient_map) for a in appointments],
    }
    await cache_set_json(cache_key, response, TTL_2_MINUTES)
    return response


@router.get(
    "/notifications",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def get_doctor_notifications(
    limit: int = Query(25, ge=1, le=100),
    since: str | None = Query(None, description="ISO format date to filter from"),
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    cleared_before = ensure_utc(doctor.get("notifications_cleared_at"))

    query: dict = {"doctor_id": doctor["_id"]}
    if since:
        try:
            from datetime import datetime
            since_dt = ensure_utc(datetime.fromisoformat(since))
            if since_dt:
                query["updated_at"] = {"$gte": since_dt}
        except ValueError:
            pass
    else:
        # Default to 30 days to avoid full collection scan
        from datetime import timedelta
        from app.utils.time import utc_now
        query["updated_at"] = {"$gte": utc_now() - timedelta(days=30)}

    docs = await db.appointments.find(
        query,
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
        
        events = []
        if doc.get("status") == "cancelled" and doc.get("cancelled_at") and doc.get("cancel_reason") != "rescheduled":
            events.append({
                "type": "cancelled",
                "event_dt": ensure_utc(doc.get("cancelled_at")),
                "title": "Appointment cancelled",
                "message": f"{patient_name} cancelled an appointment."
            })
            
        if doc.get("rescheduled_from") and doc.get("rescheduled_at"):
            events.append({
                "type": "rescheduled",
                "event_dt": ensure_utc(doc.get("rescheduled_at")),
                "title": "Appointment rescheduled",
                "message": f"{patient_name} rescheduled an appointment."
            })
            
        if doc.get("status") in ("confirmed", "completed", "cancelled", "no_show"):
            booked_dt = ensure_utc(doc.get("confirmed_at")) or ensure_utc(doc.get("created_at"))
            events.append({
                "type": "booked",
                "event_dt": booked_dt,
                "title": "New appointment booked",
                "message": f"{patient_name} booked an appointment."
            })

        valid_events = [e for e in events if e.get("event_dt")]
        if not valid_events:
            continue
            
        for event in valid_events:
            if cleared_before and event["event_dt"] <= cleared_before:
                continue
                
            items.append({
                "id": f"{event['type']}:{doc['_id']}",
                "type": event['type'],
                "appointment_id": str(doc["_id"]),
                "patient_name": patient_name,
                "scheduled_at": scheduled_at,
                "event_at": _iso_utc(event["event_dt"]),
                "title": event["title"],
                "message": event["message"],
            })

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

    clinic_profile = clinic_profile_fields()

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
            "id": str(patient["_id"]) if patient else (str(appt["patient_user_id"]) if appt.get("patient_user_id") else None),
            "full_name": patient.get("full_name") if patient else appt.get("patient_name"),
            "age": patient.get("age") if patient else appt.get("patient_age"),
            "sex": patient.get("sex") if patient else appt.get("patient_sex"),
            "email": patient.get("email") if patient else appt.get("patient_email"),
            "phone": patient.get("phone") if patient else appt.get("patient_phone"),
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
            "clinic_name": clinic_profile.get("clinic_name"),
            "city": clinic_profile.get("city"),
        },
        "created_at": _iso_utc(appt.get("created_at")),
        "updated_at": _iso_utc(appt.get("updated_at")), # codeql[py/clear-text-logging-sensitive-data]
    }
 # codeql[py/clear-text-logging-sensitive-data]

from pydantic import BaseModel

class VideoTokenRequest(BaseModel):
    recovery_reason: str | None = None
    session_id: str | None = None

@router.post(
    "/appointments/{appointment_id}/video-token",
    dependencies=[rl(settings.RL_DOCTOR_VIDEO_JOIN_TIMES, settings.RL_DOCTOR_VIDEO_JOIN_SECONDS)],
)
async def doctor_video_token(
    appointment_id: str,
    payload: VideoTokenRequest = None,
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

    from app.services.call_state_machine import reconcile_call_state
    await reconcile_call_state(appointment_id)
    from pymongo import ReadPreference
    from pymongo.read_concern import ReadConcern
    appointments_col = db.appointments.with_options(
        read_preference=ReadPreference.PRIMARY,
        read_concern=ReadConcern(level="majority")
    )

    appt = await appointments_col.find_one({"_id": appt_oid, "doctor_id": doctor["_id"]})
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

    # Log recovery reason if present
    recovery_reason = payload.recovery_reason if payload else None
    if recovery_reason:
        logger.info(
            "doctor_reconnecting_recovery_reason",
            extra={"appointment_id": appointment_id, "recovery_reason": recovery_reason}
        )

    # Record doctor_joined_at for analytics (does not change call state)
    from pymongo import ReturnDocument
    update_set = {
        "doctor_last_token_issued_at": now,
        "updated_at": now,
    }
    if not appt.get("doctor_joined_at"):
        update_set["doctor_joined_at"] = now

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
        leader_key = RedisKeys.call_leader(appointment_id, "doctor")
        epoch_key = RedisKeys.epoch_key(appointment_id, "doctor")
        try:
            result = await redis.eval(
                LUA_ACQUIRE_LOCK,
                [join_lock_key, call_version_key, leader_key, epoch_key],
                [str(session_version), token_id, session_id]
            )
            if result and result[0] == -1:
                logger.warning("doctor_token_request_failed_version_mismatch: current=%s expected=%s", result[1], session_version)
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
        active_token_key = RedisKeys.active_token(appointment_id, "doctor")
        prev_token_key = RedisKeys.prev_token(appointment_id, "doctor")
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
    identity = f"doctor:{str(doctor['_id'])}"
    logger.info("video_token_issued", extra={"appointment_id": appointment_id, "role": "doctor", "identity": identity, "trace_id": trace_id, "session_version": session_version, "session_id": session_id, "epoch": epoch})

    try:
        join_token = create_video_token(
            room=room,
            identity=identity,
            name=doctor.get("full_name") or "Doctor",
            metadata={
                "appointment_id": str(appt["_id"]),
                "role": "doctor",
                "trace_id": trace_id,
                "session_version": session_version,
                "session_id": session_id,
                "epoch": epoch,
                "token_id": token_id
            },
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
        "session_version": session_version,
        "session_id": session_id,
        "epoch": epoch
    }


@router.get("/appointments/{appointment_id}/reconcile")
async def doctor_reconcile_call(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    """Exposes call state reconciliation to resolve potential webhook/state drift."""
    db = get_db()
    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid, "doctor_id": doctor["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

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


class CallHeartbeatRequest(BaseModel):
    session_version: int | None = None
    session_id: str | None = None
    epoch: int | None = None
    seq: int | None = None
    token_id: str | None = None
    sent_at: float | None = None
    rtt: float | None = None

@router.post("/appointments/{appointment_id}/call/heartbeat")
async def doctor_call_heartbeat(
    appointment_id: str,
    payload: CallHeartbeatRequest = None,
    doctor=Depends(get_current_doctor),
):
    """Periodic active call heartbeat from doctor to track presence."""
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
            db_call_status = appt.get("call_status", "idle")    # Sequence/Rate check early (Rate-Limit duplicate heartbeat packets within 500ms - return cached response)
    last_ts_key = RedisKeys.last_ts_key(appointment_id, "doctor")
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
    import time
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
            "doctor_heartbeat_session_version_mismatch: db=%s incoming=%s appt_id=%s",
            db_version, incoming_version, appointment_id
        )
        raise HTTPException(status_code=409, detail="Outdated session version")

    # 7. Epoch Fencing & Timeout Check
    leader_key = RedisKeys.call_leader(appointment_id)
    terminate = False
    terminate_reason = "none"
    leader_epoch = epoch if epoch is not None else 1
    is_zombie = False
    media_policy = "normal"

    if db_call_status == "ended":
        logger.warning("doctor_heartbeat_call_already_ended: appt_id=%s", appointment_id)
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
                        if appt and appt.get("video_room"):
                            await handle_room_finished(appt["video_room"])
                        terminate = True
                        terminate_reason = "hard_timeout"
            except Exception as exc:
                logger.warning("Failed to check hard timeout on heartbeat: %s", str(exc))

        # 7c. Heartbeat Silence Timeout Check with degraded grace tiers
        last_seen_key = RedisKeys.last_seen_key(appointment_id, "doctor")
        if not terminate and token_id:
            try:
                last_seen_str = await redis.get_str(last_seen_key)
                if last_seen_str:
                    last_seen = float(last_seen_str)
                    effective_rtt = min(client_rtt or 0.0, 2.0)
                    deadline = max(5.0, 3.0 * effective_rtt)
                    silence_duration = max(0.0, time.time() - last_seen)
                    if silence_duration > 4 * deadline:
                        logger.warning("doctor_heartbeat_silence_exceeded: silence=%fs limit=%fs appt_id=%s", silence_duration, 4 * deadline, appointment_id)
                        terminate = True
                        terminate_reason = "silence_timeout"
                    elif silence_duration > 2 * deadline:
                        logger.warning("doctor_heartbeat_silence_degraded: silence=%fs limit=%fs appt_id=%s - restricting media", silence_duration, 2 * deadline, appointment_id)
                        media_policy = "restricted"
            except Exception as exc:
                logger.warning("Failed to check heartbeat silence limit: %s", str(exc))

        # 7d. Atomic Lease Refresh and Recovery (Lua)
        if not terminate and token_id and session_id and epoch is not None:
            try:
                active_token_key = RedisKeys.active_token(appointment_id, "doctor")
                epoch_key = RedisKeys.epoch_key(appointment_id, "doctor")
                kill_switch_key = RedisKeys.kill_switch(appointment_id)
                leader_key = RedisKeys.call_leader(appointment_id, "doctor")
                
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
                        logger.warning("doctor_heartbeat_kill_switch_triggered_lua: appt_id=%s", appointment_id)
                        terminate = True
                        terminate_reason = "kill_switch_lua"
                    elif status == -3:
                        prev_token_key = RedisKeys.prev_token(appointment_id, "doctor")
                        prev_token_str = await redis.get_str(prev_token_key)
                        if token_id == prev_token_str:
                            is_zombie = True
                            media_policy = "none"
                            logger.info("doctor_heartbeat_zombie_isolated: token_id=%s appt_id=%s", token_id, appointment_id)
                        else:
                            logger.warning("doctor_heartbeat_token_fenced_out: expected_active got=%s appt_id=%s", token_id, appointment_id)
                            terminate = True
                            terminate_reason = "token_mismatch"
                    elif status == -2:
                        logger.warning("doctor_heartbeat_stale_epoch: incoming=%s stored_epoch=%s appt_id=%s", epoch, result[1], appointment_id)
                        terminate = True
                        terminate_reason = "stale_epoch"
                    elif status <= 0:
                        logger.warning("doctor_heartbeat_lease_failed_status: status=%s appt_id=%s", status, appointment_id)
                        terminate = True
                        terminate_reason = "lua_reject"
                    else:
                        leader_epoch = int(result[1])
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
            await redis.redis.setex(f"call_participant:{appointment_id}:doctor", 15, "1")
        except Exception as exc:
            logger.warning("Failed to write participant timestamps/liveness lease to Redis: %s", str(exc))

    # 9. Offload side-effects to background task
    from app.worker.tasks.appointment_tasks import process_call_heartbeat
    process_call_heartbeat.apply_async(args=[appointment_id, "doctor", session_id, epoch, db_version, not is_zombie, is_zombie])

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
        "heartbeat", session_id or "", token_id or "", db_version, "doctor", leader_session_id, token_id or ""
    )

    if session_id and seq is not None and authority_mode == "redis":
        try:
            await redis.redis.setex(f"heartbeat_seq:{session_id}", 30, str(seq))
            await redis.redis.setex(f"heartbeat_response:{session_id}", 30, json.dumps(response_data))
        except Exception as exc:
            logger.warning("Failed to cache heartbeat response: %s", str(exc))

    return response_data
