from fastapi import APIRouter, HTTPException, Query
from bson import ObjectId

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.services.cache_service import (
    TTL_5_MINUTES,
    TTL_10_MINUTES,
    cache_get_json,
    cache_set_json,
    doctor_public_profile_key,
    doctors_list_key,
)
from app.utils.clinic import clinic_profile_fields
from app.utils.security_sanitize import escaped_regex, sanitize_search_query, strip_sensitive_fields

router = APIRouter(prefix="/public", tags=["Public Doctors"])

def _public_doctor(doc: dict, slot_duration_min: int | None) -> dict:
    clinic_fields = clinic_profile_fields()
    return {
        "doctor_id": str(doc["_id"]),

        # Identity & Basic Info
        "full_name": doc.get("full_name"),
        "profile_photo": doc.get("profile_photo"),
        "gender": doc.get("gender"),
        "about": doc.get("about"),
        "registration_no": doc.get("registration_no"),

        # Professional Info
        "specialization": doc.get("specialization"),
        "experience_years": doc.get("experience_years"),
        "qualifications": doc.get("qualifications", []),
        "languages": doc.get("languages", []),

        # Clinic Info
        "clinic_name": clinic_fields["clinic_name"],
        "city": clinic_fields["city"],
        "clinic_address": clinic_fields["clinic_address"],
        "clinic_phone": clinic_fields["clinic_phone"],

        # Consultation Info
        "available_modes": doc.get("available_modes", []),   # ["online","walk_in"]
        "online_fee": doc.get("online_fee"),
        "walkin_fee": doc.get("walkin_fee"),
        "slot_duration_min": slot_duration_min,              # from availability
        "verification_status": doc.get("verification_status", "pending"),
    }


@router.get(
    "/doctors",
    dependencies=[rl(settings.RL_PUBLIC_READ_TIMES, settings.RL_PUBLIC_READ_SECONDS)],
)
async def list_public_doctors(
    q: str | None = Query(default=None, description="Search by name/specialization/city/clinic"),
    limit: int = Query(default=20, ge=1, le=100),
    skip: int = Query(default=0, ge=0),
):
    db = get_db()
    q = sanitize_search_query(q)
    list_key = doctors_list_key(q=q, limit=limit, skip=skip)
    cached = await cache_get_json(list_key)
    if isinstance(cached, dict):
        return cached

    # Only list doctors with complete profiles:
    # approved, not suspended, has specialization, available_modes, about, profile_photo, signature
    profile_complete_filter: dict = {
        "verification_status": "approved",
        "is_suspended": {"$ne": True},
        "specialization": {"$nin": [None, ""]},
        "available_modes": {"$exists": True, "$not": {"$size": 0}, "$ne": None},
        "about": {"$nin": [None, ""]},
        "profile_photo": {"$nin": [None, ""]},
        "signature_url": {"$nin": [None, ""]},
    }
    if q:
        profile_complete_filter["$or"] = [
            {"full_name": escaped_regex(q)},
            {"specialization": escaped_regex(q)},
            {"city": escaped_regex(q)},
            {"clinic_name": escaped_regex(q)},
        ]

    pipeline = [
        {"$match": profile_complete_filter},
        # Join availability — only keep doctors who have set their schedule
        {
            "$lookup": {
                "from": "doctor_availability",
                "let": {"doctor_id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$doctor_id", "$$doctor_id"]}}},
                    {"$project": {"_id": 0, "slot_duration_min": 1}},
                    {"$limit": 1},
                ],
                "as": "availability",
            }
        },
        # Filter out doctors without availability set
        {"$match": {"availability": {"$ne": []}}},
        {
            "$addFields": {
                "slot_duration_min": {
                    "$let": {
                        "vars": {"avail": {"$arrayElemAt": ["$availability", 0]}},
                        "in": "$$avail.slot_duration_min",
                    }
                }
            }
        },
        {"$project": {"availability": 0}},
        {"$sort": {"_id": -1}},
        {"$skip": skip},
        {"$limit": limit},
    ]
    doctors = await db.doctors.aggregate(pipeline).to_list(length=limit)

    out = []
    for d in doctors:
        slot_duration = d.get("slot_duration_min")
        slot_duration_min = int(slot_duration) if slot_duration is not None else None
        out.append(_public_doctor(d, slot_duration_min))

    response = strip_sensitive_fields({"count": len(out), "doctors": out})
    await cache_set_json(list_key, response, TTL_5_MINUTES)
    return response


@router.get(
    "/doctors/{doctor_id}",
    dependencies=[rl(settings.RL_PUBLIC_READ_TIMES, settings.RL_PUBLIC_READ_SECONDS)],
)
async def get_public_doctor(doctor_id: str):
    db = get_db()
    profile_key = doctor_public_profile_key(doctor_id)
    cached = await cache_get_json(profile_key)
    if isinstance(cached, dict):
        return cached

    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")

    doc = await db.doctors.find_one({
        "_id": doctor_oid,
        "verification_status": "approved",
        "is_suspended": {"$ne": True},
        "specialization": {"$nin": [None, ""]},
        "available_modes": {"$exists": True, "$not": {"$size": 0}, "$ne": None},
        "about": {"$nin": [None, ""]},
        "profile_photo": {"$nin": [None, ""]},
        "signature_url": {"$nin": [None, ""]},
    })
    if not doc:
        raise HTTPException(status_code=404, detail="Doctor not found")

    avail = await db.doctor_availability.find_one({"doctor_id": doctor_oid}, {"slot_duration_min": 1})
    if not avail:
        raise HTTPException(status_code=404, detail="Doctor not found")
    slot_duration_min = int(avail.get("slot_duration_min")) if avail.get("slot_duration_min") else None

    response = strip_sensitive_fields(_public_doctor(doc, slot_duration_min))
    await cache_set_json(profile_key, response, TTL_10_MINUTES)
    return response
