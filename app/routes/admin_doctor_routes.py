from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.admin_auth_routes import verify_admin_session
from app.services.audit_service import log_audit
from app.services.cache_service import invalidate_doctor_cache
from app.utils.security_sanitize import escaped_regex, sanitize_search_query
from app.utils.time import utc_now

router = APIRouter(prefix="/admin/doctors", tags=["Admin Doctors"])

_SORT_ORDER = {
    "pending": 0,
    "rejected": 1,
    "approved": 2,
}


class RejectDoctorIn(BaseModel):
    rejection_reason: str = Field(..., min_length=3, max_length=300)


def _doctor_summary(doc: dict) -> dict:
    return {
        "doctor_id": str(doc["_id"]),
        "full_name": doc.get("full_name"),
        "email": doc.get("email"),
        "phone": doc.get("phone"),
        "registration_no": doc.get("registration_no"),
        "verification_status": doc.get("verification_status", "pending"),
        "is_suspended": bool(doc.get("is_suspended", False)),
        "is_admin": bool(doc.get("is_admin", False)),
        "rejection_reason": doc.get("rejection_reason"),
        "verified_at": doc.get("verified_at"),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
    }


async def _parse_doctor_or_422(doctor_id: str) -> ObjectId:
    try:
        return ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")


@router.get(
    "",
    dependencies=[rl(settings.RL_ADMIN_READ_TIMES, settings.RL_ADMIN_READ_SECONDS)],
)
async def list_admin_doctors(
    verification_status: str | None = Query(default=None),
    is_suspended: bool | None = Query(default=None),
    search: str | None = Query(default=None, description="Search full_name, email, registration_no"),
    limit: int = Query(default=20, ge=1, le=50),
    skip: int = Query(default=0, ge=0),
    current=Depends(verify_admin_session),
):
    db = get_db()
    filter_q: dict = {"role": "doctor"}
    if verification_status:
        filter_q["verification_status"] = verification_status
    if is_suspended is not None:
        filter_q["is_suspended"] = is_suspended

    cleaned_search = sanitize_search_query(search)
    if cleaned_search:
        filter_q["$or"] = [
            {"full_name": escaped_regex(cleaned_search)},
            {"email": escaped_regex(cleaned_search)},
            {"registration_no": escaped_regex(cleaned_search)},
        ]

    status_branches = [
        {"case": {"$eq": ["$verification_status", status]}, "then": rank}
        for status, rank in _SORT_ORDER.items()
    ]
    pipeline = [
        {"$match": filter_q},
        {
            "$addFields": {
                "verification_status_rank": {
                    "$switch": {
                        "branches": status_branches,
                        "default": 99,
                    }
                }
            }
        },
        {"$sort": {"verification_status_rank": 1, "created_at": -1, "_id": -1}},
        {"$skip": skip},
        {"$limit": limit},
        {
            "$project": {
                "_id": 1,
                "full_name": 1,
                "email": 1,
                "phone": 1,
                "registration_no": 1,
                "verification_status": 1,
                "is_suspended": 1,
                "is_admin": 1,
                "rejection_reason": 1,
                "verified_at": 1,
                "created_at": 1,
                "updated_at": 1,
            }
        },
    ]
    page = await db.doctors.aggregate(pipeline).to_list(length=limit)

    return {
        "count": len(page),
        "doctors": [_doctor_summary(d) for d in page],
    }


@router.get(
    "/{doctor_id}",
    dependencies=[rl(settings.RL_ADMIN_READ_TIMES, settings.RL_ADMIN_READ_SECONDS)],
)
async def get_admin_doctor_detail(
    doctor_id: str,
    current=Depends(verify_admin_session),
):
    db = get_db()
    doctor_oid = await _parse_doctor_or_422(doctor_id)
    doc = await db.doctors.find_one({"_id": doctor_oid, "role": "doctor"})
    if not doc:
        raise HTTPException(status_code=404, detail="Doctor not found")
    return _doctor_summary(doc)


@router.post(
    "/{doctor_id}/approve",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def approve_doctor(
    doctor_id: str,
    request: Request,
    current=Depends(verify_admin_session),
):
    db = get_db()
    now = utc_now()
    doctor_oid = await _parse_doctor_or_422(doctor_id)

    existing = await db.doctors.find_one({"_id": doctor_oid, "role": "doctor"})
    if not existing:
        raise HTTPException(status_code=404, detail="Doctor not found")
    old_status = existing.get("verification_status", "pending")

    result = await db.doctors.update_one(
        {
            "_id": doctor_oid,
            "role": "doctor",
            "verification_status": {"$ne": "approved"},
        },
        {
            "$set": {
                "verification_status": "approved",
                "verified_at": now,
                "verified_by_admin_id": current["_id"],
                "rejection_reason": None,
                "updated_at": now,
            }
        },
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=409, detail="Doctor already approved")

    await log_audit(
        db,
        actor_type="admin",
        actor_id=current["_id"],
        action="approve_doctor",
        target_type="doctor",
        target_id=doctor_oid,
        meta={"old_status": old_status, "new_status": "approved"},
        request=request,
    )
    await invalidate_doctor_cache(str(doctor_oid), invalidate_list=True)
    return {"message": "doctor_approved", "doctor_id": doctor_id, "verification_status": "approved"}


@router.post(
    "/{doctor_id}/reject",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def reject_doctor(
    doctor_id: str,
    payload: RejectDoctorIn,
    request: Request,
    current=Depends(verify_admin_session),
):
    db = get_db()
    now = utc_now()
    doctor_oid = await _parse_doctor_or_422(doctor_id)

    existing = await db.doctors.find_one({"_id": doctor_oid, "role": "doctor"})
    if not existing:
        raise HTTPException(status_code=404, detail="Doctor not found")
    if existing.get("verification_status") == "approved":
        raise HTTPException(
            status_code=409,
            detail="Approved doctors cannot be rejected. Suspend the doctor instead.",
        )
    old_status = existing.get("verification_status", "pending")

    result = await db.doctors.update_one(
        {
            "_id": doctor_oid,
            "role": "doctor",
            "verification_status": "pending",
        },
        {
            "$set": {
                "verification_status": "rejected",
                "verified_at": now,
                "verified_by_admin_id": current["_id"],
                "rejection_reason": payload.rejection_reason.strip(),
                "updated_at": now,
            }
        },
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=409, detail="Only pending doctors can be rejected")

    await log_audit(
        db,
        actor_type="admin",
        actor_id=current["_id"],
        action="reject_doctor",
        target_type="doctor",
        target_id=doctor_oid,
        meta={
            "old_status": old_status,
            "new_status": "rejected",
            "reason": payload.rejection_reason.strip(),
        },
        request=request,
    )
    await invalidate_doctor_cache(str(doctor_oid), invalidate_list=True)
    return {"message": "doctor_rejected", "doctor_id": doctor_id, "verification_status": "rejected"}


@router.post(
    "/{doctor_id}/suspend",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def suspend_doctor(
    doctor_id: str,
    request: Request,
    current=Depends(verify_admin_session),
):
    db = get_db()
    now = utc_now()
    doctor_oid = await _parse_doctor_or_422(doctor_id)
    if doctor_oid == current["_id"]:
        raise HTTPException(status_code=409, detail="You cannot suspend your own account")

    existing = await db.doctors.find_one({"_id": doctor_oid, "role": "doctor"})
    if not existing:
        raise HTTPException(status_code=404, detail="Doctor not found")
    if existing.get("verification_status") != "approved":
        raise HTTPException(status_code=409, detail="Only approved doctors can be suspended")
    if existing.get("is_suspended"):
        raise HTTPException(status_code=409, detail="Doctor already suspended")

    result = await db.doctors.update_one(
        {
            "_id": doctor_oid,
            "role": "doctor",
            "verification_status": "approved",
            "is_suspended": {"$ne": True},
        },
        {
            "$set": {
                "is_suspended": True,
                "suspended_at": now,
                "suspended_by_admin_id": current["_id"],
                "updated_at": now,
            }
        },
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=409, detail="Doctor state changed. Please retry.")

    await log_audit(
        db,
        actor_type="admin",
        actor_id=current["_id"],
        action="suspend_doctor",
        target_type="doctor",
        target_id=doctor_oid,
        meta={"old_is_suspended": False, "new_is_suspended": True},
        request=request,
    )
    await invalidate_doctor_cache(str(doctor_oid), invalidate_list=True)
    return {"message": "doctor_suspended", "doctor_id": doctor_id, "is_suspended": True}


@router.post(
    "/{doctor_id}/unsuspend",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def unsuspend_doctor(
    doctor_id: str,
    request: Request,
    current=Depends(verify_admin_session),
):
    db = get_db()
    now = utc_now()
    doctor_oid = await _parse_doctor_or_422(doctor_id)

    existing = await db.doctors.find_one({"_id": doctor_oid, "role": "doctor"})
    if not existing:
        raise HTTPException(status_code=404, detail="Doctor not found")
    if not existing.get("is_suspended"):
        raise HTTPException(status_code=409, detail="Doctor is not suspended")

    result = await db.doctors.update_one(
        {
            "_id": doctor_oid,
            "role": "doctor",
            "is_suspended": True,
        },
        {
            "$set": {
                "is_suspended": False,
                "unsuspended_at": now,
                "unsuspended_by_admin_id": current["_id"],
                "updated_at": now,
            }
        },
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=409, detail="Doctor state changed. Please retry.")

    await log_audit(
        db,
        actor_type="admin",
        actor_id=current["_id"],
        action="unsuspend_doctor",
        target_type="doctor",
        target_id=doctor_oid,
        meta={"old_is_suspended": True, "new_is_suspended": False},
        request=request,
    )
    await invalidate_doctor_cache(str(doctor_oid), invalidate_list=True)
    return {"message": "doctor_unsuspended", "doctor_id": doctor_id, "is_suspended": False}
