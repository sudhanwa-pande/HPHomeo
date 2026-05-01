from datetime import datetime, timezone
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pymongo.errors import DuplicateKeyError

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.auth_routes import get_current_doctor
from app.services.cache_service import (
    TTL_5_MINUTES,
    cache_delete_keys,
    cache_get_json,
    cache_set_json,
    doctor_patients_key,
    invalidate_patient_cache,
)
from app.schemas.patient_schema import PatientCreate, PatientUpdate
from app.utils.security_sanitize import escaped_regex, sanitize_search_query

router = APIRouter(prefix="/patients", tags=["Patients"])


def _parse_patient_id(patient_id: str) -> ObjectId:
    try:
        return ObjectId(patient_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid patient_id")


def to_out(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "doctor_id": str(doc["doctor_id"]),
        "full_name": doc.get("full_name"),
        "email": doc.get("email"),
        "phone": doc.get("phone"),
        "age": doc.get("age"),
        "sex": doc.get("sex"),
        "notes": doc.get("notes"),
        "created_at": doc.get("created_at"),
        "updated_at": doc.get("updated_at"),
    }

@router.post(
    "",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def create_patient(payload: PatientCreate, doctor=Depends(get_current_doctor)):
    db = get_db()
    now = datetime.now(timezone.utc)

    doc = {
        "doctor_id": doctor["_id"],
        "full_name": payload.full_name,
        "phone": payload.phone,
        "age": payload.age,
        "sex": payload.sex,
        "created_by": "doctor",
        "created_at": now,
        "updated_at": now,
    }
    # Only include optional fields when they have values — storing None
    # would collide on sparse unique indexes (e.g. doctor_id + email).
    if payload.email:
        doc["email"] = payload.email
    if payload.notes:
        doc["notes"] = payload.notes

    try:
        res = await db.patients.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(
            status_code=409,
            detail="A patient with this email or phone already exists for this doctor",
        )

    created = await db.patients.find_one({"_id": res.inserted_id})
    await cache_delete_keys(doctor_patients_key(str(doctor["_id"])))
    await invalidate_patient_cache(str(res.inserted_id))
    return to_out(created)

@router.get(
    "",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def list_patients(doctor=Depends(get_current_doctor), q: str | None = None):
    db = get_db()
    q = sanitize_search_query(q)
    cache_key = None
    if not q:
        cache_key = doctor_patients_key(str(doctor["_id"]))
        cached = await cache_get_json(cache_key)
        if isinstance(cached, list):
            return cached

    base = {"doctor_id": doctor["_id"]}
    if q:
        # simple search by name/email/phone (case-insensitive)
        base["$or"] = [
            {"full_name": escaped_regex(q)},
            {"email": escaped_regex(q)},
            {"phone": escaped_regex(q)},
        ]

    cursor = db.patients.find(base).sort("created_at", -1)
    items = [to_out(x) async for x in cursor]
    if cache_key:
        await cache_set_json(cache_key, items, TTL_5_MINUTES)
    return items

@router.get(
    "/{patient_id}",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def get_patient(patient_id: str, doctor=Depends(get_current_doctor)):
    db = get_db()
    patient_oid = _parse_patient_id(patient_id)

    doc = await db.patients.find_one({
        "_id": patient_oid,
        "doctor_id": doctor["_id"],
    })
    if not doc:
        raise HTTPException(status_code=404, detail="Patient not found")

    return to_out(doc)

@router.put(
    "/{patient_id}",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def update_patient(patient_id: str, payload: PatientUpdate, doctor=Depends(get_current_doctor)):
    db = get_db()
    patient_oid = _parse_patient_id(patient_id)

    update_data = payload.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    update_data["updated_at"] = datetime.now(timezone.utc)

    try:
        res = await db.patients.update_one(
            {"_id": patient_oid, "doctor_id": doctor["_id"]},
            {"$set": update_data},
        )
    except DuplicateKeyError:
        raise HTTPException(
            status_code=409,
            detail="A patient with this email or phone already exists for this doctor",
        )
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Patient not found")

    doc = await db.patients.find_one({"_id": patient_oid})
    await cache_delete_keys(doctor_patients_key(str(doctor["_id"])))
    await invalidate_patient_cache(str(patient_oid))
    return to_out(doc)

@router.delete(
    "/{patient_id}",
    dependencies=[rl(settings.RL_PATIENT_MUTATION_TIMES, settings.RL_PATIENT_MUTATION_SECONDS)],
)
async def delete_patient(patient_id: str, doctor=Depends(get_current_doctor)):
    db = get_db()
    patient_oid = _parse_patient_id(patient_id)

    res = await db.patients.delete_one(
        {"_id": patient_oid, "doctor_id": doctor["_id"]}
    )
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Patient not found")

    await cache_delete_keys(doctor_patients_key(str(doctor["_id"])))
    await invalidate_patient_cache(str(patient_oid))
    return {"message": "deleted"}
