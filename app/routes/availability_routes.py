from datetime import datetime, timezone
import hashlib
import json
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.database import get_db
from app.core.config import settings
from app.routes.auth_routes import get_current_doctor
from app.services.cache_service import (
    TTL_30_MINUTES,
    cache_get_json,
    cache_set_json,
    doctor_availability_key,
    invalidate_doctor_cache,
)
from app.services.appointment_reschedule_service import reschedule_or_cancel_appointments
from app.services.doctor_availability_service import get_candidate_slots_for_date_with_availability
from app.schemas.availability_schema import WeeklyAvailability
from app.utils.time import ensure_utc, utc_now
from app.utils.time_utils import validate_time_format

router = APIRouter(prefix="/doctor/availability", tags=["Doctor Availability"])
AVAILABILITY_CONFIRM_TTL_SECONDS = 300


def _validate_weekly(payload: WeeklyAvailability) -> None:
    for day, ranges in payload.weekly.items():
        normalized_ranges: list[tuple[str, str]] = []
        for r in ranges:
            validate_time_format(r.start)
            validate_time_format(r.end)
            if r.start >= r.end:
                raise HTTPException(status_code=400, detail=f"Invalid range in {day}")
            normalized_ranges.append((r.start, r.end))

        normalized_ranges.sort(key=lambda item: item[0])
        for idx in range(1, len(normalized_ranges)):
            prev_start, prev_end = normalized_ranges[idx - 1]
            cur_start, cur_end = normalized_ranges[idx]
            if cur_start < prev_end:
                raise HTTPException(
                    status_code=409,
                    detail=f"Overlapping ranges in {day}: {prev_start}-{prev_end} overlaps {cur_start}-{cur_end}",
                )


async def _upsert_availability(payload: WeeklyAvailability, doctor_id) -> None:
    db = get_db()
    _validate_weekly(payload)

    now = datetime.now(timezone.utc)
    doc = {
        "doctor_id": doctor_id,
        "slot_duration_min": payload.slot_duration_min,
        "timezone": payload.timezone,
        "weekly": payload.model_dump()["weekly"],
        "updated_at": now,
    }
    await db.doctor_availability.update_one(
        {"doctor_id": doctor_id},
        {"$set": doc, "$setOnInsert": {"created_at": now}},
        upsert=True,
    )
    await invalidate_doctor_cache(str(doctor_id), invalidate_list=True)


async def _build_availability_conflicts_preview(
    *,
    doctor_id,
    availability_doc: dict,
    current_availability_doc: dict | None,
    now_utc,
) -> tuple[list[dict], str]:
    db = get_db()
    try:
        local_tz = ZoneInfo(availability_doc.get("timezone", "Asia/Kolkata"))
    except Exception:
        local_tz = ZoneInfo("Asia/Kolkata")

    active_q = {
        "doctor_id": doctor_id,
        "scheduled_at": {"$gte": now_utc},
        "is_rescheduled": {"$ne": True},
        "$or": [
            {"status": "confirmed"},
            {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now_utc}},
        ],
    }
    appointments = await db.appointments.find(active_q).sort("scheduled_at", 1).to_list(length=10000)

    impacted = []
    for appt in appointments:
        appt_dt = ensure_utc(appt["scheduled_at"])
        target_date = appt_dt.astimezone(local_tz).date()
        proposed_slots, _, _ = await get_candidate_slots_for_date_with_availability(
            db=db,
            doctor_id=doctor_id,
            target_date=target_date,
            availability_override=availability_doc,
        )
        currently_supported = True
        if current_availability_doc:
            current_slots, _, _ = await get_candidate_slots_for_date_with_availability(
                db=db,
                doctor_id=doctor_id,
                target_date=target_date,
                availability_override=current_availability_doc,
            )
            currently_supported = appt_dt in current_slots

        if currently_supported and appt_dt not in proposed_slots:
            appt["scheduled_at"] = appt_dt
            impacted.append(appt)

    canonical = "|".join(
        sorted(
            f"{str(item['_id'])}:{item['scheduled_at'].isoformat()}"
            for item in impacted
        )
    )
    conflict_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return impacted, conflict_hash


def _make_confirm_token(*, doctor_id: str, conflict_hash: str, now_ts: int) -> str:
    payload = {"doctor_id": doctor_id, "conflict_hash": conflict_hash, "ts": now_ts}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    secret = getattr(settings, "SECRET_KEY", "dev-secret")
    return f"{now_ts}.{hashlib.sha256(f'{raw}:{secret}'.encode('utf-8')).hexdigest()}"


def _validate_confirm_token(*, token: str, doctor_id: str, conflict_hash: str, now_ts: int) -> bool:
    try:
        ts_str, digest = token.split(".", 1)
        token_ts = int(ts_str)
    except Exception:
        return False
    if now_ts - token_ts > AVAILABILITY_CONFIRM_TTL_SECONDS:
        return False
    expected = _make_confirm_token(doctor_id=doctor_id, conflict_hash=conflict_hash, now_ts=token_ts)
    return expected == token


async def _reconcile_availability_conflicts(*, doctor_id, timezone_name: str, now_utc, impacted: list[dict]) -> tuple[int, int, int]:
    db = get_db()
    try:
        local_tz = ZoneInfo(timezone_name)
    except Exception:
        local_tz = ZoneInfo("Asia/Kolkata")

    rescheduled = 0
    cancelled = 0
    for appt in impacted:
        start_date = ensure_utc(appt["scheduled_at"]).astimezone(local_tz).date()
        r, c = await reschedule_or_cancel_appointments(
            db=db,
            doctor_id=doctor_id,
            appointments=[appt],
            now_utc=now_utc,
            start_date=start_date,
            search_days=30,
        )
        rescheduled += r
        cancelled += c

    return len(impacted), rescheduled, cancelled


@router.post("/conflicts-preview")
async def preview_availability_conflicts(payload: WeeklyAvailability, doctor=Depends(get_current_doctor)):
    _validate_weekly(payload)
    db = get_db()
    now = utc_now()
    current_availability_doc = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]})
    availability_doc = {
        "doctor_id": doctor["_id"],
        "slot_duration_min": payload.slot_duration_min,
        "timezone": payload.timezone,
        "weekly": payload.model_dump()["weekly"],
    }
    impacted, conflict_hash = await _build_availability_conflicts_preview(
        doctor_id=doctor["_id"],
        availability_doc=availability_doc,
        current_availability_doc=current_availability_doc,
        now_utc=now,
    )
    confirm_token = _make_confirm_token(
        doctor_id=str(doctor["_id"]),
        conflict_hash=conflict_hash,
        now_ts=int(now.timestamp()),
    )
    return {
        "count": len(impacted),
        "conflict_hash": conflict_hash,
        "confirm_token": confirm_token,
        "appointments": [
            {
                "appointment_id": str(item["_id"]),
                "scheduled_at": item.get("scheduled_at"),
                "status": item.get("status"),
                "patient_name": item.get("patient_name"),
            }
            for item in impacted
        ],
    }

@router.get("")
async def get_availability(doctor=Depends(get_current_doctor)):
    db = get_db()
    cache_key = doctor_availability_key(str(doctor["_id"]))
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    doc = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]})
    if not doc:
        return {"message": "Availability not configured"}
    doc["doctor_id"] = str(doc["doctor_id"])
    doc["_id"] = str(doc["_id"])
    await cache_set_json(cache_key, doc, TTL_30_MINUTES)
    return doc

@router.post("")
async def set_availability(payload: WeeklyAvailability, doctor=Depends(get_current_doctor)):
    await _upsert_availability(payload, doctor["_id"])
    return {"message": "Availability saved successfully"}


@router.put("")
async def update_availability(
    payload: WeeklyAvailability,
    confirm_token: str | None = Query(default=None),
    doctor=Depends(get_current_doctor),
):
    _validate_weekly(payload)
    db = get_db()
    now = utc_now()
    current_availability_doc = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]})
    availability_doc = {
        "doctor_id": doctor["_id"],
        "slot_duration_min": payload.slot_duration_min,
        "timezone": payload.timezone,
        "weekly": payload.model_dump()["weekly"],
    }
    impacted_preview, conflict_hash = await _build_availability_conflicts_preview(
        doctor_id=doctor["_id"],
        availability_doc=availability_doc,
        current_availability_doc=current_availability_doc,
        now_utc=now,
    )
    if impacted_preview:
        if not confirm_token:
            raise HTTPException(status_code=409, detail="Conflicts exist. confirm_token required.")
        if not _validate_confirm_token(
            token=confirm_token,
            doctor_id=str(doctor["_id"]),
            conflict_hash=conflict_hash,
            now_ts=int(now.timestamp()),
        ):
            raise HTTPException(status_code=409, detail="Invalid or expired confirm_token")

    await _upsert_availability(payload, doctor["_id"])
    impacted, rescheduled, cancelled = await _reconcile_availability_conflicts(
        doctor_id=doctor["_id"],
        timezone_name=payload.timezone,
        now_utc=now,
        impacted=impacted_preview,
    )
    return {
        "message": "Availability saved successfully",
        "impacted": impacted,
        "rescheduled": rescheduled,
        "cancelled": cancelled,
    }
