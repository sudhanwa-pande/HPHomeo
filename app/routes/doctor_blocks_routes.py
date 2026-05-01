from datetime import date, datetime, timedelta
import hashlib
import json
from zoneinfo import ZoneInfo

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import settings
from app.core.database import get_db
from app.routes.auth_routes import get_current_doctor
from app.schemas.block_schema import AvailabilityExceptionCreate
from app.services.cache_service import invalidate_doctor_cache
from app.services.appointment_reschedule_service import reschedule_or_cancel_appointments
from app.utils.slots import parse_hhmm
from app.utils.time import day_window_to_utc, ensure_utc, utc_now

router = APIRouter(prefix="/doctor", tags=["Doctor Exceptions"])
CONFIRM_TOKEN_TTL_SECONDS = 300


def _ranges_overlap(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    return a_start < b_end and b_start < a_end


def _validate_time_ranges(ranges: list[dict]) -> None:
    normalized: list[tuple[str, str]] = []
    for r in ranges:
        start = str(r.get("start") or "")
        end = str(r.get("end") or "")
        try:
            parse_hhmm(start)
            parse_hhmm(end)
        except Exception:
            raise HTTPException(status_code=422, detail="time_slots must use HH:MM")
        if start >= end:
            raise HTTPException(status_code=400, detail="time_slots contains invalid range")
        normalized.append((start, end))

    normalized.sort(key=lambda item: item[0])
    for idx in range(1, len(normalized)):
        prev_start, prev_end = normalized[idx - 1]
        cur_start, cur_end = normalized[idx]
        if cur_start < prev_end:
            raise HTTPException(
                status_code=409,
                detail=f"Overlapping exception ranges: {prev_start}-{prev_end} overlaps {cur_start}-{cur_end}",
            )


async def _reschedule_or_cancel_impacted(
    *,
    db,
    doctor_id,
    target_date: date,
    blocked_ranges: list[dict],
    now_utc: datetime,
):
    avail_doc = await db.doctor_availability.find_one({"doctor_id": doctor_id}, {"timezone": 1})
    tz_name = (avail_doc or {}).get("timezone", "Asia/Kolkata")
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = ZoneInfo("Asia/Kolkata")

    day_start_utc, day_end_utc = day_window_to_utc(target_date.isoformat(), tz_name=tz_name)

    impacted_q = {
        "doctor_id": doctor_id,
        "scheduled_at": {"$gte": day_start_utc, "$lt": day_end_utc},
        "$or": [
            {"status": "confirmed"},
            {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now_utc}},
        ],
    }
    impacted = await db.appointments.find(impacted_q).sort("scheduled_at", 1).to_list(length=5000)

    if blocked_ranges:
        blocked_windows_utc: list[tuple[datetime, datetime]] = []
        for r in blocked_ranges:
            start_t = parse_hhmm(r["start"])
            end_t = parse_hhmm(r["end"])
            start_local = datetime.combine(target_date, start_t).replace(tzinfo=local_tz)
            end_local = datetime.combine(target_date, end_t).replace(tzinfo=local_tz)
            blocked_windows_utc.append((start_local.astimezone(day_start_utc.tzinfo), end_local.astimezone(day_start_utc.tzinfo)))

        in_window = []
        for appt in impacted:
            appt_utc = ensure_utc(appt["scheduled_at"])
            blocked = any(start <= appt_utc < end for start, end in blocked_windows_utc)
            if blocked:
                in_window.append(appt)
        impacted = in_window

    impacted = [item for item in impacted if not item.get("is_rescheduled", False)]
    rescheduled, cancelled = await reschedule_or_cancel_appointments(
        db=db,
        doctor_id=doctor_id,
        appointments=impacted,
        now_utc=now_utc,
        start_date=target_date + timedelta(days=1),
        search_days=30,
    )

    return len(impacted), rescheduled, cancelled


def _conflict_hash(appointments: list[dict]) -> str:
    items = []
    for item in appointments:
        appt_id = str(item.get("appointment_id") or item.get("_id") or "")
        scheduled = item.get("scheduled_at")
        scheduled_str = scheduled.isoformat() if hasattr(scheduled, "isoformat") else str(scheduled or "")
        items.append(f"{appt_id}:{scheduled_str}")
    canonical = "|".join(sorted(items))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _make_confirm_token(
    *,
    doctor_id: str,
    date_value: str,
    start_time: str,
    end_time: str,
    conflict_hash: str,
    now_ts: int,
) -> str:
    payload = {
        "doctor_id": doctor_id,
        "date": date_value,
        "start_time": start_time,
        "end_time": end_time,
        "conflict_hash": conflict_hash,
        "ts": now_ts,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    secret = getattr(settings, "SECRET_KEY", "dev-secret")
    digest = hashlib.sha256(f"{raw}:{secret}".encode("utf-8")).hexdigest()
    return f"{now_ts}.{digest}"


def _validate_confirm_token(
    *,
    token: str,
    doctor_id: str,
    date_value: str,
    start_time: str,
    end_time: str,
    conflict_hash: str,
    now_ts: int,
) -> bool:
    try:
        ts_str, digest = token.split(".", 1)
        token_ts = int(ts_str)
    except Exception:
        return False
    if now_ts - token_ts > CONFIRM_TOKEN_TTL_SECONDS:
        return False
    expected = _make_confirm_token(
        doctor_id=doctor_id,
        date_value=date_value,
        start_time=start_time,
        end_time=end_time,
        conflict_hash=conflict_hash,
        now_ts=token_ts,
    )
    return expected == token


@router.get("/schedule/conflicts")
async def get_schedule_conflicts(
    date_str: str = Query(..., alias="date"),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    now = utc_now()
    avail_doc = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]}, {"timezone": 1})
    tz_name = (avail_doc or {}).get("timezone", "Asia/Kolkata")
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = ZoneInfo("Asia/Kolkata")

    try:
        target_date = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD")

    day_start, day_end = day_window_to_utc(target_date.isoformat(), tz_name=tz_name)
    q = {
        "doctor_id": doctor["_id"],
        "scheduled_at": {"$gte": day_start, "$lt": day_end},
        "is_rescheduled": {"$ne": True},
        "$or": [
            {"status": "confirmed"},
            {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now}},
        ],
    }
    docs = await db.appointments.find(
        q,
        {
            "_id": 1,
            "patient_name": 1,
            "patient_phone": 1,
            "scheduled_at": 1,
            "status": 1,
        },
    ).sort("scheduled_at", 1).to_list(length=2000)

    if start_time and end_time:
        try:
            parse_hhmm(start_time)
            parse_hhmm(end_time)
        except Exception:
            raise HTTPException(status_code=422, detail="start_time/end_time must be HH:MM")
        if start_time >= end_time:
            raise HTTPException(status_code=400, detail="start_time must be earlier than end_time")

        filtered = []
        for item in docs:
            hhmm = ensure_utc(item["scheduled_at"]).astimezone(local_tz).strftime("%H:%M")
            if start_time <= hhmm < end_time:
                filtered.append(item)
        docs = filtered

    token_start = start_time or ""
    token_end = end_time or ""
    conflict_hash = _conflict_hash(docs)
    confirm_token = _make_confirm_token(
        doctor_id=str(doctor["_id"]),
        date_value=date_str,
        start_time=token_start,
        end_time=token_end,
        conflict_hash=conflict_hash,
        now_ts=int(now.timestamp()),
    )

    return {
        "count": len(docs),
        "conflict_hash": conflict_hash,
        "confirm_token": confirm_token,
        "appointments": [
            {
                "appointment_id": str(item["_id"]),
                "patient_name": item.get("patient_name"),
                "patient_phone": item.get("patient_phone"),
                "scheduled_at": item.get("scheduled_at"),
                "status": item.get("status"),
            }
            for item in docs
        ],
    }


@router.post("/availability/exception")
async def create_exception(
    payload: AvailabilityExceptionCreate,
    confirm_token: str | None = Query(default=None),
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    now = utc_now()
    avail_doc = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]}, {"timezone": 1})
    tz_name = (avail_doc or {}).get("timezone", "Asia/Kolkata")
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = ZoneInfo("Asia/Kolkata")

    try:
        target_date = date.fromisoformat(payload.date)
    except ValueError:
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD")

    if target_date < now.astimezone(local_tz).date():
        raise HTTPException(status_code=409, detail="Cannot create exception for past date")

    ranges = [r.model_dump() for r in payload.time_slots]
    _validate_time_ranges(ranges)

    existing = await db.doctor_availability_exceptions.find(
        {"doctor_id": doctor["_id"], "date": payload.date},
        {"time_slots": 1},
    ).to_list(length=500)

    if not ranges:
        if existing:
            raise HTTPException(status_code=409, detail="Exception overlaps an existing exception")
    else:
        for exc in existing:
            existing_ranges = exc.get("time_slots") or []
            if not existing_ranges:
                raise HTTPException(status_code=409, detail="Exception overlaps an existing exception")
            for r in ranges:
                for er in existing_ranges:
                    if _ranges_overlap(r["start"], r["end"], er.get("start", ""), er.get("end", "")):
                        raise HTTPException(status_code=409, detail="Exception overlaps an existing exception")

    impacted = 0
    rescheduled = 0
    cancelled = 0

    if payload.status == "blocked":
        if ranges:
            start_time = min(item["start"] for item in ranges)
            end_time = max(item["end"] for item in ranges)
        else:
            start_time = ""
            end_time = ""
        conflict_response = await get_schedule_conflicts(
            date_str=payload.date,
            start_time=start_time if start_time else None,
            end_time=end_time if end_time else None,
            doctor=doctor,
        )
        conflict_docs = conflict_response["appointments"]
        conflict_hash = conflict_response["conflict_hash"]
        if conflict_docs:
            if not confirm_token:
                raise HTTPException(status_code=409, detail="Conflicts exist. confirm_token required.")
            is_valid = _validate_confirm_token(
                token=confirm_token,
                doctor_id=str(doctor["_id"]),
                date_value=payload.date,
                start_time=start_time,
                end_time=end_time,
                conflict_hash=conflict_hash,
                now_ts=int(now.timestamp()),
            )
            if not is_valid:
                raise HTTPException(status_code=409, detail="Invalid or expired confirm_token")

    exception_doc = {
        "doctor_id": doctor["_id"],
        "date": payload.date,
        "status": payload.status,
        "time_slots": ranges,
        "reason": payload.reason,
        "created_at": now,
        "updated_at": now,
        "apply_status": "processing" if payload.status == "blocked" else "completed",
    }
    result = await db.doctor_availability_exceptions.insert_one(exception_doc)

    if payload.status == "blocked":
        try:
            impacted, rescheduled, cancelled = await _reschedule_or_cancel_impacted(
                db=db,
                doctor_id=doctor["_id"],
                target_date=target_date,
                blocked_ranges=ranges,
                now_utc=now,
            )
            await db.doctor_availability_exceptions.update_one(
                {"_id": result.inserted_id},
                {
                    "$set": {
                        "apply_status": "completed",
                        "impacted": impacted,
                        "rescheduled": rescheduled,
                        "cancelled": cancelled,
                        "updated_at": utc_now(),
                    }
                },
            )
        except Exception as exc:
            await db.doctor_availability_exceptions.update_one(
                {"_id": result.inserted_id},
                {
                    "$set": {
                        "apply_status": "failed",
                        "apply_error": str(exc)[:500],
                        "updated_at": utc_now(),
                    }
                },
            )
            raise

    await invalidate_doctor_cache(str(doctor["_id"]), day=payload.date)

    return {
        "message": "exception_saved",
        "exception_id": str(result.inserted_id),
        "status": payload.status,
        "impacted": impacted,
        "rescheduled": rescheduled,
        "cancelled": cancelled,
    }


@router.get("/availability/exception")
async def list_exceptions(
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    doctor=Depends(get_current_doctor),
):
    db = get_db()

    query: dict = {"doctor_id": doctor["_id"]}
    if from_date or to_date:
        date_filter: dict = {}
        if from_date:
            date_filter["$gte"] = from_date
        if to_date:
            date_filter["$lte"] = to_date
        query["date"] = date_filter

    docs = await db.doctor_availability_exceptions.find(query).sort("date", -1).to_list(length=1000)
    return [
        {
            "id": str(doc["_id"]),
            "date": doc.get("date"),
            "status": doc.get("status"),
            "time_slots": doc.get("time_slots", []),
            "reason": doc.get("reason"),
            "apply_status": doc.get("apply_status"),
            "impacted": doc.get("impacted"),
            "rescheduled": doc.get("rescheduled"),
            "cancelled": doc.get("cancelled"),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
        }
        for doc in docs
    ]


@router.delete("/availability/exception/{exception_id}")
async def delete_exception(exception_id: str, doctor=Depends(get_current_doctor)):
    db = get_db()

    try:
        exc_oid = ObjectId(exception_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid exception_id")

    existing = await db.doctor_availability_exceptions.find_one({"_id": exc_oid, "doctor_id": doctor["_id"]})
    if not existing:
        raise HTTPException(status_code=404, detail="Exception not found")

    res = await db.doctor_availability_exceptions.delete_one({"_id": exc_oid, "doctor_id": doctor["_id"]})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Exception not found")

    await invalidate_doctor_cache(str(doctor["_id"]), day=existing.get("date"))
    return {"message": "deleted", "exception_id": exception_id}
