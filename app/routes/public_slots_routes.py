from datetime import date, timedelta
from fastapi import APIRouter, HTTPException, Query
from bson import ObjectId

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.services.cache_service import TTL_5_MINUTES, cache_get_json, cache_set_json, slots_key
from app.services.doctor_availability_service import get_available_slots_for_date
from app.utils.appointment_rules import is_within_booking_window
from app.utils.time import utc_now

router = APIRouter(prefix="/public", tags=["Public Slots"])

@router.get(
    "/doctors/{doctor_id}/slots",
    dependencies=[rl(settings.RL_PUBLIC_READ_TIMES, settings.RL_PUBLIC_READ_SECONDS)],
)
async def get_doctor_slots(
    doctor_id: str,
    day: str = Query(..., description="YYYY-MM-DD"),
):
    """
    Returns available slots for a doctor on a given date.

    Filters out:
    - Doctor blocks
    - confirmed appointments
    - pending_payment appointments that are not expired
    """

    db = get_db()
    cache_key = slots_key(doctor_id, day)
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    # 1️⃣ Validate doctor
    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")

    doctor = await db.doctors.find_one(
        {"_id": doctor_oid, "verification_status": "approved", "is_suspended": {"$ne": True}}
    )
    if not doctor:
        raise HTTPException(status_code=404, detail="Doctor not found")

    avail = await db.doctor_availability.find_one({"doctor_id": doctor_oid}, {"timezone": 1})
    if not avail:
        raise HTTPException(status_code=400, detail="Doctor hasn't set availability yet")
    avail_tz = avail.get("timezone", "Asia/Kolkata")

    # 2️⃣ Parse day
    try:
        target_date = date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid day format. Use YYYY-MM-DD")

    now = utc_now()
    if not is_within_booking_window(
        target_date.isoformat(),
        now,
        days=settings.BOOKING_WINDOW_DAYS,
        tz_name=avail_tz,
    ):
        raise HTTPException(
            status_code=400,
            detail=f"Booking window is limited to the next {settings.BOOKING_WINDOW_DAYS} days",
        )

    available, slot_minutes, _ = await get_available_slots_for_date(
        db=db,
        doctor_id=doctor_oid,
        target_date=target_date,
        now_utc=now,
    )

    response = {
        "doctor_id": doctor_id,
        "day": day,
        "slot_minutes": slot_minutes,
        "slots": [s.isoformat() for s in available],
    }
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


@router.get(
    "/doctors/{doctor_id}/available-slots",
    dependencies=[rl(settings.RL_PUBLIC_READ_TIMES, settings.RL_PUBLIC_READ_SECONDS)],
)
async def get_doctor_available_slots(
    doctor_id: str,
    from_date: str = Query(..., alias="from", description="YYYY-MM-DD"),
    to_date: str = Query(..., alias="to", description="YYYY-MM-DD"),
):
    db = get_db()
    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")

    doctor = await db.doctors.find_one(
        {"_id": doctor_oid, "verification_status": "approved", "is_suspended": {"$ne": True}}
    )
    if not doctor:
        raise HTTPException(status_code=404, detail="Doctor not found")

    avail = await db.doctor_availability.find_one({"doctor_id": doctor_oid}, {"timezone": 1})
    if not avail:
        raise HTTPException(status_code=400, detail="Doctor hasn't set availability yet")
    avail_tz = avail.get("timezone", "Asia/Kolkata")

    try:
        start_date = date.fromisoformat(from_date)
        end_date = date.fromisoformat(to_date)
    except ValueError:
        raise HTTPException(status_code=422, detail="from/to must be YYYY-MM-DD")
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="to must be >= from")

    days = (end_date - start_date).days + 1
    if days > settings.BOOKING_WINDOW_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Date range cannot exceed {settings.BOOKING_WINDOW_DAYS} days",
        )

    now = utc_now()
    if not is_within_booking_window(
        start_date.isoformat(),
        now,
        days=settings.BOOKING_WINDOW_DAYS,
        tz_name=avail_tz,
    ) or not is_within_booking_window(
        end_date.isoformat(),
        now,
        days=settings.BOOKING_WINDOW_DAYS,
        tz_name=avail_tz,
    ):
        raise HTTPException(
            status_code=400,
            detail=f"Booking window is limited to the next {settings.BOOKING_WINDOW_DAYS} days",
        )

    out: list[dict] = []
    for offset in range(days):
        target_date = start_date + timedelta(days=offset)
        available, slot_minutes, _ = await get_available_slots_for_date(
            db=db,
            doctor_id=doctor_oid,
            target_date=target_date,
            now_utc=now,
        )
        for slot in available:
            out.append(
                {
                    "date": target_date.isoformat(),
                    "start": slot.isoformat(),
                    "end": (slot + timedelta(minutes=slot_minutes)).isoformat(),
                    "duration_minutes": slot_minutes,
                }
            )

    return {
        "doctor_id": doctor_id,
        "from": from_date,
        "to": to_date,
        "count": len(out),
        "slots": out,
    }
