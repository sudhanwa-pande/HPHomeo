from datetime import date, datetime, time, timedelta, timezone
import math
from zoneinfo import ZoneInfo

from app.utils.slots import generate_slots_for_date, parse_hhmm, weekday_key
from app.utils.time import ensure_utc


def _resolve_tz(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Asia/Kolkata")


def _local_range_to_utc(target_date: date, start_hhmm: str, end_hhmm: str, tz_name: str) -> tuple[datetime, datetime]:
    local_tz = _resolve_tz(tz_name)
    start_t = parse_hhmm(start_hhmm)
    end_t = parse_hhmm(end_hhmm)
    start_dt = datetime.combine(target_date, start_t).replace(tzinfo=local_tz)
    end_dt = datetime.combine(target_date, end_t).replace(tzinfo=local_tz)
    return start_dt.astimezone(timezone.utc), end_dt.astimezone(timezone.utc)


async def get_candidate_slots_for_date(*, db, doctor_id, target_date: date) -> tuple[list[datetime], int, str]:
    return await get_candidate_slots_for_date_with_availability(
        db=db,
        doctor_id=doctor_id,
        target_date=target_date,
        availability_override=None,
    )


async def get_candidate_slots_for_date_with_availability(
    *,
    db,
    doctor_id,
    target_date: date,
    availability_override: dict | None,
) -> tuple[list[datetime], int, str]:
    avail = availability_override or await db.doctor_availability.find_one({"doctor_id": doctor_id})
    if not avail:
        return [], 20, "Asia/Kolkata"

    tz_name = avail.get("timezone", "Asia/Kolkata")
    slot_minutes = int(avail.get("slot_duration_min", 20))
    wk = weekday_key(target_date)
    weekly_ranges = avail.get("weekly", {}).get(wk, [])

    base_slots = generate_slots_for_date(target_date, weekly_ranges, slot_minutes, clinic_tz=tz_name) if weekly_ranges else []
    candidate = set(base_slots)

    exceptions = await db.doctor_availability_exceptions.find(
        {"doctor_id": doctor_id, "date": target_date.isoformat()},
        {"status": 1, "time_slots": 1},
    ).to_list(length=500)

    blocked_windows: list[tuple[datetime, datetime]] = []

    for exc in exceptions:
        status = exc.get("status")
        ranges = exc.get("time_slots") or []
        if status == "available":
            if ranges:
                extra_slots = generate_slots_for_date(target_date, ranges, slot_minutes, clinic_tz=tz_name)
                for slot in extra_slots:
                    candidate.add(slot)
        elif status == "blocked":
            if not ranges:
                day_start, day_end = _local_range_to_utc(target_date, "00:00", "23:59", tz_name)
                blocked_windows.append((day_start, day_end + timedelta(minutes=1)))
            else:
                for r in ranges:
                    start_hhmm = str(r.get("start") or "")
                    end_hhmm = str(r.get("end") or "")
                    if not start_hhmm or not end_hhmm:
                        continue
                    blocked_windows.append(_local_range_to_utc(target_date, start_hhmm, end_hhmm, tz_name))

    filtered_slots: list[datetime] = []
    for slot in sorted(candidate):
        blocked = any(start <= slot < end for start, end in blocked_windows)
        if not blocked:
            filtered_slots.append(slot)

    return filtered_slots, slot_minutes, tz_name


async def get_available_slots_for_date(
    *,
    db,
    doctor_id,
    target_date: date,
    now_utc: datetime,
    exclude_appointment_id=None,
    required_duration_min: int | None = None,
) -> tuple[list[datetime], int, str]:
    candidate, slot_minutes, tz_name = await get_candidate_slots_for_date(
        db=db,
        doctor_id=doctor_id,
        target_date=target_date,
    )
    if not candidate:
        return [], slot_minutes, tz_name

    local_tz = _resolve_tz(tz_name)
    day_start_local = datetime.combine(target_date, time.min).replace(tzinfo=local_tz)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(timezone.utc)
    day_end_utc = day_end_local.astimezone(timezone.utc)

    effective_duration_min = int(required_duration_min or slot_minutes)
    step_minutes = max(1, int(slot_minutes))
    required_steps = max(1, int(math.ceil(effective_duration_min / step_minutes)))
    candidate_set = set(candidate)

    query = {
        "doctor_id": doctor_id,
        "scheduled_at": {"$gte": day_start_utc, "$lt": day_end_utc},
        "$or": [
            {"status": "confirmed"},
            {"status": "pending_payment", "pending_payment_expires_at": {"$gt": now_utc}},
        ],
    }
    if exclude_appointment_id is not None:
        query["_id"] = {"$ne": exclude_appointment_id}

    booked = await db.appointments.find(query, {"scheduled_at": 1, "duration_min": 1}).to_list(length=5000)

    available: list[datetime] = []
    for slot in candidate:
        if slot <= now_utc:
            continue

        slot_end = slot + timedelta(minutes=effective_duration_min)
        if slot_end > day_end_utc:
            continue

        # Ensure contiguous base slot coverage for longer durations.
        contiguous_ok = True
        for step_idx in range(1, required_steps):
            next_slot = slot + timedelta(minutes=step_idx * step_minutes)
            if next_slot not in candidate_set:
                contiguous_ok = False
                break
        if not contiguous_ok:
            continue

        overlaps = False
        for item in booked:
            booked_start = item.get("scheduled_at")
            if not booked_start:
                continue
            booked_start = ensure_utc(booked_start)
            booked_duration = int(item.get("duration_min") or slot_minutes)
            booked_end = booked_start + timedelta(minutes=max(1, booked_duration))
            if slot < booked_end and booked_start < slot_end:
                overlaps = True
                break
        if overlaps:
            continue

        available.append(slot)

    return available, slot_minutes, tz_name
