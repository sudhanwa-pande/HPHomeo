from datetime import datetime, date, time, timedelta, timezone
from typing import List, Dict
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def weekday_key(d: date) -> str:
    # Monday=0 ... Sunday=6
    return WEEKDAY_KEYS[d.weekday()]


def parse_hhmm(hhmm: str) -> time:
    return datetime.strptime(hhmm, "%H:%M").time()


def generate_slots_for_date(
    target_date: date,
    ranges: List[Dict[str, str]],
    slot_minutes: int,
    clinic_tz: str = "Asia/Kolkata",
) -> List[datetime]:
    """
    Generates slot start datetimes for the given date based on ranges.
    Input ranges are local clinic times (doctor timezone). Output slots are UTC-aware datetimes.

    Slots are [start, start+slot_minutes). We include slot if it fits fully inside range.
    """
    slots: List[datetime] = []
    step = timedelta(minutes=slot_minutes)
    try:
        local_tz = ZoneInfo(clinic_tz)
    except Exception:
        local_tz = IST

    for r in ranges:
        start_t = parse_hhmm(r["start"])
        end_t = parse_hhmm(r["end"])

        # Build in doctor local timezone
        start_dt_local = datetime.combine(target_date, start_t).replace(tzinfo=local_tz)
        end_dt_local = datetime.combine(target_date, end_t).replace(tzinfo=local_tz)

        cur = start_dt_local
        while cur + step <= end_dt_local:
            # Convert to UTC for storage/comparison
            slots.append(cur.astimezone(timezone.utc))
            cur += step

    return slots
