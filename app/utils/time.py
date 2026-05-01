from datetime import datetime, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def _resolve_timezone(tz_name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name or "Asia/Kolkata")
    except Exception:
        return IST


def parse_client_datetime_to_utc(dt_str: str, naive_tz: str | None = None) -> datetime:
    """
    Accepts ISO datetime string.
    - If timezone offset is present (e.g. +05:30, Z) → convert to UTC.
    - If naive and naive_tz is given → treat as that timezone → convert to UTC.
    - If naive and no naive_tz → raise ValueError so callers surface a 422 to the client.
      Clients must always send an explicit UTC offset to avoid silent IST assumptions.
    Returns timezone-aware UTC datetime.
    """
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        if naive_tz is None:
            raise ValueError(
                "scheduled_at must include a UTC offset (e.g. 2026-03-02T09:00:00+05:30). "
                "Naive datetimes are not accepted."
            )
        dt = dt.replace(tzinfo=_resolve_timezone(naive_tz))
    return dt.astimezone(timezone.utc)


def utc_now() -> datetime:
    """Timezone-aware current UTC time."""
    return datetime.now(timezone.utc)


def ist_day_window_to_utc(day_str: str) -> tuple[datetime, datetime]:
    """
    Input: 'YYYY-MM-DD' (interpreted as IST calendar day)
    Output: (start_utc, end_utc) timezone-aware datetimes for Mongo queries.
    """
    from datetime import date, timedelta

    return day_window_to_utc(day_str, tz_name="Asia/Kolkata")


def day_window_to_utc(day_str: str, tz_name: str = "Asia/Kolkata") -> tuple[datetime, datetime]:
    """
    Input: 'YYYY-MM-DD' interpreted in provided timezone.
    Output: (start_utc, end_utc) timezone-aware datetimes for Mongo queries.
    """
    from datetime import date, timedelta

    target_date = date.fromisoformat(day_str)
    local_tz = _resolve_timezone(tz_name)
    start_local = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=local_tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

def ensure_utc(dt: datetime) -> datetime:
    """Make a datetime timezone-aware (UTC) if it's naive."""
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def format_ist(dt: datetime) -> str:
    """Format a datetime as IST string for display."""
    if dt is None:
        return "N/A"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).strftime("%d %b %Y, %I:%M %p IST")
