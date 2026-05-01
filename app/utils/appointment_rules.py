from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from fastapi import Cookie, HTTPException
from app.core.config import settings
from app.utils.time import ensure_utc
from app.utils.magic_token import verify_magic_token


def is_within_cancel_window(scheduled_at, now, hours=2) -> bool:
    scheduled_at = ensure_utc(scheduled_at)
    return scheduled_at - now < timedelta(hours=hours)


def is_within_booking_window(
    scheduled_at,
    now,
    *,
    days: int,
    tz_name: str = "Asia/Kolkata",
) -> bool:
    if isinstance(scheduled_at, str):
        try:
            if "T" in scheduled_at:
                scheduled_at = datetime.fromisoformat(scheduled_at)
            else:
                scheduled_at = datetime.combine(date.fromisoformat(scheduled_at), time.min)
        except ValueError:
            return False
    elif isinstance(scheduled_at, date) and not isinstance(scheduled_at, datetime):
        scheduled_at = datetime.combine(scheduled_at, time.min)

    scheduled_at = ensure_utc(scheduled_at)
    now = ensure_utc(now)
    if not scheduled_at or not now:
        return False

    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = ZoneInfo("Asia/Kolkata")

    today_local = now.astimezone(local_tz).date()
    target_local = scheduled_at.astimezone(local_tz).date()
    end_local = today_local + timedelta(days=max(0, int(days) - 1))
    return today_local <= target_local <= end_local


def build_patient_access_expiry(
    scheduled_at,
    *,
    duration_min: int | None = None,
    video_enabled: bool = False,
):
    scheduled_at = ensure_utc(scheduled_at)
    if not scheduled_at:
        return None

    if not video_enabled:
        return scheduled_at

    duration = int(duration_min or 20)
    late_grace = int(settings.VIDEO_JOIN_LATE_GRACE_MINUTES)
    return scheduled_at + timedelta(minutes=duration + late_grace)


def validate_patient_token(appt: dict, token: str, now) -> None:
    """
    Validates patient magic token + expiry.
    Raises HTTPException(403) if invalid/expired.
    """
    token_hash = appt.get("patient_access_token_hash")
    if not token_hash or not verify_magic_token(token, token_hash):
        raise HTTPException(status_code=403, detail="Invalid token")

    access_expires_at = ensure_utc(appt.get("patient_access_expires_at"))
    if not access_expires_at or access_expires_at <= now:
        raise HTTPException(status_code=403, detail="Token expired")


async def get_patient_access_token(
    public_patient_access_token: str | None = Cookie(
        default=None,
        alias=settings.PUBLIC_PATIENT_ACCESS_COOKIE_NAME,
    ),
) -> str:
    token = (public_patient_access_token or "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Patient access session required")
    return token
