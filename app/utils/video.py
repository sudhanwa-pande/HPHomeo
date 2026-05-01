from datetime import datetime, timedelta

from fastapi import HTTPException

from app.core.config import settings
from app.utils.time import ensure_utc


def check_join_window(appointment: dict, now: datetime, role: str = "patient") -> None:
    if role == "doctor":
        return

    scheduled_at = ensure_utc(appointment["scheduled_at"])
    duration_min = int(appointment.get("duration_min") or 20)
    early_limit = int(settings.VIDEO_JOIN_EARLY_MINUTES)
    late_limit = int(settings.VIDEO_JOIN_LATE_GRACE_MINUTES)

    window_start = scheduled_at - timedelta(minutes=early_limit)
    window_end = scheduled_at + timedelta(minutes=duration_min + late_limit)

    if now < window_start:
        raise HTTPException(
            status_code=403,
            detail=f"You can join {early_limit} minutes before your appointment",
        )
    if now > window_end:
        raise HTTPException(
            status_code=403,
            detail="This appointment call window has ended",
        )
