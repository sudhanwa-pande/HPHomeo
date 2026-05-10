from datetime import datetime, timedelta

from fastapi import HTTPException

from app.core.config import settings
from app.utils.time import ensure_utc


def check_join_window(appointment: dict, now: datetime, role: str = "patient") -> None:
    scheduled_at = ensure_utc(appointment["scheduled_at"])
    
    # Define role-based early limits
    if role == "doctor":
        early_limit = 5  # Doctor can join 5 mins early
    else:
        early_limit = int(settings.VIDEO_JOIN_EARLY_MINUTES) if hasattr(settings, 'VIDEO_JOIN_EARLY_MINUTES') else 10
        
    late_limit = 30  # Hard cutoff at 30 minutes after scheduled time

    window_start = scheduled_at - timedelta(minutes=early_limit)
    window_end = scheduled_at + timedelta(minutes=late_limit)

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
