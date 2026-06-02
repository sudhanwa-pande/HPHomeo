from datetime import datetime

def _review_out(review: dict | None) -> dict | None:
    """Serialize the embedded review sub-document."""
    if not review:
        return None
    from app.utils.time import ensure_utc
    created_at = ensure_utc(review.get("created_at"))
    return {
        "rating": review.get("rating"),
        "comment": review.get("comment"),
        "created_at": created_at.isoformat() if created_at else None,
    }

def _reminder_prefs_out(prefs: dict | None) -> dict | None:
    """Serialize the embedded reminder_preferences sub-document."""
    if not prefs:
        return None
    return {
        "email": prefs.get("email", True),
        "whatsapp": prefs.get("whatsapp", True),
        "timing": prefs.get("timing", ["1h", "1d"]),
    }
