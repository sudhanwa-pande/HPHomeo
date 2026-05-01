"""
Patient features: Notes, Reviews, Reminder Preferences.

All endpoints are scoped to the authenticated patient and their own appointments.
Data is stored directly on the appointment document (embedded, not separate collections)
to keep reads cheap — a single findOne fetches everything the patient detail page needs.
"""

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.patient_auth_routes import get_current_patient
from app.schemas.patient_feature_schema import (
    PatientNotesUpdateIn,
    ReminderPreferencesUpdateIn,
    ReviewCreateIn,
)
from app.services.cache_service import invalidate_patient_cache
from app.utils.time import utc_now, ensure_utc

router = APIRouter(prefix="/patient", tags=["Patient Features"])

# ── helpers ──────────────────────────────────────────────

MUTABLE_STATUSES = ["pending_payment", "confirmed", "completed", "cancelled", "no_show"]


async def _get_own_appointment(db, appointment_id: str, patient_user_id):
    """Fetch an appointment owned by this patient. Raises 404 if not found."""
    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one(
        {
            "_id": appt_oid,
            "patient_user_id": patient_user_id,
            "status": {"$in": MUTABLE_STATUSES},
        }
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")
    return appt


# ═════════════════════════════════════════════════════════
#  PATIENT NOTES
# ═════════════════════════════════════════════════════════

@router.put(
    "/appointments/{appointment_id}/notes",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def update_patient_notes(
    request: Request,
    appointment_id: str,
    payload: PatientNotesUpdateIn,
    current=Depends(get_current_patient),
):
    """
    Save or update the patient's personal notes on an appointment.
    These notes are visible to the patient and to the doctor (read-only on doctor side).
    """
    db = get_db()
    appt = await _get_own_appointment(db, appointment_id, current["_id"])
    now = utc_now()

    notes_text = payload.notes.strip()

    await db.appointments.update_one(
        {"_id": appt["_id"]},
        {
            "$set": {
                "patient_notes": notes_text,
                "patient_notes_updated_at": now,
                "updated_at": now,
            }
        },
    )

    await invalidate_patient_cache(str(current["_id"]))

    return {
        "message": "notes_saved",
        "appointment_id": appointment_id,
        "notes": notes_text,
        "updated_at": now.isoformat(),
    }


# ═════════════════════════════════════════════════════════
#  RATING & REVIEW
# ═════════════════════════════════════════════════════════

@router.post(
    "/appointments/{appointment_id}/review",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def create_review(
    request: Request,
    appointment_id: str,
    payload: ReviewCreateIn,
    current=Depends(get_current_patient),
):
    """
    Submit a review for a completed appointment.
    One review per appointment — returns 409 if already reviewed.
    """
    db = get_db()
    appt = await _get_own_appointment(db, appointment_id, current["_id"])
    now = utc_now()

    # Only completed appointments can be reviewed
    if appt.get("status") != "completed":
        raise HTTPException(
            status_code=409,
            detail="Only completed appointments can be reviewed",
        )

    # Prevent duplicate reviews
    if appt.get("review"):
        raise HTTPException(
            status_code=409,
            detail="You have already reviewed this appointment",
        )

    comment = payload.comment.strip() if payload.comment else None

    review_doc = {
        "rating": payload.rating,
        "comment": comment,
        "created_at": now,
        "patient_user_id": current["_id"],
        "patient_name": current.get("full_name"),
    }

    await db.appointments.update_one(
        {"_id": appt["_id"], "review": {"$exists": False}},
        {
            "$set": {
                "review": review_doc,
                "updated_at": now,
            }
        },
    )

    # Double-check the update succeeded (race condition guard)
    updated = await db.appointments.find_one(
        {"_id": appt["_id"]}, {"review": 1}
    )
    if not updated or not updated.get("review"):
        # Fallback: maybe the $exists guard failed because review was already set
        # between our check and the update
        raise HTTPException(
            status_code=409,
            detail="You have already reviewed this appointment",
        )

    await invalidate_patient_cache(str(current["_id"]))

    return {
        "message": "review_submitted",
        "appointment_id": appointment_id,
        "review": {
            "rating": payload.rating,
            "comment": comment,
            "created_at": now.isoformat(),
        },
    }


# ═════════════════════════════════════════════════════════
#  REMINDER PREFERENCES
# ═════════════════════════════════════════════════════════

@router.put(
    "/appointments/{appointment_id}/reminders",
    dependencies=[rl(settings.RL_PATIENT_WRITE_TIMES, settings.RL_PATIENT_WRITE_SECONDS)],
)
async def update_reminder_preferences(
    request: Request,
    appointment_id: str,
    payload: ReminderPreferencesUpdateIn,
    current=Depends(get_current_patient),
):
    """
    Update reminder preferences for an appointment.
    Controls which channels (email/WhatsApp) and timing (1h/1d before) are used.
    Only applies to upcoming appointments (confirmed or pending_payment).
    """
    db = get_db()
    appt = await _get_own_appointment(db, appointment_id, current["_id"])
    now = utc_now()

    # Only allow setting reminders on active (not completed/cancelled) appointments
    if appt.get("status") not in ("confirmed", "pending_payment"):
        raise HTTPException(
            status_code=409,
            detail="Reminders can only be set for upcoming appointments",
        )

    # Ensure appointment is in the future
    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at and scheduled_at <= now:
        raise HTTPException(
            status_code=409,
            detail="Cannot set reminders for past appointments",
        )

    prefs_doc = {
        "email": payload.email,
        "whatsapp": payload.whatsapp,
        "timing": payload.timing,
        "updated_at": now,
    }

    await db.appointments.update_one(
        {"_id": appt["_id"]},
        {
            "$set": {
                "reminder_preferences": prefs_doc,
                "updated_at": now,
            }
        },
    )

    await invalidate_patient_cache(str(current["_id"]))

    return {
        "message": "reminders_updated",
        "appointment_id": appointment_id,
        "reminder_preferences": {
            "email": payload.email,
            "whatsapp": payload.whatsapp,
            "timing": payload.timing,
        },
    }
