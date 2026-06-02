import asyncio
from datetime import timedelta
import logging

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from bson import ObjectId

from app.core.config import settings
from app.core.database import get_db
from app.core.redis import get_redis
from app.routes.auth_routes import get_current_doctor
from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
from app.services.email_service import safe_send_email, send_prescription_email
from app.services.event_bus import (
    notify_appointment,
    notify_doctor,
    notify_patient,
    EVENT_APPOINTMENT_COMPLETED,
    EVENT_APPOINTMENT_NO_SHOW,
    EVENT_CALL_STATE_CHANGED,
)
from app.services.whatsapp_service import safe_send_whatsapp, send_patient_prescription_whatsapp
from app.services.call_state_machine import _emit_state_change
from app.utils.time import utc_now, ensure_utc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/doctor/appointments", tags=["Doctor Appointment Actions"])


@router.post("/{appointment_id}/complete")
async def complete_appointment(
    appointment_id: str,
    background_tasks: BackgroundTasks,
    doctor=Depends(get_current_doctor),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one({"_id": appt_oid, "doctor_id": doctor["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    # ✅ allow confirmed only
    if appt.get("status") != "confirmed":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot complete appointment with status={appt.get('status')}",
        )

    prescription = await db.prescriptions.find_one(
        {"appointment_id": appt_oid},
        {"is_draft": 1, "pdf_url": 1, "rx_id": 1},
    )
    if not prescription or prescription.get("is_draft", True):
        raise HTTPException(
            status_code=409,
            detail="Finalize the prescription before completing this appointment",
        )

    now = utc_now()
    scheduled_at = ensure_utc(appt.get("scheduled_at"))

    follow_up_days = int(getattr(settings, "FOLLOW_UP_DAYS", 7))
    follow_up_until = now + timedelta(days=follow_up_days)

    set_doc = {
        "status": "completed",
        "completed_at": now,
        "updated_at": now,
        "call_status": "ended",
        "patient_participant": None,
        "doctor_participant": None,
        "call_participant_count": 0,
        "call_ended_at": now,
    }

    # Only "new" appointments can grant follow-up eligibility
    if appt.get("appointment_type", "new") == "new":
        set_doc.update(
            {
                "is_follow_up_eligible": True,
                "follow_up_eligible_until": follow_up_until,
            }
        )

    update_res = await db.appointments.update_one(
        {"_id": appt_oid, "doctor_id": doctor["_id"], "status": "confirmed"},
        {"$set": set_doc},
    )
    if update_res.modified_count == 0:
        raise HTTPException(status_code=409, detail="Appointment state changed. Please retry.")

    if scheduled_at:
        await invalidate_doctor_cache(str(doctor["_id"]), day=scheduled_at.date().isoformat())
    if appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))

    # ─── Call Cleanup ───
    from app.services.call_state_machine import cleanup_call_resources
    await cleanup_call_resources(appointment_id, str(doctor["_id"]), appt.get("video_room"))

    # SSE: notify patient and appointment channel that appointment is completed
    event_data = {
        "appointment_id": appointment_id,
        "status": "completed",
        "completed_at": now.isoformat(),
        "call_status": "ended",
    }
    await notify_appointment(appointment_id, EVENT_APPOINTMENT_COMPLETED, event_data)
    if appt.get("patient_user_id"):
        await notify_patient(str(appt["patient_user_id"]), EVENT_APPOINTMENT_COMPLETED, event_data)

    # Also notify call state changed to ended
    await _emit_state_change(appt, "ended", 0, [], now)

    # ── Fire prescription notifications (WhatsApp + email) in the background ──
    pdf_url = prescription.get("pdf_url") if prescription else None
    rx_id = str(prescription.get("rx_id", "RX")) if prescription else "RX"
    if pdf_url:
        background_tasks.add_task(
            safe_send_whatsapp,
            send_patient_prescription_whatsapp(appt, str(pdf_url), rx_id),
            f"prescription WA appt={appointment_id}",
        )
        if appt.get("patient_email"):
            background_tasks.add_task(
                safe_send_email,
                send_prescription_email(appt, str(pdf_url), rx_id),
                f"prescription email appt={appointment_id}",
            )

    return {
        "message": "completed",
        "appointment_id": appointment_id,
        "completed_at": now.isoformat(),
        "is_follow_up_eligible": set_doc.get("is_follow_up_eligible", False),
        "follow_up_eligible_until": (
            follow_up_until.isoformat() if set_doc.get("is_follow_up_eligible", False) else None
        ),
    }


@router.post("/{appointment_id}/cancel")
async def cancel_appointment(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    raise HTTPException(
        status_code=403,
        detail=(
            "Doctors cannot manually cancel appointments. "
            "Patients must cancel through the authenticated patient flow or magic link. "
            "For schedule changes, update availability or create an exception so impacted appointments "
            "are auto-rescheduled for up to 30 days before cancellation."
        ),
    )


@router.put("/{appointment_id}/no-show")
async def mark_no_show(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid appointment_id")

    appt = await db.appointments.find_one({"_id": appt_oid, "doctor_id": doctor["_id"]})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    # ✅ allow confirmed
    if appt.get("status") != "confirmed":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot mark no_show from status={appt.get('status')}",
        )

    now = utc_now()

    # ✅ no_show should never grant follow-up eligibility
    update_res = await db.appointments.update_one(
        {"_id": appt_oid, "doctor_id": doctor["_id"], "status": "confirmed"},
        {
            "$set": {
                "status": "no_show",
                "no_show_at": now,
                "updated_at": now,
                "is_follow_up_eligible": False,
                "follow_up_eligible_until": None,
                "call_status": "ended",
                "patient_participant": None,
                "doctor_participant": None,
                "call_participant_count": 0,
                "call_ended_at": now,
            }
        },
    )
    if update_res.modified_count == 0:
        raise HTTPException(status_code=409, detail="Appointment state changed. Please retry.")

    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(doctor["_id"]), day=scheduled_at.date().isoformat())
    if appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))

    # ─── Call Cleanup ───
    from app.services.call_state_machine import cleanup_call_resources
    await cleanup_call_resources(appointment_id, str(doctor["_id"]), appt.get("video_room"))

    # SSE: notify patient and appointment channel of no-show marking
    event_data = {
        "appointment_id": appointment_id,
        "status": "no_show",
        "no_show_at": now.isoformat(),
        "call_status": "ended",
    }
    await notify_appointment(appointment_id, EVENT_APPOINTMENT_NO_SHOW, event_data)
    if appt.get("patient_user_id"):
        await notify_patient(str(appt["patient_user_id"]), EVENT_APPOINTMENT_NO_SHOW, event_data)

    # Also notify call state changed to ended
    await _emit_state_change(appt, "ended", 0, [], now)

    return {
        "message": "marked_no_show",
        "appointment_id": appointment_id,
        "no_show_at": now.isoformat(),
    }
