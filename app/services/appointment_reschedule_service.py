from datetime import timedelta

from app.core.config import settings
from app.services.cache_service import invalidate_patient_cache
from app.services.doctor_availability_service import get_available_slots_for_date
from app.services.email_service import safe_send_email, send_cancellation_email, send_reschedule_confirmation
from app.services.refund_service import enqueue_refund_processing
from app.services.whatsapp_service import safe_send_whatsapp, send_patient_appointment_update_whatsapp
from app.utils.appointment_rules import build_patient_access_expiry


async def reschedule_or_cancel_appointments(
    *,
    db,
    doctor_id,
    appointments: list[dict],
    now_utc,
    start_date,
    search_days: int = 30,
) -> tuple[int, int]:
    rescheduled = 0
    cancelled = 0

    for appt in appointments:
        duration_min = int(appt.get("duration_min") or 20)
        new_slot = None
        for offset in range(0, search_days):
            candidate_date = start_date + timedelta(days=offset)
            available, _, _ = await get_available_slots_for_date(
                db=db,
                doctor_id=doctor_id,
                target_date=candidate_date,
                now_utc=now_utc,
                exclude_appointment_id=appt["_id"],
                required_duration_min=duration_min,
            )
            if available:
                new_slot = available[0]
                break

        if new_slot is not None:
            hold_min = int(getattr(settings, "PAYMENT_HOLD_MINUTES", 15))
            update_set = {
                "scheduled_at": new_slot,
                "rescheduled_at": now_utc,
                "reschedule_reason": "doctor_exception",
                "is_rescheduled": True,
                "updated_at": now_utc,
                "patient_access_expires_at": build_patient_access_expiry(
                    new_slot,
                    duration_min=duration_min,
                    video_enabled=appt.get("mode") == "online" or bool(appt.get("video_enabled")),
                ),
            }
            if appt.get("status") == "pending_payment":
                update_set["pending_payment_expires_at"] = now_utc + timedelta(minutes=hold_min)

            res = await db.appointments.update_one({"_id": appt["_id"]}, {"$set": update_set})
            if res.modified_count:
                rescheduled += 1
                if appt.get("patient_user_id"):
                    await invalidate_patient_cache(str(appt["patient_user_id"]))
                updated = await db.appointments.find_one({"_id": appt["_id"]})
                if updated and updated.get("patient_email"):
                    await safe_send_email(send_reschedule_confirmation(updated), "doctor exception reschedule")
                if updated:
                    await safe_send_whatsapp(
                        send_patient_appointment_update_whatsapp(updated, "rescheduled"),
                        "doctor exception reschedule",
                    )
            continue

        cancel_set = {
            "status": "cancelled",
            "cancel_reason": "doctor_unavailable",
            "cancelled_at": now_utc,
            "cancelled_by": "doctor",
            "cancelled_by_id": doctor_id,
            "updated_at": now_utc,
        }
        if appt.get("status") == "pending_payment":
            cancel_set["payment_status"] = "failed"
            cancel_set["pending_payment_expires_at"] = None
        if appt.get("payment_choice") == "pay_now" and appt.get("payment_status") == "paid":
            cancel_set["refund_status"] = "pending"

        res = await db.appointments.update_one({"_id": appt["_id"]}, {"$set": cancel_set})
        if res.modified_count:
            cancelled += 1
            if appt.get("patient_user_id"):
                await invalidate_patient_cache(str(appt["patient_user_id"]))
            cancelled_doc = await db.appointments.find_one({"_id": appt["_id"]})
            if cancelled_doc and cancelled_doc.get("patient_email"):
                await safe_send_email(send_cancellation_email(cancelled_doc), "doctor exception cancellation")
            if cancelled_doc:
                await safe_send_whatsapp(
                    send_patient_appointment_update_whatsapp(cancelled_doc, "cancelled"),
                    "doctor exception cancellation",
                )
                if cancelled_doc.get("refund_status") == "pending":
                    enqueue_refund_processing(str(appt["_id"]))

    return rescheduled, cancelled
