import logging
from datetime import timedelta
from typing import Any

from bson import ObjectId

from app.core.config import settings
from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
from app.services.email_service import safe_send_email, send_reminder_email, send_payment_confirmation, send_doctor_new_booking
from app.services.whatsapp_service import safe_send_whatsapp, send_patient_appointment_update_whatsapp
from app.services.video_service import ensure_video_room
from app.routes.receipt_routes import generate_receipt_for_appointment
from app.services.cloudinary_service import cloudinary_service
from app.services.event_bus import notify_doctor, EVENT_PAYMENT_CONFIRMED, EVENT_APPOINTMENT_BOOKED
from app.services.razorpay_service import fetch_order, fetch_order_payments, fetch_refund, initiate_refund
from app.utils.time import utc_now
from app.worker.base_task import BaseTaskWithDLQ
from app.worker.celery_app import celery_app
from app.worker.db import get_task_db
from app.worker.task_base import async_task

logger = logging.getLogger(__name__)
REFUND_PROCESSING_STALE_MINUTES = 45
REFUND_MAX_INITIATION_ATTEMPTS = 5


def _refund_processing_started_at_is_stale(appt: dict[str, Any], stale_before) -> bool:
    started_at = appt.get("refund_processing_started_at")
    return not started_at or started_at <= stale_before


def _extract_refund_failure_reason(refund_doc: dict[str, Any]) -> str | None:
    for key in (
        "error_description",
        "description",
        "failure_reason",
        "reason",
        "status_description",
    ):
        value = refund_doc.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _resolve_refund_amount_paise(appt: dict[str, Any]) -> int | None:
    amount_paise = appt.get("payment_amount_paise")
    if amount_paise is not None:
        return int(amount_paise)

    fee = appt.get("consultation_fee")
    if fee is None:
        return None
    return int(fee) * 100


async def _invalidate_refund_related_caches(appt: dict[str, Any]) -> None:
    scheduled_at = appt.get("scheduled_at")
    doctor_id = appt.get("doctor_id")
    patient_user_id = appt.get("patient_user_id")

    if doctor_id and scheduled_at:
        try:
            await invalidate_doctor_cache(str(doctor_id), day=scheduled_at.date().isoformat())
        except Exception:
            logger.exception("Failed to invalidate doctor cache for refund appointment_id=%s", appt.get("_id"))  # codeql[py/clear-text-logging-sensitive-data]

    if patient_user_id:
        try:
            await invalidate_patient_cache(str(patient_user_id))
        except Exception:
            logger.exception("Failed to invalidate patient cache for refund appointment_id=%s", appt.get("_id"))  # codeql[py/clear-text-logging-sensitive-data]


async def _mark_refund_terminal_failure(
    db,
    appt: dict[str, Any],
    *,
    now,
    reason: str | None = None,
) -> None:
    update_set = {
        "refund_status": "failed",
        "refund_processing_started_at": None,
        "updated_at": now,
    }
    if reason:
        update_set["refund_failure_reason"] = reason
    if not appt.get("payment_status") or appt.get("payment_status") == "refunded":
        update_set["payment_status"] = "paid"

    await db.appointments.update_one(
        {"_id": appt["_id"]},
        {"$set": update_set},
    )
    await _invalidate_refund_related_caches(appt)


async def _mark_refund_processed_from_provider(
    db,
    appt: dict[str, Any],
    *,
    refund_id: str | None,
    amount_paise: int | None,
    now,
) -> None:
    await db.appointments.update_one(
        {"_id": appt["_id"]},
        {
            "$set": {
                "refund_status": "processed",
                "refund_id": refund_id or appt.get("refund_id"),
                "refund_amount_paise": amount_paise if amount_paise is not None else appt.get("refund_amount_paise"),
                "payment_status": "refunded",
                "refund_processing_started_at": None,
                "updated_at": now,
            }
        },
    )
    await _invalidate_refund_related_caches(appt)


async def _attempt_refund_for_appointment(
    db,
    appt: dict[str, Any],
    *,
    now,
    processing_stale_before,
) -> bool:
    if appt.get("payment_status") != "paid":
        return False
    if not appt.get("payment_id") or appt.get("refund_id"):
        return False

    refund_status = appt.get("refund_status")
    if refund_status == "pending":
        allowed_criteria: dict[str, Any] = {"refund_status": "pending"}
    elif refund_status == "processing" and _refund_processing_started_at_is_stale(appt, processing_stale_before):
        allowed_criteria = {
            "refund_status": "processing",
            "$or": [
                {"refund_processing_started_at": {"$lte": processing_stale_before}},
                {"refund_processing_started_at": {"$exists": False}},
            ],
        }
    else:
        return False

    res = await db.appointments.update_one(
        {
            "_id": appt["_id"],
            "refund_id": {"$exists": False},
            **allowed_criteria,
        },
        {
            "$set": {
                "refund_status": "processing",
                "refund_processing_started_at": now,
                "updated_at": now,
            }
        },
    )
    if res.modified_count == 0:
        return False

    amount_paise = _resolve_refund_amount_paise(appt)
    if amount_paise is None:
        logger.warning(
            "Refund amount missing for appointment_id=%s; marking refund failed",
            appt["_id"], # codeql[py/clear-text-logging-sensitive-data]
        )
        await _mark_refund_terminal_failure(
            db,
            appt,
            now=now,
            reason="missing_refund_amount",
        )
        return False

    try:
        refund_resp = await initiate_refund(
            payment_id=appt["payment_id"],
            amount_paise=amount_paise,
            idempotency_key=f"refund-{appt['_id']}",
        )
        await db.appointments.update_one(
            {"_id": appt["_id"], "refund_status": "processing"},
            {
                "$set": {
                    "refund_status": "processing",
                    "refund_id": refund_resp.get("id"),
                    "refund_amount_paise": amount_paise,
                    "refund_requested_at": now,
                    "refund_processing_started_at": None,
                    "refund_failure_reason": None,
                    "updated_at": now,
                }
            },
        )
        appt["refund_status"] = "processing"
        appt["refund_id"] = refund_resp.get("id")
        appt["refund_amount_paise"] = amount_paise
        await _invalidate_refund_related_caches(appt)
        return True
    except Exception as exc:
        next_attempt_count = int(appt.get("refund_attempt_count") or 0) + 1
        terminal_failure = next_attempt_count >= REFUND_MAX_INITIATION_ATTEMPTS
        await db.appointments.update_one(
            {"_id": appt["_id"], "refund_status": "processing"},
            {
                "$set": {
                    "refund_status": "failed" if terminal_failure else "pending",
                    "refund_processing_started_at": None,
                    "refund_last_error": str(exc),
                    "refund_failure_reason": str(exc) if terminal_failure else appt.get("refund_failure_reason"),
                    "updated_at": now,
                },
                "$inc": {"refund_attempt_count": 1},
            },
        )
        appt["refund_status"] = "failed" if terminal_failure else "pending"
        logger.exception(
            "Refund failed for appointment_id=%s: %s",
            appt["_id"], # codeql[py/clear-text-logging-sensitive-data]
            exc,
            extra={ # codeql[py/clear-text-logging-sensitive-data]
                "appointment_id": str(appt["_id"]),
                "payment_id": appt.get("payment_id"),
                "refund_id": appt.get("refund_id"),
            }
        )
        await _invalidate_refund_related_caches(appt)
        return False


async def _reconcile_stale_processing_refunds(db, *, now, processing_stale_before) -> tuple[int, int]:
    appointments = await db.appointments.find(
        {
            "refund_status": "processing",
            "refund_id": {"$exists": True, "$ne": None},
            "$or": [
                {"refund_requested_at": {"$lte": processing_stale_before}},
                {"updated_at": {"$lte": processing_stale_before}},
            ],
        }
    ).to_list(length=100)

    processed = 0
    failed = 0
    for appt in appointments:
        try:
            refund_doc = await fetch_refund(str(appt["refund_id"]))
        except Exception:
            logger.exception(
                "Refund reconciliation fetch failed for appointment_id=%s refund_id=%s",
                appt["_id"], # codeql[py/clear-text-logging-sensitive-data]
                appt.get("refund_id"), # codeql[py/clear-text-logging-sensitive-data]
            )
            continue

        provider_status = str(refund_doc.get("status") or "").strip().lower()
        if provider_status == "processed":
            await _mark_refund_processed_from_provider(
                db,
                appt,
                refund_id=refund_doc.get("id"),
                amount_paise=refund_doc.get("amount"),
                now=now,
            )
            processed += 1
        elif provider_status == "failed":
            await _mark_refund_terminal_failure(
                db,
                appt,
                now=now,
                reason=_extract_refund_failure_reason(refund_doc) or "provider_refund_failed",
            )
            failed += 1

    return processed, failed


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.expire_pending_payments",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def expire_pending_payments():
    db = await get_task_db()
    now = utc_now()
    result = await db.appointments.update_many(
        {
            "status": "pending_payment",
            "pending_payment_expires_at": {"$lte": now},
        },
        {
            "$set": {
                "status": "cancelled",
                "payment_status": "failed",
                "cancel_reason": "payment_timeout",
                "cancelled_at": now,
                "cancelled_by": "system",
                "updated_at": now,
            }
        },
    )
    logger.info("expire_pending_payments: expired=%d", result.modified_count)


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.auto_no_show",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def auto_no_show():
    db = await get_task_db()
    now = utc_now()
    cutoff = now - timedelta(minutes=30)
    
    cursor = db.appointments.find(
        {
            "status": "confirmed",
            "scheduled_at": {"$lte": cutoff},
            "call_connected_at": None,
            "video_enabled": True,
        }
    )
    docs = await cursor.to_list(length=500)
    if not docs:
        return

    from app.services.event_bus import notify_appointment, notify_patient
    from app.core.config import settings

    count = 0
    for doc in docs:
        res = await db.appointments.update_one(
            {"_id": doc["_id"], "status": "confirmed"},
            {
                "$set": {
                    "status": "no_show",
                    "call_status": "ended",
                    "no_show_at": now,
                    "updated_at": now,
                }
            },
        )
        if res.modified_count > 0:
            count += 1
            appt_id_str = str(doc["_id"])
            event_data = {
                "appointment_id": appt_id_str,
                "status": "no_show",
                "call_status": "ended",
                "no_show_at": now.isoformat(),
            }
            # Force immediate SSE updates to waiting participants
            await notify_appointment(appt_id_str, "appointment_no_show", event_data)
            await notify_appointment(appt_id_str, "call_state_changed", event_data)
            
            if doc.get("patient_user_id"):
                await notify_patient(str(doc["patient_user_id"]), "appointment_no_show", event_data)

            # Forcefully terminate the WebRTC connection
            room_name = doc.get("video_room")
            if room_name and settings.VIDEO_ENABLED:
                try:
                    from livekit import api
                    async with api.LiveKitAPI(
                        settings.LIVEKIT_URL, 
                        settings.LIVEKIT_API_KEY, 
                        settings.LIVEKIT_API_SECRET.get_secret_value()
                    ) as lkapi:
                        await lkapi.room.delete_room(api.DeleteRoomRequest(room=room_name))
                except Exception as e:
                    logger.warning("auto_no_show: failed to delete room %s: %s", room_name, str(e))

    logger.info("auto_no_show: marked=%d", count)


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.send_24hr_reminder_emails",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def send_24hr_reminder_emails():
    db = await get_task_db()
    now = utc_now()
    window_start = now + timedelta(hours=23)
    window_end = now + timedelta(hours=25)
    appointments = await db.appointments.find(
        {
            "status": "confirmed",
            "scheduled_at": {"$gte": window_start, "$lt": window_end},
            "email_reminder_24hr_sent": {"$ne": True},
            "patient_email": {"$exists": True, "$ne": None},
        }
    ).to_list(length=500)

    sent = 0
    for appt in appointments:
        # Send before marking: a missed reminder is worse than a duplicate.
        # If the send fails, the flag is never set and the next run retries.
        # If the DB write fails after a successful send, the patient receives
        # two reminders — acceptable for a low-volume scheduled notification.
        sent_ok = await safe_send_email(send_reminder_email(appt), "24hr reminder")
        if not sent_ok:
            continue
        await db.appointments.update_one(
            {"_id": appt["_id"], "email_reminder_24hr_sent": {"$ne": True}},
            {"$set": {"email_reminder_24hr_sent": True, "updated_at": now}},
        )
        sent += 1
    logger.info("send_24hr_reminder_emails: sent=%d", sent)


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.send_12hr_whatsapp_reminders",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def send_12hr_whatsapp_reminders():
    db = await get_task_db()
    now = utc_now()
    lead_hours = max(int(getattr(settings, "WHATSAPP_REMINDER_LEAD_HOURS", 12)), 1)
    target_time = now + timedelta(hours=lead_hours)
    window_start = target_time - timedelta(minutes=15)
    window_end = target_time + timedelta(minutes=15)

    appointments = await db.appointments.find(
        {
            "status": "confirmed",
            "scheduled_at": {"$gte": window_start, "$lt": window_end},
            "wa_reminder_12hr_sent": {"$ne": True},
            "patient_phone": {"$exists": True, "$ne": None},
        }
    ).to_list(length=500)

    sent = 0
    for appt in appointments:
        # Send before marking: a missed reminder is worse than a duplicate.
        sent_ok = await safe_send_whatsapp(
            send_patient_appointment_update_whatsapp(appt, "reminder_12hr"),
            "12hr whatsapp reminder",
        )
        if not sent_ok:
            continue
        await db.appointments.update_one(
            {"_id": appt["_id"], "wa_reminder_12hr_sent": {"$ne": True}},
            {"$set": {"wa_reminder_12hr_sent": True, "updated_at": now}},
        )
        sent += 1

    logger.info("send_12hr_whatsapp_reminders: sent=%d", sent)


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.process_pending_refunds",
    base=BaseTaskWithDLQ,
    bind=True,
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def process_pending_refunds(self):
    db = await get_task_db()
    now = utc_now()
    processing_stale_before = now - timedelta(minutes=REFUND_PROCESSING_STALE_MINUTES)

    # Recover appointments that were moved to "processing" but never completed
    # (for example: worker crash before refund_id persisted).
    await db.appointments.update_many(
        {
            "refund_status": "processing",
            "refund_id": {"$exists": False},
            "$or": [
                {"refund_processing_started_at": {"$lte": processing_stale_before}},
                {"refund_processing_started_at": {"$exists": False}},
            ],
        },
        {
            "$set": {
                "refund_status": "pending",
                "updated_at": now,
            }
        },
    )

    appointments = await db.appointments.find(
        {
            "payment_status": "paid",
            "payment_id": {"$exists": True, "$ne": None},
            "refund_id": {"$exists": False},
            "$or": [
                {"refund_status": "pending"},
                {
                    "refund_status": "processing",
                    "$or": [
                        {"refund_processing_started_at": {"$lte": processing_stale_before}},
                        {"refund_processing_started_at": {"$exists": False}},
                    ],
                },
            ],
        }
    ).to_list(length=100)

    failed = 0
    initiated = 0
    for appt in appointments:
        if await _attempt_refund_for_appointment(
            db,
            appt,
            now=now,
            processing_stale_before=processing_stale_before,
        ):
            initiated += 1
        else:
            failed += 1

    reconciled_processed, reconciled_failed = await _reconcile_stale_processing_refunds(
        db,
        now=now,
        processing_stale_before=processing_stale_before,
    )

    logger.info(
        "process_pending_refunds: scanned=%d initiated=%d failed=%d reconciled_processed=%d reconciled_failed=%d",
        len(appointments),
        initiated,
        failed,
        reconciled_processed,
        reconciled_failed,
    )


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.process_refund_for_appointment",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    rate_limit="10/m",
)
@async_task
async def process_refund_for_appointment(appointment_id: str):
    db = await get_task_db()
    now = utc_now()
    processing_stale_before = now - timedelta(minutes=REFUND_PROCESSING_STALE_MINUTES)

    try:
        appt_oid = ObjectId(str(appointment_id))
    except Exception: # codeql[py/clear-text-logging-sensitive-data]
        logger.warning("process_refund_for_appointment: invalid appointment_id=%s", appointment_id)
        return

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt: # codeql[py/clear-text-logging-sensitive-data]
        logger.warning("process_refund_for_appointment: appointment not found id=%s", appointment_id)
        return

    initiated = await _attempt_refund_for_appointment(
        db,
        appt,
        now=now,
        processing_stale_before=processing_stale_before,
    )
    if initiated: # codeql[py/clear-text-logging-sensitive-data]
        logger.info("process_refund_for_appointment: initiated appointment_id=%s", appointment_id)
        return




@celery_app.task(
    name="app.worker.tasks.appointment_tasks.expire_followup_eligibility",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def expire_followup_eligibility():
    db = await get_task_db()
    now = utc_now()
    result = await db.appointments.update_many(
        {
            "is_follow_up_eligible": True,
            "follow_up_eligible_until": {"$lte": now},
        },
        {
            "$set": {
                "is_follow_up_eligible": False,
                "updated_at": now,
            }
        },
    )
    logger.info("expire_followup_eligibility: expired=%d", result.modified_count)


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.cleanup_stale_video_rooms",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def cleanup_stale_video_rooms():
    """Clean up stale call states.

    Handles two cases:
    1. Disconnected calls that exceeded the timeout (Redis key expired but
       Celery didn't fire in time — safety net).
    2. Calls stuck in waiting/connected for >15 minutes (zombie calls from
       missed webhooks or abandoned sessions).
    """
    db = await get_task_db()
    now = utc_now()

    # 1. End any disconnected calls past the timeout window
    disconnect_timeout = int(getattr(settings, "CALL_DISCONNECT_TIMEOUT_SECONDS", 300))
    disconnect_cutoff = now - timedelta(seconds=disconnect_timeout + 60)  # +60s grace
    disconnected_appts = await db.appointments.find(
        {
            "call_status": "disconnected",
            "call_disconnected_at": {"$lte": disconnect_cutoff},
        }
    ).to_list(length=100)

    from app.services.call_state_machine import _emit_state_change
    disconnected_ended = 0
    for appt in disconnected_appts:
        res = await db.appointments.update_one(
            {"_id": appt["_id"], "call_status": "disconnected"},
            {
                "$set": {
                    "call_status": "ended",
                    "patient_participant": None,
                    "doctor_participant": None,
                    "call_participant_count": 0,
                    "call_ended_at": now,
                    "updated_at": now,
                }
            }
        )
        if res.modified_count > 0:
            disconnected_ended += 1
            updated = {**appt, "call_status": "ended", "call_participant_count": 0}
            await _emit_state_change(updated, "ended", 0, [], now)

    # 2. Safety net: end zombie calls stuck for >15 minutes
    stale_cutoff = now - timedelta(minutes=15)
    stale_appts = await db.appointments.find(
        {
            "call_status": {"$in": ["waiting", "connected"]},
            "$or": [
                {"call_last_activity_at": {"$lte": stale_cutoff}},
                {"call_last_activity_at": {"$exists": False}, "updated_at": {"$lte": stale_cutoff}},
            ],
        }
    ).to_list(length=100)
    
    stale_ended = 0
    for appt in stale_appts:
        res = await db.appointments.update_one(
            {"_id": appt["_id"], "call_status": {"$in": ["waiting", "connected"]}},
            {
                "$set": {
                    "call_status": "ended",
                    "patient_participant": None,
                    "doctor_participant": None,
                    "call_participant_count": 0,
                    "call_ended_at": now,
                    "updated_at": now,
                }
            }
        )
        if res.modified_count > 0:
            stale_ended += 1
            updated = {**appt, "call_status": "ended", "call_participant_count": 0}
            await _emit_state_change(updated, "ended", 0, [], now)

    logger.info(
        "cleanup_stale_video_rooms: disconnected_ended=%d stale_ended=%d",
        disconnected_ended,
        stale_ended,
    )


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.reconcile_captured_payments",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
@async_task
async def reconcile_captured_payments():
    """Poll Razorpay for appointments whose webhook was never received.

    Targets appointments that:
    - Are still pending_payment
    - Have a payment_order_id (order was created, user reached checkout)
    - Whose hold window expired at least 30 minutes ago

    If Razorpay shows the order as paid and the captured amount matches the
    stored fee, the appointment is confirmed exactly as the webhook handler
    would have done. If the amount mismatches, the appointment is left alone
    and an error is logged for manual review — it will be cancelled by
    expire_pending_payments on the next run.
    """
    db = await get_task_db()
    now = utc_now() # codeql[py/clear-text-logging-sensitive-data]
    # Only look at appointments whose hold window has been closed for at
    # least 30 minutes — if the webhook was going to arrive it would have.
    stale_before = now - timedelta(minutes=30)

    appointments = await db.appointments.find(
        {
            "status": "pending_payment",
            "payment_provider": "razorpay",
            "payment_order_id": {"$exists": True, "$ne": None},
            "pending_payment_expires_at": {"$lte": stale_before},
        }
    ).to_list(length=50)

    confirmed = 0
    skipped = 0
    for appt in appointments: # codeql[py/clear-text-logging-sensitive-data]
        order_id = appt.get("payment_order_id")
        appt_id = appt["_id"]
        try:
            order = await fetch_order(order_id)
        except Exception:
            logger.exception(
                "reconcile_captured_payments: fetch_order failed "
                "appointment_id=%s order_id=%s",
                appt_id, order_id,
            ) # codeql[py/clear-text-logging-sensitive-data]
            skipped += 1
            continue

        if order.get("status") != "paid":
            # Not paid — expire_pending_payments will cancel it.
            continue

        # Fetch the individual payment to get payment_id and captured amount.
        try:
            payments = await fetch_order_payments(order_id)
        except Exception:
            logger.exception(
                "reconcile_captured_payments: fetch_order_payments failed "
                "appointment_id=%s order_id=%s",
                appt_id, order_id, # codeql[py/clear-text-logging-sensitive-data]
            )
            skipped += 1
            continue

        captured = [p for p in payments if p.get("status") == "captured"]
        if not captured:
            logger.warning(
                "reconcile_captured_payments: order paid but no captured payment "
                "appointment_id=%s order_id=%s",
                appt_id, order_id,
            )
            skipped += 1
            continue

        payment = captured[0]
        payment_id = payment.get("id")
        amount_paise = payment.get("amount")
        expected_paise = int(appt.get("consultation_fee", 0)) * 100

        if int(amount_paise or 0) != expected_paise:
            logger.error(
                "reconcile_captured_payments: AMOUNT MISMATCH appointment_id=%s "
                "expected_paise=%s got_paise=%s payment_id=%s — skipping, "
                "will be cancelled by expire_pending_payments", # codeql[py/clear-text-logging-sensitive-data]
                appt_id, expected_paise, amount_paise, payment_id,
            )
            skipped += 1
            continue

        res = await db.appointments.update_one(
            {"_id": appt_id, "status": "pending_payment"},
            {
                "$set": {
                    "status": "confirmed",
                    "confirmed_at": now,
                    "payment_status": "paid",
                    "payment_provider": "razorpay",
                    "payment_id": payment_id,
                    "payment_amount_paise": amount_paise,
                    "pending_payment_expires_at": None,
                    "updated_at": now,
                }
            },
        )
        if res.modified_count:
            logger.info(
                "reconcile_captured_payments: confirmed appointment_id=%s "
                "payment_id=%s via API poll",
                appt_id, payment_id,
            )
            confirmed += 1
            
            # ── Trigger Emails & Notifications (Fallback) ──
            try:
                from datetime import timezone
                updated_appt = await db.appointments.find_one({"_id": appt_id})
                if updated_appt:
                    if updated_appt.get("mode") == "online" and updated_appt.get("video_enabled", True):
                        try:
                            await ensure_video_room(db, updated_appt)
                            updated_appt = await db.appointments.find_one({"_id": appt_id})
                        except Exception:
                            logger.warning("reconcile_captured_payments: ensure_video_room failed", exc_info=True)

                    receipt_pdf_bytes = None
                    receipt_pdf_url = None
                    receipt_id = None
                    try:
                        receipt, receipt_pdf_bytes = await generate_receipt_for_appointment(
                            db, updated_appt, payment_id=payment_id,
                        )
                        raw_receipt_pdf_url = receipt.get("pdf_url")
                        if raw_receipt_pdf_url:
                            receipt_pdf_url = cloudinary_service.pdf_view_url(url=raw_receipt_pdf_url)
                        receipt_id = receipt.get("receipt_id")
                    except Exception:
                        logger.warning("reconcile_captured_payments: generate_receipt failed", exc_info=True)

                    if updated_appt.get("patient_email"):
                        await safe_send_email(
                            send_payment_confirmation(
                                updated_appt,
                                receipt_pdf_bytes=receipt_pdf_bytes,
                                receipt_id=receipt_id,
                            ),
                            "payment confirmation",
                        )

                    await safe_send_whatsapp(
                        send_patient_appointment_update_whatsapp(
                            updated_appt,
                            "payment_confirmed",
                            receipt_pdf_url=receipt_pdf_url,
                            receipt_id=receipt_id,
                        ),
                        "payment confirmation",
                    )

                    if updated_appt.get("doctor_email"):
                        await safe_send_email(
                            send_doctor_new_booking(updated_appt, updated_appt["doctor_email"]),
                            "doctor new booking",
                        )

                    scheduled_at = updated_appt.get("scheduled_at")
                    if scheduled_at:
                        if scheduled_at.tzinfo is None:
                            scheduled_at = scheduled_at.replace(tzinfo=timezone.utc) # codeql[py/clear-text-logging-sensitive-data]
                        await invalidate_doctor_cache(str(updated_appt.get("doctor_id")), day=scheduled_at.date().isoformat())
                    
                    if updated_appt.get("patient_user_id"):
                        await invalidate_patient_cache(str(updated_appt["patient_user_id"]))

                    doctor_id = str(updated_appt.get("doctor_id"))
                    event_data = {
                        "appointment_id": str(appt_id),
                        "patient_name": updated_appt.get("patient_name"),
                        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
                        "mode": updated_appt.get("mode"),
                    }
                    await notify_doctor(doctor_id, EVENT_PAYMENT_CONFIRMED, event_data)
                    await notify_doctor(doctor_id, EVENT_APPOINTMENT_BOOKED, event_data)
            except Exception:
                logger.exception("reconcile_captured_payments: failed to send fallback notifications")
        else:
            # Race lost: expire_pending_payments cancelled this appointment
            # between our read and write. Razorpay shows it as paid, but the
            # appointment is no longer pending_payment — manual review required.
            logger.error(
                "reconcile_captured_payments: LOST RACE — appointment_id=%s "
                "was cancelled before confirmation could be written. "
                "payment_id=%s REQUIRES MANUAL REVIEW.",
                appt_id, payment_id,
            )
            try:
                from app.worker.tasks.appointment_tasks import process_refund_for_appointment
                process_refund_for_appointment.apply_async(args=[str(appt_id)])
                logger.info("reconcile_captured_payments: enqueued refund for lost race appointment_id=%s", appt_id)
            except Exception:
                logger.exception("reconcile_captured_payments: failed to enqueue refund for lost race")
            skipped += 1

    logger.info(
        "reconcile_captured_payments: scanned=%d confirmed=%d skipped=%d",
        len(appointments), confirmed, skipped,
    )

@celery_app.task(
    name="app.worker.tasks.appointment_tasks.auto_complete_appointment",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,
    retry_jitter=True,
)
@async_task
async def auto_complete_appointment(appointment_id: str):
    db = await get_task_db() # codeql[py/clear-text-logging-sensitive-data]
    now = utc_now()
    
    try:
        appt_oid = ObjectId(str(appointment_id))
    except Exception:
        return
        
    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        return
        
    prescription = await db.prescriptions.find_one({"appointment_id": appt_oid})
    if not prescription or prescription.get("is_draft", True):
        logger.info("auto_complete_appointment: skipping appointment_id=%s because prescription is not finalized", appointment_id)
        return

    if (
        appt.get("status") == "confirmed" 
        and appt.get("call_status") == "ended" 
        and appt.get("call_connected_at") is not None
    ):
        from app.services.event_bus import notify_appointment, notify_patient
        
        update_set: dict[str, Any] = {
            "status": "completed",
            "completed_at": now,
            "updated_at": now,
            "auto_completed": True,
        }
        
        if appt.get("appointment_type") == "new":
            update_set["is_follow_up_eligible"] = True
            update_set["follow_up_eligible_until"] = now + timedelta(days=int(getattr(settings, "FOLLOW_UP_DAYS", 7)))

        result = await db.appointments.update_one(
            {"_id": appt_oid, "status": "confirmed"},
            {"$set": update_set}
        )
        
        if result.modified_count > 0:
            logger.info("auto_complete_appointment: auto-completed appointment_id=%s", appointment_id)
            
            from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
            from datetime import timezone
            
            scheduled_at = appt.get("scheduled_at")
            if scheduled_at:
                if scheduled_at.tzinfo is None:
                    scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
                await invalidate_doctor_cache(str(appt.get("doctor_id")), day=scheduled_at.date().isoformat())
            if appt.get("patient_user_id"):
                await invalidate_patient_cache(str(appt["patient_user_id"]))
                
            event_data = {
                "appointment_id": appointment_id,
                "status": "completed",
                "completed_at": now.isoformat(),
            }
            await notify_appointment(appointment_id, "appointment_completed", event_data)
            if appt.get("patient_user_id"):
                await notify_patient(str(appt["patient_user_id"]), "appointment_completed", event_data)
        else:
            logger.info("auto_complete_appointment: skipped appointment_id=%s (already completed or status changed)", appointment_id)


@celery_app.task(
    name="app.worker.tasks.appointment_tasks.enforce_disconnect_timeout",
    base=BaseTaskWithDLQ,
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,
    retry_jitter=True,
)
@async_task
async def enforce_disconnect_timeout(appointment_id: str):
    from app.services.call_state_machine import handle_disconnect_timeout
    await handle_disconnect_timeout(appointment_id)
