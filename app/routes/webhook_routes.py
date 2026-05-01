import logging
import hashlib
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Header

from app.core.database import get_db
from app.core.redis import get_redis
from app.services.cache_service import invalidate_doctor_cache, invalidate_patient_cache
from app.services.razorpay_service import verify_webhook_signature
from app.services.email_service import (
    safe_send_email,
    send_payment_confirmation,
    send_doctor_new_booking,
)
from app.routes.receipt_routes import generate_receipt_for_appointment
from app.services.whatsapp_service import (
    safe_send_whatsapp,
    send_patient_appointment_update_whatsapp,
)
from app.services.cloudinary_service import cloudinary_service
from app.services.video_service import ensure_video_room
from app.services.event_bus import notify_doctor, EVENT_PAYMENT_CONFIRMED, EVENT_APPOINTMENT_BOOKED
from app.utils.time import ensure_utc, utc_now

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])
logger = logging.getLogger(__name__)
WEBHOOK_IDEMPOTENCY_TTL_SECONDS = 72 * 60 * 60  # 72 hours — covers Razorpay's full retry window


def _webhook_unique_id(event: str | None, payload: dict, body: bytes) -> str:
    event_name = event or "unknown"
    entity = payload.get("payload", {})

    payment = entity.get("payment", {}).get("entity", {})
    refund = entity.get("refund", {}).get("entity", {})
    payload_event_id = str(payload.get("event_id") or "").strip()

    if event_name in {"payment.captured", "payment.failed"}:
        payment_id = str(payment.get("id") or "").strip()
        order_id = str(payment.get("order_id") or "").strip()
        if payment_id or order_id:
            return f"{event_name}:{payment_id}:{order_id}"

    if event_name in {"refund.processed", "refund.failed"}:
        refund_id = str(refund.get("id") or "").strip()
        payment_id = str(refund.get("payment_id") or "").strip()
        if refund_id or payment_id:
            return f"{event_name}:{refund_id}:{payment_id}"

    if payload_event_id:
        return f"{event_name}:event_id:{payload_event_id}"

    # Fallback for providers/payloads without stable ids.
    return f"{event_name}:body_sha256:{hashlib.sha256(body).hexdigest()}"


async def _is_duplicate_webhook(event_id: str) -> bool:
    redis = get_redis()
    key = f"webhook:razorpay:event:{event_id}"
    created = await redis.set(key, "1", ex=WEBHOOK_IDEMPOTENCY_TTL_SECONDS, nx=True)
    return not bool(created)


async def _release_webhook_claim(event_id: str) -> None:
    redis = get_redis()
    key = f"webhook:razorpay:event:{event_id}"
    await redis.delete(key)

@router.post("/razorpay")
async def razorpay_webhook(
    request: Request,
    x_razorpay_signature: Optional[str] = Header(default=None),
):
    # ---------------------------
    # 1. Read raw body first
    # ---------------------------
    body = await request.body()

    # ---------------------------
    # 2. Verify signature
    # ---------------------------
    if not x_razorpay_signature:
        logger.warning("Razorpay webhook: missing signature header")
        raise HTTPException(status_code=400, detail="Missing signature")

    if not verify_webhook_signature(body, x_razorpay_signature):
        logger.warning("Razorpay webhook: invalid signature")
        raise HTTPException(status_code=400, detail="Invalid signature")

    # ---------------------------
    # 3. Parse payload
    # ---------------------------
    try:
        payload = await request.json()
    except Exception:
        logger.exception("Razorpay webhook: failed to parse JSON")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event = payload.get("event")
    logger.info("Razorpay webhook received: event=%s", event)
    event_id = _webhook_unique_id(event, payload, body)
    if await _is_duplicate_webhook(event_id):
        logger.warning("Razorpay webhook duplicate ignored: event=%s event_id=%s", event, event_id)
        return {"status": "duplicate_ignored"}

    # ---------------------------
    # 4. Route by event
    # ---------------------------
    try:
        if event == "payment.captured":
            await _handle_payment_captured(payload)
        elif event == "payment.failed":
            await _handle_payment_failed(payload)
        elif event == "refund.processed":
            await _handle_refund_processed(payload)
        elif event == "refund.failed":
            await _handle_refund_failed(payload)
        else:
            logger.info("Razorpay webhook: unhandled event=%s, ignoring", event)
    except Exception:
        # Release the claim so Razorpay retries can reprocess the event.
        await _release_webhook_claim(event_id)
        logger.exception("Razorpay webhook processing failed: event=%s event_id=%s", event, event_id)
        raise HTTPException(status_code=500, detail="Webhook processing failed")

    # Always return 200 — Razorpay retries on non-200
    return {"status": "ok"}


async def _handle_payment_captured(payload: dict):
    db = get_db()
    now = utc_now()

    payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
    order_id = payment.get("order_id")
    payment_id = payment.get("id")
    amount_paise = payment.get("amount")

    if not order_id or not payment_id:
        logger.error("payment.captured: missing order_id or payment_id")
        return

    appt = await db.appointments.find_one({"payment_order_id": order_id})
    if not appt:
        logger.error("payment.captured: no appointment found for order_id=%s", order_id)
        return

    appt_id = appt["_id"]

    # ---------------------------
    # Idempotency — already confirmed
    # ---------------------------
    if appt.get("payment_status") == "paid" and appt.get("status") == "confirmed":
        logger.info("payment.captured: already confirmed, appointment_id=%s", appt_id)
        return

    # ---------------------------
    # Validate payment amount matches expected fee — HARD BLOCK
    # Logging only is not sufficient: a mismatch means either a Razorpay
    # misconfiguration, a tampered webhook, or a race with a fee change.
    # In all cases we must not confirm the appointment. Raising here causes
    # the outer except block to release the idempotency claim so Razorpay
    # will retry, and Sentry will capture the event for investigation.
    # ---------------------------
    expected_fee = appt.get("consultation_fee")
    if amount_paise is None:
        logger.error(
            "payment.captured: missing amount in webhook payload for appointment_id=%s "
            "payment_id=%s — rejecting",
            appt_id, payment_id,
        )
        raise Exception(f"Missing amount in payment.captured payload for appointment_id={appt_id}")

    if expected_fee is not None:
        expected_paise = int(expected_fee) * 100
        if int(amount_paise) != expected_paise:
            logger.error(
                "payment.captured: AMOUNT MISMATCH appointment_id=%s "
                "expected_paise=%s received_paise=%s payment_id=%s — rejecting",
                appt_id, expected_paise, amount_paise, payment_id,
            )
            raise Exception(
                f"Amount mismatch for appointment_id={appt_id}: "
                f"expected {expected_paise} paise, received {amount_paise} paise"
            )

    update_set = {
        "status": "confirmed",
        "confirmed_at": now,
        "payment_status": "paid",
        "payment_provider": "razorpay",
        "payment_id": payment_id,
        "payment_amount_paise": amount_paise,
        "pending_payment_expires_at": None,
        "updated_at": now,
    }

    # ---------------------------
    # Concurrency-safe update
    # ---------------------------
    update_res = await db.appointments.update_one(
        {"_id": appt_id, "status": "pending_payment"},
        {"$set": update_set},
    )

    if update_res.modified_count == 0:
        logger.warning(
            "payment.captured: could not update appointment_id=%s — already modified",
            appt_id,
        )
        return

    logger.info("payment.captured: confirmed appointment_id=%s", appt_id)

    # ---------------------------
    # Re-fetch for emails
    # ---------------------------
    updated_appt = await db.appointments.find_one({"_id": appt_id})
    if not updated_appt:
        return
    if updated_appt.get("mode") == "online" and updated_appt.get("video_enabled", True):
        # The DB write (payment confirmed) has already committed above.
        # A failure here must NOT propagate to the outer exception handler,
        # which would release the idempotency claim and cause Razorpay to
        # retry an already-confirmed payment.
        try:
            await ensure_video_room(db, updated_appt)
            updated_appt = await db.appointments.find_one({"_id": appt_id})
            if not updated_appt:
                return
        except Exception:
            logger.warning(
                "payment.captured: ensure_video_room failed for appointment_id=%s — "
                "payment is confirmed, video room will be created lazily",
                appt_id,
                exc_info=True,
            )

    # ── Generate payment receipt before sending confirmations ──
    receipt_pdf_bytes: bytes | None = None
    receipt_pdf_url: str | None = None
    receipt_id: str | None = None
    try:
        receipt, receipt_pdf_bytes = await generate_receipt_for_appointment(
            db, updated_appt, payment_id=payment_id,
        )
        raw_receipt_pdf_url = receipt.get("pdf_url")
        if raw_receipt_pdf_url:
            receipt_pdf_url = cloudinary_service.pdf_view_url(url=raw_receipt_pdf_url)
        receipt_id = receipt.get("receipt_id")
    except Exception:
        logger.warning("Failed to generate receipt for appointment_id=%s", appt_id, exc_info=True)

    # ── Send confirmation email (with receipt PDF attached) ──
    if updated_appt.get("patient_email"):
        await safe_send_email(
            send_payment_confirmation(
                updated_appt,
                receipt_pdf_bytes=receipt_pdf_bytes,
                receipt_id=receipt_id,
            ),
            "payment confirmation",
        )

    # ── Send confirmation WhatsApp (with receipt PDF in template header) ──
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

    scheduled_at = ensure_utc(updated_appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(updated_appt.get("doctor_id")), day=scheduled_at.date().isoformat())
    if updated_appt.get("patient_user_id"):
        await invalidate_patient_cache(str(updated_appt["patient_user_id"]))

    # SSE: notify doctor — payment confirmed + new booking
    doctor_id = str(updated_appt.get("doctor_id"))
    event_data = {
        "appointment_id": str(appt_id),
        "patient_name": updated_appt.get("patient_name"),
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
        "mode": updated_appt.get("mode"),
    }
    await notify_doctor(doctor_id, EVENT_PAYMENT_CONFIRMED, event_data)
    await notify_doctor(doctor_id, EVENT_APPOINTMENT_BOOKED, event_data)


async def _handle_payment_failed(payload: dict):
    db = get_db()
    now = utc_now()

    payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
    order_id = payment.get("order_id")
    payment_id = payment.get("id")

    if not order_id:
        logger.error("payment.failed: missing order_id")
        return

    appt = await db.appointments.find_one({"payment_order_id": order_id})
    if not appt:
        logger.error("payment.failed: no appointment found for order_id=%s", order_id)
        return

    appt_id = appt["_id"]

    # Ignore late/duplicate failure events once payment is already successful/finalised.
    if appt.get("payment_status") == "paid" or appt.get("status") == "confirmed":
        logger.info("payment.failed: ignoring late event for already-paid appointment_id=%s", appt_id)
        return

    # Idempotency — already terminal
    if appt.get("status") in ("cancelled", "completed", "no_show"):
        logger.info("payment.failed: already handled, appointment_id=%s", appt_id)
        return

    update_set = {
        "payment_status": "failed",
        "payment_provider": "razorpay",
        "pending_payment_expires_at": None,
        "updated_at": now,
    }
    if payment_id:
        update_set["payment_id"] = payment_id

    update_res = await db.appointments.update_one(
        {"_id": appt_id, "status": "pending_payment", "payment_status": {"$ne": "paid"}},
        {"$set": update_set},
    )
    if update_res.modified_count == 0:
        logger.info("payment.failed: skipped stale event for appointment_id=%s", appt_id)
        return

    logger.info("payment.failed: marked payment failed for appointment_id=%s", appt_id)
    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(appt.get("doctor_id")), day=scheduled_at.date().isoformat())
    if appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))


async def _handle_refund_processed(payload: dict):
    db = get_db()
    now = utc_now()

    refund = payload.get("payload", {}).get("refund", {}).get("entity", {})
    payment_id = refund.get("payment_id")
    refund_id = refund.get("id")
    amount_paise = refund.get("amount")

    if not payment_id:
        logger.error("refund.processed: missing payment_id")
        return

    appt = await db.appointments.find_one({"payment_id": payment_id})
    if not appt:
        logger.error("refund.processed: no appointment found for payment_id=%s", payment_id)
        return

    appt_id = appt["_id"]

    # Guard: refund webhook should apply only to appointment that currently owns this payment.
    if not appt.get("payment_id") or appt.get("payment_id") != payment_id:
        logger.warning("refund.processed: payment ownership mismatch for appointment_id=%s", appt_id)
        return
    if appt.get("payment_status") == "transferred":
        logger.warning("refund.processed: ignoring transferred payment for appointment_id=%s", appt_id)
        return
    if appt.get("refund_status") not in ("pending", "processing", "processed"):
        logger.warning(
            "refund.processed: refund not pending for appointment_id=%s (status=%s)",
            appt_id,
            appt.get("refund_status"),
        )
        return

    # Idempotency — already processed
    if appt.get("refund_status") == "processed":
        logger.info("refund.processed: already processed, appointment_id=%s", appt_id)
        return

    await db.appointments.update_one(
        {"_id": appt_id},
        {
            "$set": {
                "refund_status": "processed",
                "refund_id": refund_id,
                "refund_amount_paise": amount_paise,
                "payment_status": "refunded",
                "updated_at": now,
            }
        },
    )

    logger.info("refund.processed: updated appointment_id=%s", appt_id)
    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(appt.get("doctor_id")), day=scheduled_at.date().isoformat())
    if appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))


async def _handle_refund_failed(payload: dict):
    db = get_db()
    now = utc_now()

    refund = payload.get("payload", {}).get("refund", {}).get("entity", {})
    payment_id = refund.get("payment_id")
    refund_id = refund.get("id")

    if not payment_id:
        logger.error("refund.failed: missing payment_id")
        return

    appt = await db.appointments.find_one({"payment_id": payment_id})
    if not appt:
        logger.error("refund.failed: no appointment found for payment_id=%s", payment_id)
        return

    appt_id = appt["_id"]
    if not appt.get("payment_id") or appt.get("payment_id") != payment_id:
        logger.warning("refund.failed: payment ownership mismatch for appointment_id=%s", appt_id)
        return
    if appt.get("payment_status") == "transferred":
        logger.warning("refund.failed: ignoring transferred payment for appointment_id=%s", appt_id)
        return
    if appt.get("refund_status") == "processed":
        logger.info("refund.failed: ignoring late failure for already-processed appointment_id=%s", appt_id)
        return
    if appt.get("refund_status") not in ("pending", "processing", "processed", "failed"):
        logger.warning(
            "refund.failed: refund not expected for appointment_id=%s (status=%s)",
            appt_id,
            appt.get("refund_status"),
        )
        return

    failure_reason = (
        refund.get("error_description")
        or refund.get("description")
        or refund.get("failure_reason")
        or "provider_refund_failed"
    )

    await db.appointments.update_one(
        {"_id": appt_id},
        {
            "$set": {
                "refund_status": "failed",
                "refund_id": refund_id or appt.get("refund_id"),
                "payment_status": "paid",
                "refund_processing_started_at": None,
                "refund_failure_reason": str(failure_reason),
                "updated_at": now,
            }
        },
    )

    logger.info("refund.failed: updated appointment_id=%s", appt_id)
    scheduled_at = ensure_utc(appt.get("scheduled_at"))
    if scheduled_at:
        await invalidate_doctor_cache(str(appt.get("doctor_id")), day=scheduled_at.date().isoformat())
    if appt.get("patient_user_id"):
        await invalidate_patient_cache(str(appt["patient_user_id"]))
