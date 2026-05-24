from datetime import datetime
import logging

logger = logging.getLogger(__name__)

from fastapi import HTTPException

from app.core.config import settings
from app.services.payment_order_cache_service import (
    acquire_order_lock,
    get_cached_order,
    release_order_lock,
    set_cached_order,
)
from app.services.razorpay_service import create_order as create_razorpay_order
from app.utils.time import ensure_utc
from app.services.razorpay_service import verify_payment_signature


async def create_payment_order_for_appointment(
    db,
    *,
    appointment: dict,
    appointment_id: str,
    now: datetime,
) -> dict:
    status = appointment.get("status")
    if status in ("cancelled", "completed", "no_show"):
        raise HTTPException(status_code=409, detail=f"Cannot create order for appointment with status={status}")

    if appointment.get("payment_choice") != "pay_now":
        raise HTTPException(status_code=400, detail="This appointment does not require online payment")

    consultation_fee = appointment.get("consultation_fee")
    if consultation_fee is None or int(consultation_fee) <= 0:
        raise HTTPException(status_code=400, detail="Invalid payable amount for this appointment")

    amount_paise = int(consultation_fee) * 100
    hold_expires_at = ensure_utc(appointment.get("pending_payment_expires_at"))
    expires_at = hold_expires_at.isoformat() if hold_expires_at else None

    base_response = {
        "appointment_id": appointment_id,
        "provider": "razorpay",
        "amount_paise": amount_paise,
        "currency": "INR",
        "key_id": settings.RAZORPAY_KEY_ID,
        "expires_at": expires_at,
    }

    if status == "confirmed" and appointment.get("payment_status") == "paid":
        return {
            "message": "already_paid",
            "status": "confirmed",
            "payment_status": "paid",
            "order_id": appointment.get("payment_order_id"),
            **base_response,
        }

    if status != "pending_payment":
        raise HTTPException(status_code=409, detail=f"Cannot create order from status={status}")

    if hold_expires_at and hold_expires_at <= now:
        expire_res = await db.appointments.update_one(
            {"_id": appointment["_id"], "status": "pending_payment", "pending_payment_expires_at": {"$lte": now}},
            {"$set": {"payment_status": "failed", "updated_at": now}},
        )
        if expire_res.matched_count == 0:
            raise HTTPException(status_code=409, detail="Appointment state changed. Please retry.")
        raise HTTPException(status_code=409, detail="Payment window expired. Please book again.")

    cached = await get_cached_order(appointment_id)
    if cached and cached.get("order_id"):
        return {
            "message": "order_exists_cached",
            "order_id": cached.get("order_id"),
            "amount_paise": int(cached.get("amount_paise") or amount_paise),
            "currency": cached.get("currency", "INR"),
            "expires_at": cached.get("expires_at"),
            **{k: v for k, v in base_response.items() if k not in {"amount_paise", "currency", "expires_at"}},
        }

    lock_owner = await acquire_order_lock(appointment_id)
    if not lock_owner:
        raise HTTPException(status_code=409, detail="Order creation already in progress. Please retry shortly.")

    try:
        if appointment.get("payment_provider") == "razorpay" and appointment.get("payment_order_id"):
            response = {
                "message": "order_exists",
                "order_id": appointment.get("payment_order_id"),
                **base_response,
            }
            await set_cached_order(
                appointment_id,
                {
                    "order_id": response["order_id"],
                    "amount_paise": response["amount_paise"],
                    "currency": response["currency"],
                    "expires_at": response["expires_at"],
                },
            )
            return response

        order = await create_razorpay_order(
            amount_paise=amount_paise,
            appointment_id=appointment_id,
        )
        order_id = order.get("id")
        if not order_id:
            raise HTTPException(status_code=502, detail="Payment provider did not return an order id")

        update_res = await db.appointments.update_one(
            {"_id": appointment["_id"], "status": "pending_payment"},
            {
                "$set": {
                    "payment_provider": "razorpay",
                    "payment_order_id": order_id,
                    "updated_at": now,
                }
            },
        )
        if update_res.modified_count == 0:
            raise HTTPException(status_code=409, detail="Could not create order due to concurrent update. Please retry.")

        response = {
            "message": "order_created",
            "order_id": order_id,
            **base_response,
        }
        await set_cached_order(
            appointment_id,
            {
                "order_id": order_id,
                "amount_paise": response["amount_paise"],
                "currency": response["currency"],
                "expires_at": response["expires_at"],
            },
        )
        return response
    finally:
        await release_order_lock(appointment_id, lock_owner)


async def verify_payment_signature_and_confirm(
    db,
    appointment_id: str,
    razorpay_payment_id: str,
    razorpay_order_id: str,
    razorpay_signature: str,
) -> dict:
    from app.core.config import settings
    if not settings.ENABLE_SYNC_VERIFICATION:
        return {"status": "pending"}

    if not verify_payment_signature(razorpay_order_id, razorpay_payment_id, razorpay_signature):
        raise HTTPException(status_code=400, detail="Invalid payment signature")

    from bson import ObjectId
    appt_oid = ObjectId(appointment_id)
    appt = await db.appointments.find_one({"_id": appt_oid})
    
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    # 🔴 CRITICAL: bind order to appointment BEFORE trusting any frontend input
    if appt.get("payment_order_id") != razorpay_order_id:
        logger.error(
            "Payment spoofing attempt",
            extra={
                "appointment_id": appointment_id,
                "expected_order_id": appt.get("payment_order_id"),
                "received_order_id": razorpay_order_id,
            }
        )
        raise HTTPException(status_code=403, detail="Invalid payment reference")

    # (Optional but Elite) Bind Payment ID Too
    if appt.get("payment_id") and appt["payment_id"] != razorpay_payment_id:
        raise HTTPException(status_code=409, detail="Payment already processed with different ID")

    from app.services.razorpay_service import client
    import asyncio
    try:
        payment_entity = await asyncio.to_thread(client.payment.fetch, razorpay_payment_id)
    except Exception as e:
        logger.warning(
            "Razorpay API fetch failed for payment_id=%s: %s",
            razorpay_payment_id, str(e),
            exc_info=True
        )
        return {"status": "pending"}

    # Validate against Razorpay response
    if payment_entity.get("order_id") != razorpay_order_id:
        raise HTTPException(status_code=400, detail="Order mismatch from Razorpay")

    if payment_entity.get("status") != "captured":
        return {"status": "pending"}

    if payment_entity.get("currency") != "INR":
        raise HTTPException(status_code=400, detail="Invalid currency")

    expected_fee = appt.get("consultation_fee")
    if expected_fee is not None:
        expected_paise = int(expected_fee) * 100
        if int(payment_entity.get("amount", 0)) != expected_paise:
            raise HTTPException(status_code=400, detail="Amount mismatch")

    logger.info({
        "appointment_id": appointment_id,
        "payment_id": razorpay_payment_id,
        "status": payment_entity.get("status"),
        "verified_via": "sync",
    })

    # Synthesize webhook payload
    payload = {
        "event": "payment.captured",
        "payload": {
            "payment": {
                "entity": payment_entity
            }
        }
    }

    # Process it synchronously via the webhook handler
    from app.routes.webhook_routes import _handle_payment_captured
    result = await _handle_payment_captured(payload)

    if isinstance(result, dict) and result.get("status") in ("missing_amount_ignored", "amount_mismatch_ignored"):
        raise HTTPException(status_code=400, detail="Payment verification failed: Amount mismatch")

    return {"status": "verified"}
