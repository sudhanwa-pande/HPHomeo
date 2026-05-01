from datetime import datetime

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
