import asyncio
import logging
import time
import uuid
from http.client import RemoteDisconnected

import razorpay
from razorpay.errors import BadRequestError, ServerError, SignatureVerificationError
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError
from fastapi import HTTPException

from app.core.config import settings

logger = logging.getLogger(__name__)

client = razorpay.Client(
    auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET.get_secret_value())
)


def _create_order_sync(amount_paise: int, appointment_id: str) -> dict:
    """
    Internal sync function — called via asyncio.to_thread.
    amount_paise — amount in paise (100 paise = 1 INR)
    """
    if amount_paise <= 0:
        raise HTTPException(status_code=400, detail="Invalid payment amount")
    if not appointment_id:
        raise HTTPException(status_code=400, detail="Invalid appointment_id")

    receipt = f"appt_{appointment_id}_{uuid.uuid4().hex[:8]}"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return client.order.create({
                "amount": amount_paise,
                "currency": "INR",
                "receipt": receipt,
                "payment_capture": 1,  # auto capture — verify against your account setup
            })
        except (ConnectionError, ProtocolError, RemoteDisconnected) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Razorpay connection failed (attempt {attempt + 1}). Retrying...")
                time.sleep(1)  # Wait 1 second before retrying
                continue
            else:
                logger.error("Razorpay connection failed after max retries.")
                raise  # Finally give up and let Sentry catch it
        except BadRequestError:
            logger.exception("Razorpay create_order bad request")
            raise HTTPException(status_code=400, detail="Payment order creation failed")
        except ServerError:
            logger.exception("Razorpay create_order server error")
            raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
        except Exception:
            logger.exception("Razorpay create_order unexpected error")
            raise HTTPException(status_code=500, detail="Payment order creation failed")


async def create_order(amount_paise: int, appointment_id: str) -> dict:
    """
    Async wrapper — safe to call from async routes.
    Always pass amount in paise.
    """
    return await asyncio.to_thread(_create_order_sync, amount_paise, appointment_id)


def _initiate_refund_sync(payment_id: str, amount_paise: int, idempotency_key: str) -> dict:
    """
    Internal sync function — called via asyncio.to_thread.
    idempotency_key must be stable and derived from the appointment, so that
    retrying after a DB write failure does not issue a second refund.
    """
    if not payment_id:
        raise HTTPException(status_code=400, detail="Invalid payment_id")
    if amount_paise <= 0:
        raise HTTPException(status_code=400, detail="Invalid refund amount")

    try:
        return client.payment.refund(
            payment_id,
            {"amount": amount_paise, "speed": "normal"},
            headers={"X-Razorpay-Idempotency-Key": idempotency_key},
        )
    except BadRequestError:
        logger.exception("Razorpay refund bad request")
        raise HTTPException(status_code=400, detail="Refund request invalid")
    except ServerError:
        logger.exception("Razorpay refund server error")
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception:
        logger.exception("Razorpay refund unexpected error")
        raise HTTPException(status_code=500, detail="Refund initiation failed")


async def initiate_refund(payment_id: str, amount_paise: int, idempotency_key: str) -> dict:
    """
    Async wrapper — safe to call from async routes.
    Always pass amount in paise.
    idempotency_key should be derived from the appointment ID so retries
    are deduplicated by Razorpay instead of issuing a second refund.
    """
    return await asyncio.to_thread(_initiate_refund_sync, payment_id, amount_paise, idempotency_key)


def _fetch_refund_sync(refund_id: str) -> dict:
    if not refund_id:
        raise HTTPException(status_code=400, detail="Invalid refund_id")

    try:
        return client.refund.fetch(refund_id)
    except BadRequestError:
        logger.exception("Razorpay fetch refund bad request")
        raise HTTPException(status_code=400, detail="Refund fetch invalid")
    except ServerError:
        logger.exception("Razorpay fetch refund server error")
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception:
        logger.exception("Razorpay fetch refund unexpected error")
        raise HTTPException(status_code=500, detail="Refund fetch failed")


async def fetch_refund(refund_id: str) -> dict:
    """
    Async wrapper to fetch a refund by id from Razorpay.
    """
    return await asyncio.to_thread(_fetch_refund_sync, refund_id)


def _fetch_order_sync(order_id: str) -> dict:
    if not order_id:
        raise HTTPException(status_code=400, detail="Invalid order_id")
    try:
        return client.order.fetch(order_id)
    except BadRequestError:
        logger.exception("Razorpay fetch_order bad request")
        raise HTTPException(status_code=400, detail="Order fetch invalid")
    except ServerError:
        logger.exception("Razorpay fetch_order server error")
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception:
        logger.exception("Razorpay fetch_order unexpected error")
        raise HTTPException(status_code=500, detail="Order fetch failed")


def _fetch_order_payments_sync(order_id: str) -> list:
    """Return the list of payment entities for a given Razorpay order."""
    if not order_id:
        raise HTTPException(status_code=400, detail="Invalid order_id")
    try:
        result = client.order.payments(order_id)
        return result.get("items", [])
    except BadRequestError:
        logger.exception("Razorpay fetch_order_payments bad request")
        raise HTTPException(status_code=400, detail="Order payments fetch invalid")
    except ServerError:
        logger.exception("Razorpay fetch_order_payments server error")
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception:
        logger.exception("Razorpay fetch_order_payments unexpected error")
        raise HTTPException(status_code=500, detail="Order payments fetch failed")


async def fetch_order(order_id: str) -> dict:
    """Async wrapper — fetches a Razorpay order by ID."""
    return await asyncio.to_thread(_fetch_order_sync, order_id)


async def fetch_order_payments(order_id: str) -> list:
    """Async wrapper — fetches all payment entities for a Razorpay order."""
    return await asyncio.to_thread(_fetch_order_payments_sync, order_id)


def verify_webhook_signature(body: bytes, signature: str) -> bool:
    """
    Verify Razorpay webhook signature.
    body — raw request bytes (not parsed JSON)
    signature — from X-Razorpay-Signature header
    """
    if not body or not signature:
        return False
    try:
        client.utility.verify_webhook_signature(
            body.decode("utf-8"),
            signature,
            settings.RAZORPAY_WEBHOOK_SECRET.get_secret_value(),
        )
        return True
    except SignatureVerificationError:
        logger.warning("Razorpay webhook signature verification failed")
        return False
