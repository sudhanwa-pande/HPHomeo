import asyncio
import logging
import time
import uuid
import sentry_sdk
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

    receipt = f"appt_{appointment_id}"
    
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
                logger.warning(
                    "Razorpay connection failed. Retrying...",
                    extra={"attempt": attempt + 1, "appointment_id": appointment_id},
                    exc_info=True
                )
                time.sleep(1)  # Wait 1 second before retrying
                continue
            else:
                logger.error("Razorpay connection failed after max retries.", exc_info=True)
                sentry_sdk.capture_exception(e)
                raise  # Finally give up and let Sentry catch it
        except BadRequestError:
            logger.exception("Razorpay create_order bad request", extra={"appointment_id": appointment_id})
            raise HTTPException(status_code=400, detail="Payment order creation failed")
        except ServerError as e:
            logger.exception("Razorpay create_order server error", extra={"appointment_id": appointment_id})
            sentry_sdk.capture_exception(e)
            raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
        except Exception as e:
            logger.exception("Razorpay create_order unexpected error", extra={"appointment_id": appointment_id})
            sentry_sdk.capture_exception(e)
            raise HTTPException(status_code=500, detail="Payment order creation failed")


async def create_order(amount_paise: int, appointment_id: str) -> dict:
    """
    Async wrapper — safe to call from async routes.
    Always pass amount in paise.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_create_order_sync, amount_paise, appointment_id),
            timeout=10.0
        )
    except asyncio.TimeoutError as e:
        logger.error("Razorpay create_order timeout", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=504, detail="Payment gateway timeout")


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

    max_retries = 3
    for attempt in range(max_retries):
        try:
            return client.payment.refund(
                payment_id,
                {"amount": amount_paise, "speed": "normal"},
                headers={"X-Razorpay-Idempotency-Key": idempotency_key},
            )
        except (ConnectionError, ProtocolError, RemoteDisconnected) as e:
            if attempt < max_retries - 1:
                logger.warning(
                    "Razorpay refund network retry",
                    extra={
                        "attempt": attempt + 1,
                        "payment_id": payment_id,
                    },
                    exc_info=True,
                )
                time.sleep(1)
                continue
            else:
                logger.error("Razorpay refund connection failed after max retries", exc_info=True)
                sentry_sdk.capture_exception(e)
                raise
        except BadRequestError:
            logger.exception("Razorpay refund bad request", extra={"payment_id": payment_id})
            raise HTTPException(status_code=400, detail="Refund request invalid")
        except ServerError as e:
            logger.exception("Razorpay refund server error", extra={"payment_id": payment_id})
            sentry_sdk.capture_exception(e)
            raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
        except Exception as e:
            logger.exception("Razorpay refund unexpected error", extra={"payment_id": payment_id})
            sentry_sdk.capture_exception(e)
            raise HTTPException(status_code=500, detail="Refund initiation failed")


async def initiate_refund(payment_id: str, amount_paise: int, idempotency_key: str) -> dict:
    """
    Async wrapper — safe to call from async routes.
    Always pass amount in paise.
    idempotency_key should be derived from the appointment ID so retries
    are deduplicated by Razorpay instead of issuing a second refund.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_initiate_refund_sync, payment_id, amount_paise, idempotency_key),
            timeout=10.0
        )
    except asyncio.TimeoutError as e:
        logger.error("Razorpay initiate_refund timeout", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=504, detail="Payment gateway timeout")


def _fetch_refund_sync(refund_id: str) -> dict:
    if not refund_id:
        raise HTTPException(status_code=400, detail="Invalid refund_id")

    try:
        return client.refund.fetch(refund_id)
    except BadRequestError:
        logger.exception("Razorpay fetch refund bad request")
        raise HTTPException(status_code=400, detail="Refund fetch invalid")
    except ServerError as e:
        logger.exception("Razorpay fetch refund server error")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception as e:
        logger.exception("Razorpay fetch refund unexpected error")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Refund fetch failed")


async def fetch_refund(refund_id: str) -> dict:
    """
    Async wrapper to fetch a refund by id from Razorpay.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_fetch_refund_sync, refund_id),
            timeout=10.0
        )
    except asyncio.TimeoutError as e:
        logger.error("Razorpay fetch_refund timeout", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=504, detail="Payment gateway timeout")


def _fetch_order_sync(order_id: str) -> dict:
    if not order_id:
        raise HTTPException(status_code=400, detail="Invalid order_id")
    try:
        return client.order.fetch(order_id)
    except BadRequestError:
        logger.exception("Razorpay fetch_order bad request")
        raise HTTPException(status_code=400, detail="Order fetch invalid")
    except ServerError as e:
        logger.exception("Razorpay fetch_order server error")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception as e:
        logger.exception("Razorpay fetch_order unexpected error")
        sentry_sdk.capture_exception(e)
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
    except ServerError as e:
        logger.exception("Razorpay fetch_order_payments server error")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=502, detail="Payment provider unavailable, try again")
    except Exception as e:
        logger.exception("Razorpay fetch_order_payments unexpected error")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Order payments fetch failed")


async def fetch_order(order_id: str) -> dict:
    """Async wrapper — fetches a Razorpay order by ID."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_fetch_order_sync, order_id),
            timeout=10.0
        )
    except asyncio.TimeoutError as e:
        logger.error("Razorpay fetch_order timeout", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=504, detail="Payment gateway timeout")


async def fetch_order_payments(order_id: str) -> list:
    """Async wrapper — fetches all payment entities for a Razorpay order."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_fetch_order_payments_sync, order_id),
            timeout=10.0
        )
    except asyncio.TimeoutError as e:
        logger.error("Razorpay fetch_order_payments timeout", exc_info=True)
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=504, detail="Payment gateway timeout")


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


def verify_payment_signature(razorpay_order_id: str, razorpay_payment_id: str, razorpay_signature: str) -> bool:
    """
    Verify Razorpay payment signature returned in the checkout onSuccess callback.
    """
    if not razorpay_order_id or not razorpay_payment_id or not razorpay_signature:
        return False
    try:
        client.utility.verify_payment_signature({
            'razorpay_order_id': razorpay_order_id,
            'razorpay_payment_id': razorpay_payment_id,
            'razorpay_signature': razorpay_signature
        })
        return True
    except SignatureVerificationError:
        logger.warning("Razorpay payment signature verification failed")
        return False
