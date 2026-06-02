import asyncio
import io
import logging
import ssl
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.request import urlopen, Request as UrlRequest

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.auth_routes import get_current_doctor
from app.routes.patient_auth_routes import get_current_patient
from app.services.cache_service import (
    TTL_5_MINUTES,
    TTL_10_MINUTES,
    cache_get_json,
    cache_set_json,
    patient_receipts_key,
)
from app.services.cloudinary_service import cloudinary_service
from app.services.receipt_pdf_service import build_receipt_pdf_bytes
from app.utils.time import utc_now

router = APIRouter(tags=["Receipts"])
logger = logging.getLogger(__name__)


from app.utils.mongo_utils import _oid


def _serialize_receipt(r: Dict[str, Any]) -> Dict[str, Any]:
    r = dict(r)
    r["id"] = str(r.pop("_id"))
    r["appointment_id"] = str(r["appointment_id"])
    r["doctor_id"] = str(r["doctor_id"])
    if r.get("patient_id"):
        r["patient_id"] = str(r["patient_id"])
    if r.get("patient_user_id"):
        r["patient_user_id"] = str(r["patient_user_id"])
    if r.get("receipt_date") and hasattr(r["receipt_date"], "isoformat"):
        r["receipt_date"] = r["receipt_date"].isoformat()
    if r.get("created_at") and hasattr(r["created_at"], "isoformat"):
        r["created_at"] = r["created_at"].isoformat()
    return r


_ssl_ctx = ssl.create_default_context()


def _fetch_url(url: str) -> bytes:
    req = UrlRequest(url, headers={"User-Agent": "eHomeo/1.0"})
    return urlopen(req, timeout=15, context=_ssl_ctx).read()


async def _proxy_pdf(pdf_url: str, filename: str) -> StreamingResponse:
    """Fetch PDF from Cloudinary and stream it with correct headers."""
    url = cloudinary_service.pdf_view_url(url=pdf_url)
    if not url:
        raise HTTPException(status_code=404, detail="PDF not available")
    try:
        data = await asyncio.to_thread(_fetch_url, url)
    except Exception:
        logger.exception("Failed to fetch PDF from %s", url)
        raise HTTPException(status_code=502, detail="Could not fetch PDF")
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


async def _next_receipt_id(db) -> str:
    """INV-YYYY-NNNN sequential receipt number."""
    yyyy = datetime.now(timezone.utc).strftime("%Y")
    name = f"receipt_{yyyy}"

    doc = await db.counters.find_one_and_update(
        {"name": name},
        {
            "$inc": {"seq": 1},
            "$setOnInsert": {"created_at": utc_now()},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    seq = int(doc["seq"])
    return f"INV-{yyyy}-{seq:04d}"


async def generate_receipt_for_appointment(
    db,
    appointment: Dict[str, Any],
    payment_id: str | None = None,
) -> tuple[Dict[str, Any], bytes | None]:
    """
    Generate receipt PDF, upload to Cloudinary, store in DB.
    Called from webhook after payment.captured.
    Returns (serialised_receipt, pdf_bytes).  pdf_bytes is None for duplicates.
    """
    # Guard: don't generate duplicate
    existing = await db.receipts.find_one({"appointment_id": appointment["_id"]})
    if existing:
        return _serialize_receipt(existing), None

    receipt_id = await _next_receipt_id(db)
    now = utc_now()

    if not appointment.get("patient_id"):
        logger.warning(
            "generate_receipt: appointment %s has no patient_id — receipt may not appear in doctor history",
            appointment["_id"],
        ) # codeql[py/clear-text-logging-sensitive-data]

    # Fetch doctor for registration_no
    doctor = await db.doctors.find_one( # codeql[py/clear-text-logging-sensitive-data]
        {"_id": appointment["doctor_id"]},
        {"registration_no": 1, "full_name": 1},
    )

    doctor_name = (doctor.get("full_name") if doctor else None) or appointment.get("doctor_name", "")
    doctor_reg_no = (doctor.get("registration_no") if doctor else None) or ""
    consultation_fee = float(appointment.get("consultation_fee", 0))
    payment_method = _resolve_payment_method(appointment)

    # Generate PDF bytes
    pdf_bytes = await asyncio.to_thread(
        build_receipt_pdf_bytes,
        receipt_id=receipt_id,
        receipt_date=now,
        patient_name=appointment.get("patient_name", ""),
        doctor_name=doctor_name,
        doctor_registration_no=doctor_reg_no,
        consultation_fee=consultation_fee,
        payment_method=payment_method,
    )

    # Upload to Cloudinary
    doctor_id_str = str(appointment["doctor_id"])
    pdf_url = await asyncio.to_thread(
        cloudinary_service.upload_pdf_bytes,
        data=pdf_bytes,
        filename=f"{receipt_id}.pdf",
        folder=f"receipts/{doctor_id_str}",
        public_id=receipt_id,
    )

    # Store in DB
    receipt_doc = {
        "receipt_id": receipt_id,
        "appointment_id": appointment["_id"],
        "doctor_id": appointment["doctor_id"],
        "patient_id": appointment.get("patient_id"),
        "patient_user_id": appointment.get("patient_user_id"),
        "patient_name": appointment.get("patient_name", ""),
        "patient_email": appointment.get("patient_email"),
        "patient_phone": appointment.get("patient_phone", ""),
        "doctor_name": doctor_name,
        "doctor_registration_no": doctor_reg_no,
        "consultation_fee": consultation_fee,
        "payment_method": payment_method,
        "payment_id": payment_id,
        "pdf_url": pdf_url,
        "receipt_date": now,
        "created_at": now,
    }
    try:
        res = await db.receipts.insert_one(receipt_doc)
        receipt_doc["_id"] = res.inserted_id
    except DuplicateKeyError:
        # Parallel webhook fired — another worker already inserted.
        # Return the existing receipt; pdf_bytes=None signals "already existed".
        existing = await db.receipts.find_one({"appointment_id": appointment["_id"]})
        if existing:
            return _serialize_receipt(existing), None
        raise  # Should never happen, but don't swallow unknown errors

    return _serialize_receipt(receipt_doc), pdf_bytes


def _resolve_payment_method(appointment: Dict[str, Any]) -> str:
    provider = appointment.get("payment_provider", "")
    if provider == "razorpay":
        return "Razorpay"
    if provider == "test":
        return "Test"
    choice = appointment.get("payment_choice", "")
    if choice == "pay_at_clinic":
        return "Cash"
    return "Online"


# ─────────────────────────────────────────────────────────────
# Patient endpoints
# ─────────────────────────────────────────────────────────────

@router.get(
    "/patient/receipts",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def patient_list_receipts(
    patient=Depends(get_current_patient),
):
    db = get_db()
    cache_key = patient_receipts_key(str(patient["_id"]))
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    ownership = {"$or": [{"patient_user_id": patient["_id"]}]}
    if patient.get("phone"):
        ownership["$or"].append({"patient_phone": patient["phone"]})

    cursor = db.receipts.find(ownership, sort=[("created_at", -1)])
    items = []
    async for r in cursor:
        items.append(_serialize_receipt(r))
    response = {"items": items}
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


@router.get(
    "/patient/appointments/{appointment_id}/receipt",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def patient_get_receipt(
    appointment_id: str,
    patient=Depends(get_current_patient),
):
    db = get_db()
    appt_q = {
        "_id": _oid(appointment_id),
        "$or": [{"patient_user_id": patient["_id"]}],
    }
    if patient.get("phone"):
        appt_q["$or"].append({"patient_phone": patient["phone"]})

    appt = await db.appointments.find_one(appt_q)
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    receipt = await db.receipts.find_one({"appointment_id": appt["_id"]})
    if not receipt:
        return {"exists": False, "receipt": None}
    return {"exists": True, "receipt": _serialize_receipt(receipt)}


@router.get(
    "/patient/appointments/{appointment_id}/receipt/pdf",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def patient_get_receipt_pdf(
    appointment_id: str,
    request: Request,
    patient=Depends(get_current_patient),
):
    db = get_db()
    appt_q = {
        "_id": _oid(appointment_id),
        "$or": [{"patient_user_id": patient["_id"]}],
    }
    if patient.get("phone"):
        appt_q["$or"].append({"patient_phone": patient["phone"]})

    appt = await db.appointments.find_one(appt_q)
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    receipt = await db.receipts.find_one({"appointment_id": appt["_id"]})
    if not receipt or not receipt.get("pdf_url"):
        raise HTTPException(status_code=404, detail="Receipt not generated yet")
    return {"pdf_url": str(request.url.path) + "/view"}


@router.get(
    "/patient/appointments/{appointment_id}/receipt/pdf/view",
    dependencies=[rl(settings.RL_PATIENT_READ_TIMES, settings.RL_PATIENT_READ_SECONDS)],
)
async def patient_view_receipt_pdf(
    appointment_id: str,
    patient=Depends(get_current_patient),
):
    db = get_db()
    appt_q = {
        "_id": _oid(appointment_id),
        "$or": [{"patient_user_id": patient["_id"]}],
    }
    if patient.get("phone"):
        appt_q["$or"].append({"patient_phone": patient["phone"]})

    appt = await db.appointments.find_one(appt_q)
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    receipt = await db.receipts.find_one({"appointment_id": appt["_id"]})
    if not receipt or not receipt.get("pdf_url"):
        raise HTTPException(status_code=404, detail="Receipt not generated yet")

    return await _proxy_pdf(receipt["pdf_url"], f"{receipt.get('receipt_id', 'receipt')}.pdf")


# ─────────────────────────────────────────────────────────────
# Doctor endpoints
# ─────────────────────────────────────────────────────────────

@router.get(
    "/doctor/appointments/{appointment_id}/receipt",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def doctor_get_receipt(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await db.appointments.find_one(
        {"_id": _oid(appointment_id), "doctor_id": doctor["_id"]}
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    receipt = await db.receipts.find_one({"appointment_id": appt["_id"]})
    if not receipt:
        return {"exists": False, "receipt": None}
    return {"exists": True, "receipt": _serialize_receipt(receipt)}


@router.get(
    "/doctor/appointments/{appointment_id}/receipt/pdf",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def doctor_get_receipt_pdf(
    appointment_id: str,
    request: Request,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await db.appointments.find_one(
        {"_id": _oid(appointment_id), "doctor_id": doctor["_id"]}
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    receipt = await db.receipts.find_one({"appointment_id": appt["_id"]})
    if not receipt or not receipt.get("pdf_url"):
        raise HTTPException(status_code=404, detail="Receipt not generated yet")
    return {"pdf_url": str(request.url.path) + "/view"}


@router.get(
    "/doctor/appointments/{appointment_id}/receipt/pdf/view",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def doctor_view_receipt_pdf(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await db.appointments.find_one(
        {"_id": _oid(appointment_id), "doctor_id": doctor["_id"]}
    )
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    receipt = await db.receipts.find_one({"appointment_id": appt["_id"]})
    if not receipt or not receipt.get("pdf_url"):
        raise HTTPException(status_code=404, detail="Receipt not generated yet")

    return await _proxy_pdf(receipt["pdf_url"], f"{receipt.get('receipt_id', 'receipt')}.pdf")


@router.get(
    "/doctor/patients/{patient_id}/receipt-history",
    dependencies=[rl(settings.RL_DOCTOR_READ_TIMES, settings.RL_DOCTOR_READ_SECONDS)],
)
async def doctor_patient_receipt_history(
    patient_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    patient_oid = _oid(patient_id)

    patient = await db.patients.find_one({"_id": patient_oid, "doctor_id": doctor["_id"]})
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")

    cursor = db.receipts.find(
        {"doctor_id": doctor["_id"], "patient_id": patient_oid},
        sort=[("created_at", -1)],
    )
    items = []
    async for r in cursor:
        items.append(_serialize_receipt(r))

    return {
        "patient": {
            "id": str(patient["_id"]),
            "full_name": patient.get("full_name"),
            "phone": patient.get("phone"),
            "email": patient.get("email"),
        },
        "items": items,
    }
