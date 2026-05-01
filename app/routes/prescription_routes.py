import io
import asyncio
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
from app.schemas.prescription_schema import PrescriptionUpsert, PrescriptionTemplateCreate

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.auth_routes import get_current_doctor
from app.routes.patient_auth_routes import get_current_patient
from app.services.cache_service import (
    TTL_5_MINUTES,
    TTL_10_MINUTES,
    TTL_30_MINUTES,
    cache_delete_keys,
    cache_get_json,
    cache_set_json,
    doctor_templates_key,
    invalidate_prescription_cache,
    invalidate_patient_cache,
    patient_prescriptions_key,
    prescription_data_key,
)
from app.services.cloudinary_service import cloudinary_service
from app.services.pdf_service import build_prescription_pdf_bytes

from app.utils.clinic import clinic_profile_fields
from app.utils.time import utc_now

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Prescriptions"])

ALLOWED_CREATE_STATUSES = {"confirmed", "completed"}


def _oid(x: Any) -> ObjectId:
    try:
        return x if isinstance(x, ObjectId) else ObjectId(str(x))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid id")


def _serialize_prescription(p: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(p)
    p["id"] = str(p.pop("_id"))
    p["appointment_id"] = str(p["appointment_id"])
    p["doctor_id"] = str(p["doctor_id"])
    p["patient_id"] = str(p["patient_id"])
    if p.get("created_at") and hasattr(p["created_at"], "isoformat"):
        p["created_at"] = p["created_at"].isoformat()
    if p.get("updated_at") and hasattr(p["updated_at"], "isoformat"):
        p["updated_at"] = p["updated_at"].isoformat()
    return p


async def _get_prescription_data_cached(prescription_id: ObjectId) -> Dict[str, Any] | None:
    cached = await cache_get_json(prescription_data_key(str(prescription_id)))
    return cached if isinstance(cached, dict) else None


async def _set_prescription_data_cache(serialized: Dict[str, Any]) -> None:
    await cache_set_json(
        prescription_data_key(serialized["id"]),
        serialized,
        TTL_10_MINUTES,
    )


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


async def _next_rx_id(db) -> str:
    # RX-YYYYMM-000001
    yyyymm = datetime.now(timezone.utc).strftime("%Y%m")
    name = f"rx_{yyyymm}"

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
    return f"RX-{yyyymm}-{seq:06d}"


async def _get_appointment_for_doctor(db, appointment_id: str, doctor_oid: ObjectId) -> Dict[str, Any]:
    appt = await db.appointments.find_one({"_id": _oid(appointment_id), "doctor_id": doctor_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")
    return appt


def _ensure_allowed_status(appt: Dict[str, Any]) -> None:
    if appt.get("status") not in ALLOWED_CREATE_STATUSES:
        raise HTTPException(
            status_code=400,
            detail="Prescription allowed only for confirmed or completed appointments",
        )


def _doctor_info_payload(doctor: dict, appt: Dict[str, Any]) -> Dict[str, Any]:
    qualifications = doctor.get("qualifications") or []
    return {
        "full_name": doctor.get("full_name") or appt.get("doctor_name"),
        "clinic_name": clinic_profile_fields()["clinic_name"],
        "registration_no": doctor.get("registration_no"),
        "qualifications": qualifications if isinstance(qualifications, list) else [],
        "signature_url": doctor.get("signature_url"),
    }


def _patient_info_payload(appt: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": appt.get("patient_name"),
        "age": appt.get("patient_age"),
        "sex": appt.get("patient_sex"),
        "phone": appt.get("patient_phone"),
        "email": appt.get("patient_email"),
    }


# -----------------------------
# Doctor
# -----------------------------

@router.get("/doctor/appointments/{appointment_id}/prescription")
async def doctor_get_prescription(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"]})
    if not pres:
        return {"exists": False, "prescription": None}
    serialized = await _get_prescription_data_cached(pres["_id"])
    if not serialized:
        serialized = _serialize_prescription(pres)
        await _set_prescription_data_cache(serialized)
    return {"exists": True, "prescription": serialized}


@router.post("/doctor/appointments/{appointment_id}/prescription")
async def doctor_create_prescription(
    appointment_id: str,
    payload: PrescriptionUpsert,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])
    _ensure_allowed_status(appt)

    existing = await db.prescriptions.find_one({"appointment_id": appt["_id"]})
    if existing:
        raise HTTPException(status_code=409, detail="Prescription already exists for this appointment")

    rx_id = await _next_rx_id(db)
    now = utc_now()

    doc = {
        "rx_id": rx_id,
        "appointment_id": appt["_id"],
        "doctor_id": appt["doctor_id"],
        "patient_id": appt["patient_id"],
        "patient_info": _patient_info_payload(appt),
        "doctor_info": _doctor_info_payload(doctor, appt),
        "status": "draft",
        "is_draft": True,
        "version": 0,
        "pdf_url": None,
        "chief_complaints": payload.chief_complaints,
        "diagnosis": payload.diagnosis,
        "advice": payload.advice,
        "items": [it.model_dump() for it in payload.items],
        "created_at": now,
        "updated_at": now,
    }
    try:
        res = await db.prescriptions.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Prescription already exists for this appointment")
    doc["_id"] = res.inserted_id
    serialized = _serialize_prescription(doc)
    await _set_prescription_data_cache(serialized)
    return serialized


@router.put("/doctor/appointments/{appointment_id}/prescription")
async def doctor_update_prescription(
    appointment_id: str,
    payload: PrescriptionUpsert,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])
    _ensure_allowed_status(appt)

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"]})
    if not pres:
        raise HTTPException(status_code=404, detail="Prescription not found")
    if not pres.get("is_draft", True):
        raise HTTPException(
            status_code=400,
            detail="Prescription is finalised and cannot be edited",
        )

    update = {
        "patient_info": _patient_info_payload(appt),
        "doctor_info": _doctor_info_payload(doctor, appt),
        "chief_complaints": payload.chief_complaints,
        "diagnosis": payload.diagnosis,
        "advice": payload.advice,
        "items": [it.model_dump() for it in payload.items],
        "updated_at": utc_now(),
    }
    await db.prescriptions.update_one(
        {"_id": pres["_id"]},
        {
            "$set": update,
            "$unset": {
                "notes": "",
                "tests": "",
                "follow_up": "",
                "follow_up_date": "",
            },
        },
    )
    pres.update(update)
    pres.pop("notes", None)
    pres.pop("tests", None)
    pres.pop("follow_up", None)
    pres.pop("follow_up_date", None)
    await invalidate_prescription_cache(str(pres["_id"]))
    serialized = _serialize_prescription(pres)
    await _set_prescription_data_cache(serialized)
    return serialized


@router.post("/doctor/appointments/{appointment_id}/prescription/preview")
async def doctor_preview_prescription(
    appointment_id: str,
    payload: PrescriptionUpsert,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])
    _ensure_allowed_status(appt)

    pdf_bytes = await asyncio.to_thread(
        build_prescription_pdf_bytes,
        rx_id=None,
        created_at=utc_now(),
        doctor=_doctor_info_payload(doctor, appt),
        patient=_patient_info_payload(appt),
        appointment=appt,
        items=[it.model_dump() for it in payload.items],
        chief_complaints=payload.chief_complaints,
        diagnosis=payload.diagnosis,
        advice=payload.advice,
        signature_url=doctor.get("signature_url"),
    )

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": 'inline; filename="prescription-preview.pdf"'},
    )


@router.post(
    "/doctor/appointments/{appointment_id}/prescription/generate",
    dependencies=[rl(settings.RL_PRESCRIPTION_GENERATE_TIMES, settings.RL_PRESCRIPTION_GENERATE_SECONDS)],
)
async def doctor_generate_prescription(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])
    _ensure_allowed_status(appt)

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"]})
    if not pres:
        raise HTTPException(status_code=404, detail="Prescription not found")
    if not pres.get("is_draft", True):
        raise HTTPException(
            status_code=400,
            detail="Prescription already generated and cannot be regenerated",
        )

    # Registration number is mandatory for a legally valid Indian prescription.
    # Reject here — before any PDF bytes are produced — so the doctor is forced
    # to complete their profile first. A PDF without a reg. number is not a
    # valid medical document and must never be issued to a patient.
    if not (doctor.get("registration_no") or "").strip():
        raise HTTPException(
            status_code=400,
            detail=(
                "Your registration number is missing. "
                "Update your profile before issuing a prescription."
            ),
        )

    # Claim generation lock so only one finalization runs.
    claim_res = await db.prescriptions.update_one(
        {"_id": pres["_id"], "is_draft": True, "generation_in_progress": {"$ne": True}},
        {"$set": {"generation_in_progress": True, "updated_at": utc_now()}},
    )
    if claim_res.modified_count == 0:
        raise HTTPException(status_code=409, detail="Prescription generation already in progress")

    patient_info = _patient_info_payload(appt)
    doctor_info = _doctor_info_payload(doctor, appt)

    version_next = int(pres.get("version") or 0) + 1

    try:
        pdf_bytes = await asyncio.to_thread(
            build_prescription_pdf_bytes,
            rx_id=pres["rx_id"],
            created_at=utc_now(),
            doctor={
                "full_name": doctor_info.get("full_name"),
                "clinic_name": doctor_info.get("clinic_name"),
                "registration_no": doctor_info.get("registration_no"),
                "qualifications": doctor_info.get("qualifications"),
            },
            patient={
                "name": patient_info.get("name"),
                "age": patient_info.get("age"),
                "sex": patient_info.get("sex"),
                "phone": patient_info.get("phone"),
                "email": appt.get("patient_email"),
            },
            appointment=appt,
            items=pres.get("items") or [],
            chief_complaints=pres.get("chief_complaints"),
            diagnosis=pres.get("diagnosis"),
            advice=pres.get("advice"),
            signature_url=doctor_info.get("signature_url"),
        )

        # Upload PDF
        doctor_id = str(appt["doctor_id"])
        public_id = f"{pres['rx_id']}-v{version_next}"
        pdf_url = await asyncio.to_thread(
            cloudinary_service.upload_pdf_bytes,
            data=pdf_bytes,
            filename=f"{pres['rx_id']}-v{version_next}.pdf",
            folder=f"prescriptions/{doctor_id}",
            public_id=public_id,
        )

        now = utc_now()

        # Save immutable generated snapshot for audit/debug.
        ver_doc = {
            "prescription_id": pres["_id"],
            "rx_id": pres["rx_id"],
            "appointment_id": pres["appointment_id"],
            "doctor_id": pres["doctor_id"],
            "patient_id": pres["patient_id"],
            "version": version_next,
            "pdf_url": pdf_url,
            "snapshot": {
                "chief_complaints": pres.get("chief_complaints"),
                "diagnosis": pres.get("diagnosis"),
                "advice": pres.get("advice"),
                "items": pres.get("items") or [],
                "patient_info": patient_info,
                "doctor_info": doctor_info,
            },
            "created_at": now,
        }
        await db.prescription_versions.insert_one(ver_doc)

        # Update current state with compare-and-set guard.
        finish_res = await db.prescriptions.update_one(
            {"_id": pres["_id"], "is_draft": True, "generation_in_progress": True},
            {
                "$set": {
                    "pdf_url": pdf_url,
                    "is_draft": False,
                    "status": "final",
                    "version": version_next,
                    "patient_info": patient_info,
                    "doctor_info": doctor_info,
                    "generation_in_progress": False,
                    "updated_at": now,
                },
                "$unset": {
                    "notes": "",
                    "tests": "",
                    "follow_up": "",
                    "follow_up_date": "",
                },
            },
        )
        if finish_res.modified_count == 0:
            await db.prescriptions.update_one(
                {"_id": pres["_id"]},
                {"$set": {"generation_in_progress": False, "updated_at": utc_now()}},
            )
            raise HTTPException(status_code=409, detail="Prescription state changed. Please retry.")
    except Exception:
        await db.prescriptions.update_one(
            {"_id": pres["_id"]},
            {"$set": {"generation_in_progress": False, "updated_at": utc_now()}},
        )
        raise

    pres["pdf_url"] = pdf_url
    pres["is_draft"] = False
    pres["status"] = "final"
    pres["version"] = version_next
    pres["patient_info"] = patient_info
    pres["doctor_info"] = doctor_info
    pres["updated_at"] = now
    pres.pop("notes", None)
    pres.pop("tests", None)
    pres.pop("follow_up", None)
    pres.pop("follow_up_date", None)
    await invalidate_prescription_cache(str(pres["_id"]))
    serialized = _serialize_prescription(pres)
    await _set_prescription_data_cache(serialized)
    await invalidate_patient_cache(str(pres["patient_id"]))

    return {"pdf_url": pdf_url, "prescription": serialized}


@router.get("/doctor/appointments/{appointment_id}/prescription/pdf")
async def doctor_get_prescription_pdf(
    appointment_id: str,
    request: Request,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"]})
    if not pres or not pres.get("pdf_url"):
        raise HTTPException(status_code=404, detail="PDF not generated yet")
    return {"pdf_url": str(request.url.path) + "/view"}


@router.get("/doctor/appointments/{appointment_id}/prescription/pdf/view")
async def doctor_view_prescription_pdf(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"]})
    if not pres or not pres.get("pdf_url"):
        raise HTTPException(status_code=404, detail="PDF not generated yet")

    return await _proxy_pdf(pres["pdf_url"], f"{pres.get('rx_id', 'prescription')}.pdf")


@router.get("/doctor/appointments/{appointment_id}/prescription/template")
async def doctor_get_template(
    appointment_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    appt = await _get_appointment_for_doctor(db, appointment_id, doctor["_id"])

    return {
        "patient_info": _patient_info_payload(appt),
        "doctor_info": _doctor_info_payload(doctor, appt),
    }


@router.get("/doctor/patients/{patient_id}/prescription-history")
async def doctor_patient_prescription_history(
    patient_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    patient_oid = _oid(patient_id)

    patient = await db.patients.find_one({"_id": patient_oid, "doctor_id": doctor["_id"]})
    if not patient:
        raise HTTPException(status_code=404, detail="Patient not found")

    cursor = db.prescriptions.find(
        {"doctor_id": doctor["_id"], "patient_id": patient_oid},
        sort=[("created_at", -1)],
    )
    raw_items = await cursor.to_list(length=200)
    appt_ids = [p["appointment_id"] for p in raw_items]
    appts = await db.appointments.find(
        {"_id": {"$in": appt_ids}},
        {"scheduled_at": 1, "status": 1},
    ).to_list(length=200)
    appt_map = {a["_id"]: a for a in appts}

    items = []
    for p in raw_items:
        sp = _serialize_prescription(p)
        appt = appt_map.get(p["appointment_id"])
        sp["appointment"] = {
            "scheduled_at": appt.get("scheduled_at").isoformat() if appt and appt.get("scheduled_at") else None,
            "status": appt.get("status") if appt else None,
        }
        items.append(sp)

    return {
        "patient": {
            "id": str(patient["_id"]),
            "full_name": patient.get("full_name"),
            "age": patient.get("age"),
            "sex": patient.get("sex"),
            "phone": patient.get("phone"),
            "email": patient.get("email"),
        },
        "items": items,
    }


# -----------------------------
# Patient
# -----------------------------

@router.get("/patient/prescriptions")
async def patient_list_prescriptions(
    patient=Depends(get_current_patient),
):
    db = get_db()
    cache_key = patient_prescriptions_key(str(patient["_id"]))
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    ownership = {"$or": [{"patient_user_id": patient["_id"]}]}
    if patient.get("phone"):
        ownership["$or"].append({"patient_phone": patient["phone"]})

    appt_ids = await db.appointments.distinct("_id", ownership)
    if not appt_ids:
        response = {"items": []}
        await cache_set_json(cache_key, response, TTL_5_MINUTES)
        return response

    cursor = db.prescriptions.find(
        {"appointment_id": {"$in": appt_ids}, "is_draft": False},
        sort=[("created_at", -1)],
    )
    items = []
    async for p in cursor:
        items.append(_serialize_prescription(p))
    response = {"items": items}
    await cache_set_json(cache_key, response, TTL_5_MINUTES)
    return response


@router.get("/patient/appointments/{appointment_id}/prescription")
async def patient_get_prescription(
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

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"], "is_draft": False})
    if not pres:
        return {"exists": False, "prescription": None}
    serialized = await _get_prescription_data_cached(pres["_id"])
    if not serialized:
        serialized = _serialize_prescription(pres)
        await _set_prescription_data_cache(serialized)
    return {"exists": True, "prescription": serialized}


@router.get("/patient/appointments/{appointment_id}/prescription/pdf")
async def patient_get_prescription_pdf(
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

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"], "is_draft": False})
    if not pres or not pres.get("pdf_url"):
        raise HTTPException(status_code=404, detail="PDF not generated yet")
    return {"pdf_url": str(request.url.path) + "/view"}


@router.get("/patient/appointments/{appointment_id}/prescription/pdf/view")
async def patient_view_prescription_pdf(
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

    pres = await db.prescriptions.find_one({"appointment_id": appt["_id"], "is_draft": False})
    if not pres or not pres.get("pdf_url"):
        raise HTTPException(status_code=404, detail="PDF not generated yet")

    return await _proxy_pdf(pres["pdf_url"], f"{pres.get('rx_id', 'prescription')}.pdf")


# -----------------------------
# Templates (doctor)
# -----------------------------

@router.get("/doctor/prescription-templates")
async def doctor_list_templates(
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    cache_key = doctor_templates_key(str(doctor["_id"]))
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    cursor = db.prescription_templates.find({"doctor_id": doctor["_id"]}, sort=[("created_at", -1)])
    items = []
    async for t in cursor:
        t = dict(t)
        t["id"] = str(t.pop("_id"))
        t["doctor_id"] = str(t["doctor_id"])
        if t.get("created_at"):
            t["created_at"] = t["created_at"].isoformat()
        items.append(t)
    response = {"items": items}
    await cache_set_json(cache_key, response, TTL_30_MINUTES)
    return response


@router.post("/doctor/prescription-templates")
async def doctor_create_template(
    payload: PrescriptionTemplateCreate,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    now = utc_now()
    doc = {
        "doctor_id": doctor["_id"],
        "name": payload.name.strip(),
        "payload": payload.payload.model_dump(),
        "created_at": now,
        "updated_at": now,
    }
    res = await db.prescription_templates.insert_one(doc)
    doc["_id"] = res.inserted_id

    out = dict(doc)
    out["id"] = str(out.pop("_id"))
    out["doctor_id"] = str(out["doctor_id"])
    out["created_at"] = out["created_at"].isoformat()
    out["updated_at"] = out["updated_at"].isoformat()
    await cache_delete_keys(doctor_templates_key(str(doctor["_id"])))
    return out


@router.delete("/doctor/prescription-templates/{template_id}")
async def doctor_delete_template(
    template_id: str,
    doctor=Depends(get_current_doctor),
):
    db = get_db()
    res = await db.prescription_templates.delete_one({"_id": _oid(template_id), "doctor_id": doctor["_id"]})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Template not found")
    await cache_delete_keys(doctor_templates_key(str(doctor["_id"])))
    return {"ok": True}
