from datetime import datetime, timedelta, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.admin_auth_routes import verify_admin_session
from app.utils.time import day_window_to_utc, ensure_utc

router = APIRouter(prefix="/admin/appointments", tags=["Admin Appointments"])


def _refund_display_status(refund_status: str | None) -> str:
    # Webhook terminal state is stored as "processed"; expose it as "refunded" for UI clarity.
    if refund_status == "processed":
        return "refunded"
    return refund_status or "none"


def _is_refund_terminal(refund_status: str | None) -> bool:
    return _refund_display_status(refund_status) in {"refunded", "failed", "none"}


def _parse_datetime_or_day_start(value: str) -> datetime:
    try:
        if len(value) == 10:
            start_utc, _ = day_window_to_utc(value, tz_name="Asia/Kolkata")
            return start_utc
        return ensure_utc(datetime.fromisoformat(value))
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid datetime/date format")


def _parse_datetime_or_day_end(value: str) -> datetime:
    try:
        if len(value) == 10:
            _, end_utc = day_window_to_utc(value, tz_name="Asia/Kolkata")
            return end_utc
        dt = ensure_utc(datetime.fromisoformat(value))
        return dt + timedelta(microseconds=1)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid datetime/date format")


@router.get(
    "",
    dependencies=[rl(settings.RL_ADMIN_READ_TIMES, settings.RL_ADMIN_READ_SECONDS)],
)
async def list_admin_appointments(
    doctor_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    date_from: str | None = Query(default=None, description="YYYY-MM-DD or ISO datetime"),
    date_to: str | None = Query(default=None, description="YYYY-MM-DD or ISO datetime"),
    payment_status: str | None = Query(default=None),
    refund_status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    skip: int = Query(default=0, ge=0),
    current=Depends(verify_admin_session),
):
    db = get_db()
    query: dict = {}

    if doctor_id:
        try:
            query["doctor_id"] = ObjectId(doctor_id)
        except Exception:
            raise HTTPException(status_code=422, detail="Invalid doctor_id")
    if status:
        query["status"] = status
    if payment_status:
        query["payment_status"] = payment_status
    if refund_status:
        query["refund_status"] = refund_status

    if date_from or date_to:
        scheduled_q: dict = {}
        if date_from:
            scheduled_q["$gte"] = _parse_datetime_or_day_start(date_from)
        if date_to:
            scheduled_q["$lt"] = _parse_datetime_or_day_end(date_to)
        query["scheduled_at"] = scheduled_q

    docs = (
        await db.appointments.find(query)
        .sort("scheduled_at", -1)
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )

    return {
        "count": len(docs),
        "appointments": [
            {
                "appointment_id": str(doc["_id"]),
                "doctor_id": str(doc.get("doctor_id")) if doc.get("doctor_id") else None,
                "doctor_name": doc.get("doctor_name"),
                "patient_name": doc.get("patient_name"),
                "status": doc.get("status"),
                "scheduled_at": doc.get("scheduled_at"),
                "payment_status": doc.get("payment_status"),
                "refund_status": doc.get("refund_status"),
                "refund_display_status": _refund_display_status(doc.get("refund_status")),
                "refund_terminal": _is_refund_terminal(doc.get("refund_status")),
                "consultation_fee": doc.get("consultation_fee"),
            }
            for doc in docs
        ],
    }
