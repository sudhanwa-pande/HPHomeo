"""
Server-Sent Events (SSE) streaming endpoints.

These replace client-side polling for real-time updates:
  - Doctor: waiting patients, new bookings, cancellations, payment confirmations
  - Patient (auth): call status changes for their appointments
  - Patient (public): call status changes for a specific appointment

Each endpoint opens a long-lived HTTP connection that streams events as they
happen via Redis Pub/Sub. The browser's native EventSource API handles
reconnection automatically.

Reliability contract:
  SSE + Redis Pub/Sub is at-most-once.  Clients MUST do a single REST poll
  after every EventSource ``open`` event (including reconnects) to fetch
  current state and catch any events missed during the reconnect window.
"""

import logging

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.database import get_db
from app.core.security import decode_token
from app.core.token_blacklist import is_token_blacklisted
from app.routes.auth_routes import get_current_doctor
from app.routes.patient_auth_routes import get_current_patient
from app.utils.appointment_rules import get_patient_access_token, validate_patient_token
from app.utils.time import utc_now
from app.services.event_bus import (
    doctor_channel,
    patient_channel,
    appointment_channel,
    subscribe_sse,
    acquire_sse_slot,
    release_sse_slot,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["SSE"])

# ── Constants ────────────────────────────────────────────────────
# Max stream lifetime — forces EventSource reconnect + fresh auth check.
# Doctor streams can be longer (active all day); patient streams are shorter.
_DOCTOR_MAX_DURATION = 3600      # 1 hour
_PATIENT_MAX_DURATION = 1800     # 30 minutes
_PUBLIC_MAX_DURATION = 1800      # 30 minutes

# How often to re-validate auth during an active stream.
_AUTH_RECHECK_SECONDS = 300      # 5 minutes


def _sse_response(generator) -> StreamingResponse:
    """Wrap an SSE async generator in the correct StreamingResponse."""
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",       # disable Nginx buffering
            "Content-Encoding": "identity",   # prevent proxy gzip buffering
        },
    )


# ═══════════════════════════════════════════════════════════════
#  Auth re-validation helpers
#
#  These are called periodically inside the SSE stream to detect
#  token expiry, blacklisting, or account suspension *during* a
#  long-lived connection.  They return True if auth is still valid.
# ═══════════════════════════════════════════════════════════════

def _make_doctor_auth_check(request: Request, doctor_id: str):
    """Return an async callable that re-validates the doctor's session."""
    async def _check() -> bool:
        token = request.cookies.get("doctor_access_token")
        if not token:
            return False
        try:
            payload = decode_token(token)  # verifies exp + signature
        except Exception:
            return False
        if payload.get("role") != "doctor":
            return False
        if await is_token_blacklisted(get_db(), payload.get("jti")):
            return False
        # Check account suspension
        db = get_db()
        doc = await db.doctors.find_one(
            {"_id": ObjectId(doctor_id)},
            {"is_suspended": 1, "verification_status": 1},
        )
        if not doc:
            return False
        if doc.get("is_suspended"):
            return False
        if doc.get("verification_status") not in {"pending", "approved"}:
            return False
        return True
    return _check


def _make_patient_auth_check(request: Request, patient_id: str):
    """Return an async callable that re-validates the patient's session."""
    async def _check() -> bool:
        token = request.cookies.get("patient_access_token")
        if not token:
            return False
        try:
            payload = decode_token(token)  # verifies exp + signature
        except Exception:
            return False
        if payload.get("role") != "patient":
            return False
        if await is_token_blacklisted(get_db(), payload.get("jti")):
            return False
        return True
    return _check


def _make_public_auth_check(appointment_id: str, token: str):
    """Return an async callable that re-validates a public magic token."""
    async def _check() -> bool:
        db = get_db()
        try:
            appt = await db.appointments.find_one(
                {"_id": ObjectId(appointment_id)},
                {
                    "patient_access_token_hash": 1,
                    "patient_access_expires_at": 1,
                },
            )
        except Exception:
            return False
        if not appt:
            return False
        try:
            validate_patient_token(appt, token, utc_now())
        except HTTPException:
            return False
        return True
    return _check


# ═══════════════════════════════════════════════════════════════
#  Doctor SSE — replaces polling for /calls/waiting + /notifications
# ═══════════════════════════════════════════════════════════════

@router.get("/doctor/events/stream")
async def doctor_event_stream(
    request: Request,
    doctor=Depends(get_current_doctor),
):
    """SSE stream for doctor events.

    Events pushed:
      - patient_waiting: a patient joined the waiting room
      - appointment_booked: new appointment confirmed
      - appointment_cancelled: patient cancelled
      - appointment_rescheduled: patient rescheduled
      - payment_confirmed: Razorpay payment captured
      - call_state_changed: call status transition
      - appointment_completed: doctor completed the appointment
      - appointment_no_show: doctor marked patient as no-show
      - reconnect: max stream duration reached, client should reconnect
      - auth_expired: token expired or revoked, client must re-authenticate
      - error: stream interrupted (Redis failure), client should reconnect
    """
    doc_id = str(doctor["_id"])
    channel = doctor_channel(doc_id)

    if not await acquire_sse_slot(channel):
        raise HTTPException(status_code=429, detail="Too many SSE connections")

    async def event_generator():
        try:
            async for event in subscribe_sse(
                [channel],
                max_duration_seconds=_DOCTOR_MAX_DURATION,
                disconnect_check=request.is_disconnected,
                auth_check=_make_doctor_auth_check(request, doc_id),
                auth_check_interval_seconds=_AUTH_RECHECK_SECONDS,
            ):
                yield event
        finally:
            await release_sse_slot(channel)

    return _sse_response(event_generator())


# ═══════════════════════════════════════════════════════════════
#  Authenticated Patient SSE — replaces waiting room polling
# ═══════════════════════════════════════════════════════════════

@router.get("/patient/events/stream")
async def patient_event_stream(
    request: Request,
    current=Depends(get_current_patient),
):
    """SSE stream for authenticated patient events.

    Events pushed:
      - call_state_changed: doctor joined / call started / call ended
      - appointment_completed: doctor completed the appointment
      - appointment_no_show: doctor marked patient as no-show
      - reconnect: max stream duration reached, client should reconnect
      - auth_expired: token expired or revoked, client must re-authenticate
      - error: stream interrupted (Redis failure), client should reconnect
    """
    patient_id = str(current["_id"])
    channel = patient_channel(patient_id)

    if not await acquire_sse_slot(channel):
        raise HTTPException(status_code=429, detail="Too many SSE connections")

    async def event_generator():
        try:
            async for event in subscribe_sse(
                [channel],
                max_duration_seconds=_PATIENT_MAX_DURATION,
                disconnect_check=request.is_disconnected,
                auth_check=_make_patient_auth_check(request, patient_id),
                auth_check_interval_seconds=_AUTH_RECHECK_SECONDS,
            ):
                yield event
        finally:
            await release_sse_slot(channel)

    return _sse_response(event_generator())


# ═══════════════════════════════════════════════════════════════
#  Public Patient SSE — replaces waiting room polling for public users
# ═══════════════════════════════════════════════════════════════

@router.get("/public/events/stream/{appointment_id}")
async def public_event_stream(
    request: Request,
    appointment_id: str,
    token: str = Depends(get_patient_access_token),
):
    """SSE stream for public (unauthenticated) patient events.

    Scoped to a single appointment. Requires valid magic token.

    Events pushed:
      - call_state_changed: doctor joined / call started / call ended
      - appointment_completed: doctor completed the appointment
      - appointment_no_show: doctor marked patient as no-show
      - reconnect: max stream duration reached, client should reconnect
      - auth_expired: magic token expired, client must re-authenticate
      - error: stream interrupted (Redis failure), client should reconnect
    """
    db = get_db()
    now = utc_now()

    try:
        appt_oid = ObjectId(appointment_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Appointment not found")

    appt = await db.appointments.find_one({"_id": appt_oid})
    if not appt:
        raise HTTPException(status_code=404, detail="Appointment not found")

    validate_patient_token(appt, token, now)

    channel = appointment_channel(appointment_id)

    if not await acquire_sse_slot(channel):
        raise HTTPException(status_code=429, detail="Too many SSE connections")

    async def event_generator():
        try:
            async for event in subscribe_sse(
                [channel],
                max_duration_seconds=_PUBLIC_MAX_DURATION,
                disconnect_check=request.is_disconnected,
                auth_check=_make_public_auth_check(appointment_id, token),
                auth_check_interval_seconds=_AUTH_RECHECK_SECONDS,
            ):
                yield event
        finally:
            await release_sse_slot(channel)

    return _sse_response(event_generator())
