"""
LiveKit Webhook Handler — receives room/participant events from LiveKit server.

LiveKit signs webhooks with a JWT in the Authorization header.
The JWT is signed with the API secret, and its `sha256` claim must match
the SHA-256 hash of the request body.

Events handled:
  - participant_joined → state machine: participant count increases
  - participant_left   → state machine: participant count decreases
  - room_finished      → state machine: room closed, end call

All handlers are idempotent — safe for retries and out-of-order delivery.
"""

import base64
import hashlib
import json
import logging

from fastapi import APIRouter, HTTPException, Request, BackgroundTasks

from jose import jwt, JWTError

from app.core.config import settings
from app.services.call_state_machine import (
    handle_participant_joined,
    handle_participant_left,
    handle_room_finished,
)

router = APIRouter(prefix="/webhooks", tags=["LiveKit Webhooks"])
logger = logging.getLogger(__name__)


def _get_webhook_secret() -> str:
    return settings.LIVEKIT_WEBHOOK_SECRET or settings.LIVEKIT_API_SECRET.get_secret_value()


def _verify_livekit_webhook(body: bytes, auth_header: str | None) -> dict:
    """Verify LiveKit webhook signature and return the decoded payload.

    LiveKit sends the JWT as the raw Authorization header value (no "Bearer "
    prefix — see livekit/server-sdk-* across Go/JS/Python). The JWT is signed
    with the API secret (HS256), and the body's SHA-256 hash is **base64-
    encoded** (not hex) and stored in the `sha256` claim.
    """
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    # Accept both raw JWT and "Bearer <jwt>" — LiveKit sends raw, but allow
    # Bearer for forward-compatibility / proxies that might add it.
    token = auth_header[7:] if auth_header.startswith("Bearer ") else auth_header
    secret = _get_webhook_secret()

    if not secret:
        raise HTTPException(status_code=500, detail="LiveKit webhook secret not configured")

    try:
        decoded = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options={"verify_aud": False, "verify_sub": False},
        )
    except JWTError as e:
        logger.warning("LiveKit webhook: JWT verification failed: %s", e)
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    # Verify issuer matches our API key
    if decoded.get("iss") != settings.LIVEKIT_API_KEY:
        logger.warning(
            "LiveKit webhook: issuer mismatch expected=%s got=%s",
            settings.LIVEKIT_API_KEY, decoded.get("iss"),
        )
        raise HTTPException(status_code=401, detail="Invalid webhook issuer")

    # Verify body hash — LiveKit base64-encodes the SHA-256 (standard with
    # padding), NOT hex. Compare in base64 to match.
    expected_hash = decoded.get("sha256")
    if expected_hash:
        actual_hash = base64.b64encode(hashlib.sha256(body).digest()).decode()
        if actual_hash != expected_hash:
            logger.warning(
                "LiveKit webhook: body hash mismatch expected=%s got=%s",
                expected_hash, actual_hash,
            )
            raise HTTPException(status_code=401, detail="Body hash mismatch")

    return decoded


def _extract_appointment_id_from_room(room_name: str) -> str | None:
    """Extract appointment_id from room name format: appt_{appointment_id}_{suffix}"""
    if not room_name or not room_name.startswith("appt_"):
        return None
    parts = room_name.split("_")
    if len(parts) >= 3:
        return parts[1]
    return None


def _extract_role_from_identity(identity: str) -> str:
    """Extract role from identity format: 'doctor:{id}' or 'patient:{id}'"""
    if identity.startswith("doctor:"):
        return "doctor"
    if identity.startswith("patient:"):
        return "patient"
    return "unknown"


def _extract_appointment_id_from_metadata(metadata_str: str | None) -> str | None:
    """Extract appointment_id from participant metadata JSON."""
    if not metadata_str:
        return None
    try:
        meta = json.loads(metadata_str)
        return meta.get("appointment_id")
    except (json.JSONDecodeError, TypeError):
        return None


@router.post("/livekit")
async def livekit_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle LiveKit server webhooks for participant/room events using BackgroundTasks."""
    body = await request.body()
    auth_header = request.headers.get("Authorization")

    # Verify signature
    _verify_livekit_webhook(body, auth_header)

    # Parse payload
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        logger.warning("LiveKit webhook: invalid JSON body")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event = payload.get("event")
    if not event:
        return {"status": "ignored", "reason": "no event type"}

    logger.info("LiveKit webhook received: event=%s", event)

    room = payload.get("room", {})
    room_name = room.get("name", "")
    participant = payload.get("participant", {})
    identity = participant.get("identity", "")
    participant_sid = participant.get("sid", "")
    metadata = participant.get("metadata")

    trace_id = None
    if metadata:
        try:
            meta = json.loads(metadata)
            trace_id = meta.get("trace_id")
        except Exception:
            pass
    trace_id = trace_id or "missing_trace"

    # Resolve appointment_id from metadata first, then room name
    appointment_id = _extract_appointment_id_from_metadata(metadata)
    if not appointment_id:
        appointment_id = _extract_appointment_id_from_room(room_name)

    if event == "participant_joined":
        if not appointment_id or not identity:
            logger.debug("LiveKit webhook: participant_joined missing data, ignoring")
            return {"status": "ignored"}

        role = _extract_role_from_identity(identity)
        logger.info("participant_joined webhook: appointment_id=%s role=%s trace_id=%s", appointment_id, role, trace_id)
        background_tasks.add_task(handle_participant_joined, appointment_id, identity, role, participant_sid)
        return {"status": "ok"}

    elif event == "participant_left":
        if not appointment_id or not identity:
            logger.debug("LiveKit webhook: participant_left missing data, ignoring")
            return {"status": "ignored"}

        role = _extract_role_from_identity(identity)
        logger.info("participant_left webhook: appointment_id=%s role=%s trace_id=%s", appointment_id, role, trace_id)
        background_tasks.add_task(handle_participant_left, appointment_id, identity, participant_sid)
        return {"status": "ok"}

    elif event == "room_finished":
        if not room_name:
            return {"status": "ignored"}

        logger.info("room_finished webhook: room=%s appointment_id=%s trace_id=%s", room_name, appointment_id, trace_id)
        background_tasks.add_task(handle_room_finished, room_name)
        return {"status": "ok"}

    else:
        logger.debug("LiveKit webhook: unhandled event=%s", event)
        return {"status": "ignored", "event": event}

